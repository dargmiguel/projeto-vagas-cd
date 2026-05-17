import os
import sys
import logging
from pathlib import Path

import polars as pl
from dotenv import load_dotenv
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Date,
    Boolean,
    create_engine,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import DeclarativeBase, Session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

load_dotenv()

NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")
if not NEON_DATABASE_URL:
    logger.error("Variavel de ambiente NEON_DATABASE_URL nao definida.")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DELTA_PATH = PROJECT_ROOT / "data" / "gold_delta" / "dim_vagas"

CHUNK_SIZE = 1000

COLUMNS_NEEDED = [
    "vaga_id", "titulo", "descricao_limpa", "empresa", "localizacao",
    "modalidade", "nivel", "area_principal", "skills_tech", "skills_soft",
    "is_tech", "data_publicacao", "data_expiracao", "url",
]

class Base(DeclarativeBase):
    pass

class Vaga(Base):
    __tablename__ = "vagas"
    vaga_id = Column(String, primary_key=True)
    titulo = Column(String, nullable=False)
    descricao_limpa = Column(Text, nullable=False)
    empresa = Column(String)
    localizacao = Column(String)
    modalidade = Column(String)
    nivel = Column(String)
    area_principal = Column(String)
    skills_tech = Column(Text)
    skills_soft = Column(Text)
    is_tech = Column(Boolean)
    data_publicacao = Column(DateTime)
    data_expiracao = Column(Date)
    url = Column(String)


def get_engine():
    return create_engine(
        NEON_DATABASE_URL,
        pool_pre_ping=True,
        pool_size=5,
    )

def load_dim_vagas() -> pl.DataFrame:
    if not DELTA_PATH.exists():
        logger.error(f"Caminho do Delta Lake nao encontrado: {DELTA_PATH}")
        sys.exit(1)

    logger.info(f"Carregando dados de {DELTA_PATH}")
    return pl.read_delta(str(DELTA_PATH))

def prepare_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    for col in ["skills_tech", "skills_soft"]:
        if col in df.columns:
            df = df.with_columns(pl.col(col).list.join(", "))

    if "nivel" in df.columns:
        df = df.with_columns(pl.col("nivel").cast(pl.Utf8))

    for col in COLUMNS_NEEDED:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    return df.select(COLUMNS_NEEDED)

def upsert_chunk(session: Session, records: list[dict]) -> int:
    if not records:
        return 0

    stmt = pg_insert(Vaga).values(records)
    update_cols = {
        col.name: stmt.excluded[col.name]
        for col in Vaga.__table__.columns
        if col.name != "vaga_id"
    }

    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=["vaga_id"],
        set_=update_cols,
    )
    session.execute(upsert_stmt)
    return len(records)

def sync_data(engine, df: pl.DataFrame) -> None:
    total_rows = df.height
    if total_rows == 0:
        logger.info("Nenhuma vaga para sincronizar!")
        return

    logger.info(f"Iniciando UPSERT de {total_rows} vagas em chunks de {CHUNK_SIZE}")

    with Session(engine) as session:
        total_synced = 0
        for i in range(0, total_rows, CHUNK_SIZE):
            chunk_df = df.slice(i, CHUNK_SIZE)
            records = chunk_df.to_dicts()

            count = upsert_chunk(session, records)
            total_synced += count
            session.commit()

            logger.info(f"  Chunk {i // CHUNK_SIZE + 1}: {total_synced}/{total_rows} processadas")

    logger.info(f"UPSERT concluido: {total_synced} vagas sincronizadas.")

def create_search_index(engine) -> None:
    index_sql = text("""
        CREATE INDEX IF NOT EXISTS idx_vagas_busca
        ON vagas
        USING GIN(to_tsvector('portuguese', titulo || ' ' || descricao_limpa));
    """)
    logger.info("Verificando indice GIN para busca textual...")
    with engine.connect() as conn:
        conn.execute(index_sql)
        conn.commit()

def add_missing_columns(engine) -> None:
    columns_to_add = [
        ("skills_soft", "TEXT"),
        ("is_tech", "BOOLEAN"),
        ("data_publicacao", "TIMESTAMP"),
        ("data_expiracao", "DATE"),
    ]
    with engine.connect() as conn:
        for col_name, col_type in columns_to_add:
            conn.execute(text(f"ALTER TABLE vagas ADD COLUMN IF NOT EXISTS {col_name} {col_type};"))
        conn.commit()
    logger.info("Schema do banco verificado/atualizado.")

def main():
    logger.info("=" * 60)
    logger.info("SYNC TO NEON - Inicio")
    logger.info("=" * 60)

    engine = get_engine()

    logger.info("Garantindo estrutura da tabela 'vagas'...")
    Base.metadata.create_all(engine)
    add_missing_columns(engine)

    df = load_dim_vagas()
    logger.info(f"Total de registros carregados: {df.height}")

    df = prepare_dataframe(df)

    sync_data(engine, df)
    create_search_index(engine)

    logger.info("=" * 60)
    logger.info("SYNC TO NEON - Concluido")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()