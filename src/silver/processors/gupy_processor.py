import logging
import os
import html
import re
import sys
from typing import Any, Dict
import polars as pl
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import yaml
from src.utils.checkpoint_manager import CheckpointManager
import unicodedata

load_dotenv()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def _build_regex(termos: list) -> str:
    """
    Constrói regex otimizado e seguro contra falsos positivos.
    - Palavras simples  → \\btermo\\b
    - Frases            → match exato da frase (sem \\b)
    - Ignora termos de 1 letra só
    """
    if not termos or not any(termos):
        return r"a^"

    # Normaliza e filtra (ignora strings vazias e de 1 char)
    cleaned = [str(t).strip().lower() for t in termos if str(t).strip() and len(str(t).strip()) >= 2]
    if not cleaned:
        return r"a^"

    # Escapa para regex
    escaped = [re.escape(t) for t in cleaned]
    # Maior primeiro (frases maiores antes de palavras curtas)
    escaped.sort(key=len, reverse=True)

    # Frases têm espaço → não usam \\b nas pontas
    phrases = [t for t in escaped if " " in t]

    # Palavras simples → usam word boundary \\b
    # IMPORTANTE: só UMA barra, usando raw f-string: fr"..."
    words = [fr"\b{t}\b" for t in escaped if " " not in t]

    pattern = "|".join(phrases + words)
    return pattern if pattern else r"a^"

def processar_vagas(df_bronze: pl.DataFrame | pl.LazyFrame, config: dict) -> pl.DataFrame:
    """Processa o DataFrame de vagas da camada bronze para a camada silver.
    Args:
        df_bronze (pl.DataFrame | pl.LazyFrame): DataFrame da camada bronze.
        config (dict): Configurações do processamento.
    Returns:
        pl.DataFrame: DataFrame processado para a camada silver."""

    logger.info("Iniciando o processamento das vagas para a camada silver.")

    lf = df_bronze.lazy() if isinstance(df_bronze, pl.DataFrame) else df_bronze

    # 1. Regex
    tech_regex = _build_regex(config.get('skills_tech', []))
    soft_regext = _build_regex(config.get('skills_soft', []))
    nivel_regex = {k: _build_regex(v) for k, v in config.get('niveis', {}).items()}
    area_regex = {k: _build_regex(v) for k, v in config.get('areas', {}).items()}

    # 2. Texto de busca
    texto_busca = (
        pl.concat_str([pl.col('name'), pl.col('description')], separator=' ')
        .fill_null('')
        .str.to_lowercase()
        .str.replace_all(r"[^a-z0-9áéíóúç\s]", " ")
        .str.replace_all(r"\s+", " ")
    )

    lf = (
        # Limpeza
        lf
        .with_columns([
            pl.col("description").fill_null("")
              .str.replace_all(r"&[#\w]+;", " ")
              .str.replace_all(r"\s+", " ")
              .str.strip_chars()
              .alias("descricao_limpa"),
            texto_busca.alias("texto_busca"),

            pl.col("name").fill_null("").str.to_lowercase().alias("_temp_name_lower"),
            pl.col("description").fill_null("").str.to_lowercase().alias("_temp_description_lower"),
        ])

        # Extração de Skills
        .with_columns([
            pl.when(pl.col("texto_busca").str.contains(tech_regex))
              .then(pl.col("texto_busca").str.extract_all(tech_regex).list.unique().list.sort())
              .otherwise(pl.lit([]))
              .alias("skills_tech"),
            pl.when(pl.col("texto_busca").str.contains(soft_regext))
              .then(pl.col("texto_busca").str.extract_all(soft_regext).list.unique().list.sort())
              .otherwise(pl.lit([]))
              .alias("skills_soft"),
        ])

        # Nível
        .with_columns([
            pl.coalesce([
                pl.when(pl.col("texto_busca").str.contains(nivel_regex.get("lead", r"(?!)"))).then(5),
                pl.when(pl.col("texto_busca").str.contains(nivel_regex.get("senior", r"(?!)"))).then(4),
                pl.when(pl.col("texto_busca").str.contains(nivel_regex.get("pleno", r"(?!)"))).then(3),
                pl.when(pl.col("texto_busca").str.contains(nivel_regex.get("junior", r"(?!)"))).then(2),
                pl.when(pl.col("texto_busca").str.contains(nivel_regex.get("estagio", r"(?!)"))).then(1),
                pl.lit(0)
            ]).cast(pl.Int8).alias("nivel")
        ]))



        # Areas
    PESO_TITULO = 3
    PESO_DESC = 1

    score_exprs = []

        # Para cada área (dados, backend, frontend...), calcula um score
    for area, regex in area_regex.items():
        matches_titulo = pl.col("_temp_name_lower").str.count_matches(regex)
        matches_desc = pl.col("_temp_description_lower").str.count_matches(regex)

        score = (matches_titulo * PESO_TITULO) + (matches_desc * PESO_DESC)
        score_exprs.append(score.alias(f"score_{area}"))

        # Adiciona as colunas de score ao LazyFrame
    lf = lf.with_columns(score_exprs)

        # --- 5. DEFINIR ÁREA PRINCIPAL (VENCEDORA) ---
    cols_scores = [f"score_{a}" for a in area_regex.keys()]

        # 1. Encontra o valor máximo de pontuação entre todas as áreas
    lf = lf.with_columns([
            pl.max_horizontal(*[pl.col(c) for c in cols_scores])  # <- usa Expr, não só nomes
            .fill_null(0)
            .alias("_max_score")
    ])

        # 2. Descobre qual área tem esse score máximo
    when_then_chains = []
    for area in area_regex.keys():
           when_then_chains.append(
                pl.when(pl.col(f"score_{area}") == pl.col("_max_score"))
                .then(pl.lit(area))
            )

        # 3. Define area_principal (Se score for 0, vira "geral")
    lf = lf.with_columns([
            pl.when(pl.col("_max_score") == 0)
            .then(pl.lit("geral"))
            .otherwise(pl.coalesce(when_then_chains))
            .alias("area_principal")
        ])

        # 4. Gera a lista 'areas' com todas as áreas que pontuaram algo (multilabel)
    list_areas_expr = pl.concat_list([
            pl.when(pl.col(f"score_{area}") > 0).then(pl.lit(area))
            for area in area_regex.keys()
        ])

    lf = lf.with_columns([
            list_areas_expr
            .list.drop_nulls()    # remove os nulls da lista
            .list.unique()        # garante áreas únicas
            .alias("areas")
        ])

        # Fallback para 'geral' se a lista ficar vazia
    lf = lf.with_columns([
            pl.when(pl.col("areas").list.len() == 0)
            .then(pl.lit(["geral"]))
            .otherwise(pl.col("areas"))
            .alias("areas")
        ])




        # Flags
    lf = lf.with_columns([
            (pl.col("skills_tech").list.len() + pl.col("skills_soft").list.len()).alias("total_skills"),
            (pl.col("skills_tech").list.len() > 2).alias("is_tech"),
        ])

        # Metadados
    lf = lf.with_columns([
            pl.col("workplaceType").replace({"remote": "remoto", "on-site": "presencial", "hybrid": "híbrido"}).alias("modalidade"),
            pl.concat_str([pl.col("city"), pl.lit(', '), pl.col("state")] ).alias("localizacao"),
            pl.col("careerPageLogo").alias("empresa_logo"),
            pl.col("careerPageUrl").alias("empresa_url")
        ])


        # Partição por publicação
    lf = lf.with_columns([
            pl.col("publishedDate")
              .str.replace("Z", "+00:00")
              .str.to_datetime(time_zone="UTC", strict=False)
              .dt.year()
              .fill_null(2025)
              .alias("ano_publicacao"),

            pl.col("publishedDate")
              .str.replace("Z", "+00:00")
              .str.to_datetime(time_zone="UTC", strict=False)
              .dt.month()
              .cast(pl.Int8)
              .fill_null(1)
              .alias("mes_publicacao"),
        ])

        # Colunas finais
    lf = lf.with_columns([
            pl.col("id").cast(pl.Utf8).alias("vaga_id"),
            pl.col("companyId").cast(pl.Int32).alias("company_id"),
            pl.col("name").alias("titulo"),
            pl.col("careerPageName").alias("empresa"),
            pl.col("jobUrl").alias("url"),
            pl.col("publishedDate").str.replace("Z", "+00:00").str.to_datetime(time_zone="UTC", strict=False).alias("data_publicacao"),
            pl.col("applicationDeadline").str.to_date().alias("data_expiracao"),
            pl.col("disabilities").alias("aceita_pcd"),
            pl.lit("gupy").alias("fonte"),
            pl.lit(datetime.now()).alias("data_processamento"),
        ])
    lf = lf.select([
            "vaga_id", "company_id", "titulo", "empresa", "empresa_logo", "empresa_url",
            "descricao_limpa", "localizacao", "modalidade", "nivel", "area_principal",
            "skills_tech", "skills_soft", "total_skills", "is_tech", "areas",
            "url", "data_publicacao", "data_expiracao", "aceita_pcd",
            "data_processamento", "fonte",
            "ano_publicacao", "mes_publicacao",
            "_horario_ingestao", "_partition_date"
        ])
    lf = lf.collect()

    logger.info(f"Silver gerado com {len(lf)} linhas e colunas: {lf.columns}")
    return lf