import argparse
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional, List
from time import perf_counter

import requests
import polars as pl
from dotenv import load_dotenv
import os

from src.utils.checkpoint_manager import CheckpointManager

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ====================== 1. SCHEMA & CONFIG ======================

BRONZE_SCHEMA = {
    "id": pl.String,
    "companyId": pl.Int64,
    "name": pl.String,
    "description": pl.String,
    "careerPageId": pl.Int64,
    "careerPageName": pl.String,
    "careerPageLogo": pl.String,
    "careerPageUrl": pl.String,
    "type": pl.String,
    "publishedDate": pl.String,
    "applicationDeadline": pl.String,
    "isRemoteWork": pl.Boolean,
    "city": pl.String,
    "state": pl.String,
    "country": pl.String,
    "jobUrl": pl.String,
    "badges": pl.List(pl.String),
    "workplaceType": pl.String,
    "disabilities": pl.Boolean,
    "_horario_ingestao": pl.Datetime,
    "_source": pl.String,
    "_offset": pl.Int64,
    "_modo": pl.String,
    "_partition_date": pl.Date,
}

@dataclass(frozen=True)
class Config:
    api_url: str
    limit: int = 100
    max_antigas: int = 10  # Para incremental: quantas vagas antigas seguidas para parar
    sleep_ok: int = 2
    sleep_error: int = 10
    max_paginas_full: int = 99
    max_paginas_incremental: int = 50
    timeout: int = 30
    max_offset: int = 9900  # Limite da API Gupy

    @classmethod
    def from_env(cls):
        load_dotenv()
        api_url = os.getenv("GUPY_API")
        if not api_url:
            raise ValueError("A variável de ambiente GUPY_API não está definida.")
        return cls(api_url=api_url)

# ====================== 2. HELPERS (COLETA) ======================

def print_metrics(paginas, max_paginas, offset, total_vagas, novas_pagina, antigas, data_recente, inicio):
    duracao = perf_counter() - inicio
    vel = duracao / (paginas + 1) if paginas > 0 else duracao
    # \r faz o print sobrescrever a linha anterior (efeito visual de carregamento)
    msg = (
        f"Pg: {paginas+1}/{max_paginas} | Off: {offset} | "
        f"Buffer: {total_vagas} (+{novas_pagina}) | "
        f"Consec. Antigas: {antigas} | Data Recente: {data_recente} | {vel:.2f}s/pg"
    )
    sys.stdout.write(f"\r{msg}")
    sys.stdout.flush()

def extrair_vagas(session: requests.Session, api_url: str, offset: int, limit: int, timeout: int) -> Optional[List[dict]]:
    """Faz a request para a API da Gupy."""
    try:
        resp = session.get(
            f"{api_url}?limit={limit}&offset={offset}",
            timeout=timeout
        )
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception as e:
        logger.error(f"\nErro API (offset {offset}): {e}")
        return None

def validar_e_enriquecer(job: dict, modo: str, offset: int) -> Optional[dict]:
    """Valida data e adiciona as colunas de metadados."""
    pub = job.get("publishedDate")
    if not pub:
        return None

    # Valida se a data é parseável (segurança)
    try:
        datetime.fromisoformat(pub.replace("Z", "+00:00"))
    except ValueError:
        return None

    agora = datetime.now()

    # AQUI ESTÁ O SEGREDO: Criamos as duas colunas
    job.update({
        "_horario_ingestao": agora,          # Datetime completo
        "_partition_date": agora.date(),     # Apenas a data (para a pasta)
        "_source": "gupy",
        "_offset": offset,
        "_modo": modo,
    })
    return job

# ====================== 3. SALVAR EM DELTA ======================

def salvar_bronze_delta(vagas: list[dict], modo_pipeline: str) -> Path:
    """Salva os dados em formato Delta Lake."""
    base_path = Path("data/bronze_delta/gupy")

    if not vagas:
        return base_path

    df = pl.DataFrame(vagas, schema=BRONZE_SCHEMA, orient="row")

    base_path.mkdir(parents=True, exist_ok=True)


    modo_escrita: Literal["append", "overwrite"] = "overwrite" if modo_pipeline == "full" else "append"

    logger.info(f"\nSalvando {len(df)} vagas em Delta ({modo_escrita})...")

    df.write_delta(
        str(base_path),
        mode=modo_escrita,
        # Particionamento por DIA para evitar excesso de arquivos
        delta_write_options={
            "schema_mode": "merge",  # Aceita colunas novas no futuro
            "partition_by": ["_partition_date"],
        }
    )
    return base_path

# ====================== 4. CORE PIPELINE ======================

def decidir_modo(modo: str, checkpoint: dict | None) -> tuple[str, str | None, int]:
    config = Config.from_env()

    if modo == "full":
        return "full", None, config.max_paginas_full

    # Se for auto e não tiver checkpoint, força full
    if modo == "auto" and not checkpoint:
        logger.info("Sem checkpoint anterior. Iniciando FULL LOAD.")
        return "full", None, config.max_paginas_full

    # Incremental
    ultima_data = checkpoint.get("ultima_data_publicacao")
    logger.info(f"Iniciando INCREMENTAL a partir de: {ultima_data}")
    return "incremental", ultima_data, config.max_paginas_incremental

def coletar_vagas_bronze(modo: str = "auto"):
    # Setup
    config = Config.from_env()
    ckpt_mgr = CheckpointManager(source="gupy_bronze")
    checkpoint = ckpt_mgr.carregar_checkpoint()

    modo_exec, ultima_data_known, max_paginas = decidir_modo(modo, checkpoint)
    is_incremental = (modo_exec == "incremental")

    session = requests.Session()
    vagas_buffer = []

    # Estado do Loop
    offset = 0
    paginas = 0
    antigas_consec = 0
    data_recente = None
    stop_signal = False
    inicio = perf_counter()

    logger.info(f"Iniciando coleta (Modo: {modo_exec})...")

    try:
        while paginas < max_paginas and offset <= config.max_offset:
            jobs = extrair_vagas(session, config.api_url, offset, config.limit, config.timeout)

            if jobs is None: # Erro API
                time.sleep(config.sleep_error)
                continue

            if not jobs: # Lista vazia = fim
                logger.info("\nAPI não retornou mais vagas.")
                break

            novas_nesta_pg = 0

            for job in jobs:
                job_processed = validar_e_enriquecer(job, modo_exec, offset)
                if not job_processed: continue

                pub_date = job_processed['publishedDate']

                # Rastreia data mais recente vista nesta execução
                if data_recente is None or pub_date > data_recente:
                    data_recente = pub_date

                # Lógica de Parada Incremental
                if is_incremental and ultima_data_known and pub_date <= ultima_data_known:
                    antigas_consec += 1
                    if antigas_consec >= config.max_antigas:
                        stop_signal = True
                        break # Sai do loop de vagas
                    continue # Ignora vaga velha, mas continua vendo a página
                else:
                    antigas_consec = 0 # Reset se achar uma nova

                vagas_buffer.append(job_processed)
                novas_nesta_pg += 1

            # Feedback visual
            print_metrics(paginas, max_paginas, offset, len(vagas_buffer), novas_nesta_pg, antigas_consec, data_recente, inicio)

            if stop_signal:
                logger.info("\nLimite incremental atingido (vagas antigas encontradas).")
                break

            paginas += 1
            offset += config.limit
            time.sleep(config.sleep_ok)

    except KeyboardInterrupt:
        logger.warning("\nInterrompido pelo usuário. Salvando o que foi coletado...")

    finally:
        session.close()
        print()

    # Salvar e Checkpoint
    caminho = salvar_bronze_delta(vagas_buffer, modo_pipeline=modo_exec)

    if vagas_buffer:
        ckpt_mgr.salvar_checkpoint(
            ultima_data_publicacao=data_recente,
            total_vagas=len(vagas_buffer),
            metadata={
                "modo": modo_exec,
                "delta_path": str(caminho),
                "particao": str(datetime.now().date())
            }
        )
        logger.info(f"Sucesso! Checkpoint atualizado com data {data_recente}")
    else:
        logger.info("Nenhuma vaga nova para salvar.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["auto", "full", "incremental"], default="auto")
    args = parser.parse_args()

    coletar_vagas_bronze(modo=args.mode)