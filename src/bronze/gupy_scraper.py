from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path
import sys
import requests
import time

import polars as pl
from typing import Optional

from dotenv import load_dotenv
import os

from src.utils.checkpoint_manager import CheckpointManager

# Configurar logging
Path("logs").mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/bronze_gupy.log")
    ]
)
logger = logging.getLogger(__name__)


@dataclass(frozen = True)
class Config:

    """Constantes de configuração para o scraper"""

    api_url: str
    limit: int = 100
    max_antigas: int = 10
    sleep_ok: int = 2
    sleep_error: int = 10
    max_paginas_full: int = 50
    max_paginas_incremental: int = 50
    timeout: int = 30
    max_offset: int = 9900


    @classmethod
    def from_env(cls):
        load_dotenv()
        api_url = os.getenv('GUPY_API')
        return cls(api_url=api_url)


# Helpers
from time import perf_counter

def print_metrics(
    paginas,
    max_paginas,
    offset,
    total_vagas,
    novas_pagina,
    antigas_consec,
    data_recente,
    tempo_inicio
):
    duracao = perf_counter() - tempo_inicio
    vel = duracao / (paginas + 1) if paginas > 0 else duracao

    bloco = f"""
=======================================================
📊 Gupy Scraper — Métricas em tempo real
Página: {paginas+1} / {max_paginas}
Offset: {offset}
Vagas coletadas: {total_vagas}
Vagas novas nesta página: {novas_pagina}
Vagas antigas consecutivas: {antigas_consec}
Última data vista: {data_recente}
Tempo total: {duracao:.1f}s
Velocidade média: {vel:.2f} s/página
-------------------------------------------------------
"""

    # limpa bloco anterior (altura igual ao número de linhas)
    linhas = bloco.count("\n")
    sys.stdout.write("\033[F" * linhas)  # move cursor pra cima
    sys.stdout.write(bloco)
    sys.stdout.flush()

def decidir_modo(modo: str, checkpoint: Optional[dict]) -> tuple[str, Optional[str], int]:

    """Decide o modo de execução do scraper"""

    if modo == 'auto':
        if checkpoint is None:
            logger.info("Full load inicial")
            return 'full', None , Config.from_env().max_paginas_full
        logger.info(f" Incremental: Desde {checkpoint['ultima_data_publicacao']}")
        return 'incremental', checkpoint['ultima_data_publicacao'], Config.from_env().max_paginas_incremental
    if modo == 'full':
        logger.info(" Full load solicitado")
        return 'full', None, Config.from_env().max_paginas_full
    if checkpoint is None:
        raise RuntimeError("Checkpoint necessário para modo incremental")


    logger.info(f" Incremental solicitado: Desde {checkpoint['ultima_data_publicacao']}")
    return 'incremental', checkpoint['ultima_data_publicacao'], Config.from_env().max_paginas_incremental

def extrair_vagas(session: requests.Session, api_url: str, offset: int, limit: int, timeout: int ) -> Optional[list]:

    """Faz request à API."""

    try:
        resp = session.get(
            f"{api_url}?limit={limit}&offset={offset}",
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception as e:
        logger.error(f" Erro ao buscar offset {offset}: {e}")
        return None

def validar_e_enriquecer(
    job: dict,
    modo: str,
    ultima_data: Optional[str],
    offset: int,
    ingest_ts: str
) -> Optional[str]:

    """Valida a vaga e retorna (válida?, publication_date)."""

    pub = job.get("publishedDate")
    if not pub:
        return None

    # Validar formato da data
    try:
        datetime.fromisoformat(pub.replace('Z', '+00:00'))
    except ValueError:
        return None

    # Enriquecimento
    job.update({
        "_horario_ingestao": ingest_ts,
        "_source": "gupy",
        "_offset": offset,
        "_modo": modo,
    })

    return pub


def salvar_parquet(vagas: list[dict], modo: str) -> tuple[Path, bool]:

    """Salva as vagas em um arquivo Parquet.
    Faz append no arquivo do dia se existir.
    Retorna (caminho_arquivo, foi_append)."""

    df_novas = pl.DataFrame(vagas)
    hoje = datetime.now()
    bronze_dir = Path(f'data/bronze/gupy/year={hoje.year}/month={hoje.month:02d}')
    bronze_dir.mkdir(parents=True, exist_ok=True)

    nome_arquivo = f'vagas_{hoje.strftime("%Y%m%d")}.parquet'
    caminho_arquivo = bronze_dir / nome_arquivo

    if caminho_arquivo.exists():
        df_existente = pl.read_parquet(caminho_arquivo)
        df_combinado = pl.concat([df_existente, df_novas])
        df_combinado.write_parquet(caminho_arquivo)
        return caminho_arquivo, True

    df_novas.write_parquet(caminho_arquivo)
    logger.info(f" Novo arquivo : {len(vagas)} vagas salvas ")
    return caminho_arquivo, False


def coletar_vagas_bronze(modo: str = 'auto') -> dict:
    """
    Pipeline de coleta de vagas da Gupy.

    Args:
        modo (str): 'auto', 'full' ou 'incremental'.
    Returns:
        dict: Estatísticas da execução.
    """

    # Inicialização
    config = Config.from_env()
    checkpoint_mgr = CheckpointManager(source="gupy_bronze")
    checkpoint = checkpoint_mgr.carregar_checkpoint()
    modo_execucao, ultima_data_conhecida, max_paginas = decidir_modo(modo, checkpoint)

    e_incremental = (modo_execucao == 'incremental')

    # Setup HTTP
    session = requests.Session()
    offset = 0
    paginas = 0
    vagas: list[dict] = []

    antigas_consec = 0          # só métrica
    data_recente = None         # maior publishedDate (string ISO)
    encontrou_antiga = False    # se vimos alguma vaga <= ultima_data_conhecida

    try:
        tempo_inicio = perf_counter()
        # Respeita max_paginas como safety e max_offset da API
        while paginas < max_paginas and offset <= config.max_offset:
            jobs = extrair_vagas(session, config.api_url, offset, config.limit, config.timeout)
            if jobs is None:
                time.sleep(config.sleep_error)
                continue
            if not jobs:
                logger.info("Acabou as vagas disponíveis.")
                break

            ingest_ts = datetime.now().isoformat()
            vagas_novas_pagina = 0

            for job in jobs:
                # valida e enriquece; se der problema de data, volta None
                pub = validar_e_enriquecer(
                    job=job,
                    modo=modo_execucao,
                    ultima_data=ultima_data_conhecida,  # hoje não é usada, mas mantida na assinatura
                    offset=offset,
                    ingest_ts=ingest_ts,
                )

                if not pub:
                    continue

                # atualiza métricas de data
                data_recente = max(data_recente, pub) if data_recente else pub

                # lógica incremental baseada em string ISO
                if e_incremental and ultima_data_conhecida:
                    if pub <= ultima_data_conhecida:
                        # já chegamos em vaga "antiga" (<= checkpoint)
                        encontrou_antiga = True
                        antigas_consec += 1
                        # não adiciona essa vaga
                        continue
                    else:
                        antigas_consec = 0

                # vaga realmente nova
                vagas.append(job)
                vagas_novas_pagina += 1

            # imprime métricas 1x por página
            print_metrics(
                paginas=paginas,
                max_paginas=max_paginas,
                offset=offset,
                total_vagas=len(vagas),
                novas_pagina=vagas_novas_pagina,
                antigas_consec=antigas_consec,
                data_recente=data_recente,
                tempo_inicio=tempo_inicio,
            )

            # critério de parada do incremental: já encontrou vagas antigas
            if e_incremental and encontrou_antiga:
                logger.info(
                    "Chegamos em vagas com data <= ultima_data_conhecida. Encerrando incremental."
                )
                break

            # IMPORTANTE: sempre avançar página e offset aqui
            paginas += 1
            offset += config.limit
            time.sleep(config.sleep_ok)

    finally:
        session.close()

    if not vagas:
        logger.info("Nenhuma vaga coletada.")
        return {
            "status": "sucesso",
            "modo": modo_execucao,
            "total_vagas": 0,
            "caminho_bronze": None,
            "tamanho_mb": 0.0,
            "foi_append": False,
            "data_recente": None,
        }

    caminho_arquivo, foi_append = salvar_parquet(vagas, modo_execucao)
    tamanho_mb = caminho_arquivo.stat().st_size / (1024 * 1024)
    logger.info(f"Arquivo salvo: {caminho_arquivo} ({tamanho_mb:.2f} MB)")

    checkpoint_mgr.salvar_checkpoint(
        ultima_data_publicacao=data_recente,
        total_vagas=len(vagas),
        metadata={
            "modo": modo_execucao,
            "paginas": paginas,
            "arquivo": str(caminho_arquivo),
            "foi_append": foi_append,
        },
    )

    return {
        "status": "sucesso",
        "modo": modo_execucao,
        "total_vagas": len(vagas),
        "caminho_bronze": str(caminho_arquivo),
        "tamanho_mb": tamanho_mb,
        "foi_append": foi_append,
    }

if __name__ == "__main__":
    resultado = coletar_vagas_bronze(modo='auto')
    logger.info(f"Resultado da coleta: {resultado}")