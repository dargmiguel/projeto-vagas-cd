import importlib
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

import polars as pl

# from src.utils.checkpoint_manager import CheckpointManager

logger = logging.getLogger(__name__)
BASE_DIR = Path(os.getenv("DATA_PATH", "data"))

def carregar_bronze(bronze_path: str, data: Optional[datetime] = None) -> Optional[pl.DataFrame]:
    """Carrega dados da camada bronze."""
    if data is None:
        data = datetime.now()

    try:
        caminho_ajustado = str(bronze_path).replace("data/", "").replace("data\\", "")
        full_path = BASE_DIR / caminho_ajustado

        df = pl.scan_delta(str(full_path))\
            .filter(pl.col("_partition_date") == data.date())\
            .collect()

        df_unique = df.sort("_horario_ingestao", descending=True).unique(subset=["id"], keep="first")

        if df.is_empty():
            logger.warning(f"Nenhum dado encontrado na camada bronze para a data {data.strftime('%Y-%m-%d')}.")
            return None

        logger.info(f"Dados bronze carregados com {df_unique.height} registros únicos.")
        return df_unique
    except FileNotFoundError:
        logger.warning(
            f"Arquivo não encontrado para a data {data.strftime('%Y-%m-%d')}. "
            f"Pulando processamento."
        )
        return None
    except Exception as e:
        logger.error(f"Erro ao carregar dados bronze: {e}")
        return None

def processar_source(source_name: str,
                      source_config: dict,
                      global_config: dict,
                      data: Optional[datetime] = None) -> Dict[str,Any]:

    """Processa dados de uma fonte específica."""
    logger.info(f"Iniciando processamento para a fonte: {source_name}")
    if not source_config.get("enabled", True):
        logger.info(f"Processamento desabilitado para a fonte: {source_name}. Pulando...")
        return {"status": "skipped", "reason": "disabled in config"}

    try:
        module_path = f'.{source_config["processor_module"]}'
        processador_module = importlib.import_module(module_path, package='src.silver.processors')
        processar = processador_module.processar_vagas
    except (ImportError, AttributeError) as e:
        logger.error(f"Erro ao importar módulo de processamento para a fonte {source_name}: {e}")
        return {"status": "error", "reason": "import_error"}

    df_bronze = carregar_bronze(source_config["bronze_path"], data)
    if df_bronze is None or df_bronze.is_empty():
        return {"status": "no_data", "reason": "no_bronze_data"}


    # Processa os dados usando a função do processador
    try:
        df_silver = processar(df_bronze, global_config['settings'])

    except Exception as e:
        logger.error(f"Erro ao processar dados para a fonte {source_name}: {e}")
        return {"status": "error", "reason": "processing_error"}

    if df_silver is None or df_silver.is_empty() :
        logger.warning(f"Nenhum dado processado para a fonte {source_name}.")
        return {"status": "no_data", "reason": "no_processed_data"}

    # Salva os dados processados
    path_config = global_config['settings']['silver_output_path']
    caminho_limpo = str(path_config).replace("data/", "").replace("data\\", "")

    silver_root = (BASE_DIR / caminho_limpo / source_name).resolve()
    silver_root.mkdir(parents=True, exist_ok=True)


    overwrite_flag = global_config["settings"].get("silver_overwrite", False)

    write_mode = "overwrite" if overwrite_flag else "append"
    logger.info(f"Modo de escrita Delta (silver): {write_mode}")

    df_silver.write_delta(
        silver_root.as_posix(),
        mode=write_mode,
        storage_options={"allow_unsafe_rename": "true"},
        delta_write_options={
            "schema_mode": "overwrite",
            "partition_by": ["ano_publicacao", "mes_publicacao"]
        }
    )

    logger.info(f"Processamento concluído para a fonte {source_name}. -> {df_silver.height} vagas.")

    return{
        "status": "sucesso",
        "total_vagas_processadas": df_silver.height,
    }