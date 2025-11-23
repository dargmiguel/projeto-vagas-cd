import importlib
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

import polars as pl

# from src.utils.checkpoint_manager import CheckpointManager

logger = logging.getLogger(__name__)


def carregar_bronze(bronze_path: str, data: Optional[datetime] = None) -> Optional[pl.DataFrame]:
    """Carrega dados da camada bronze.

    Args:
        bronze_path (str): Caminho para os dados bronze.
        data (Optional[datetime], optional): Data específica para carregar. Defaults to None.

    Returns:
        Optional[pl.DataFrame]: DataFrame carregado ou None se não houver dados.
    """
    if data is None:
        data = datetime.now()

    path_pattern = f"{bronze_path}/year={data.year}/month={data.month:02d}/vagas_{data.strftime('%Y%m%d')}.parquet"
    logger.info(f"Carregando dados bronze de: {path_pattern}")

    try:
        lf = pl.scan_parquet(path_pattern)
        lf_unique = (lf.sort("_horario_ingestao", descending=True).unique(subset=["id"], keep="first"))
        df = lf_unique.collect()
        if df.is_empty():
            logger.warning(f"Nenhum dado encontrado na camada bronze para a data {data.strftime('%Y-%m-%d')}.")
            return None
        logger.info(f"Dados bronze carregados com {df.height} registros únicos.")
        return df
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

    """Processa dados de uma fonte específica.
    Args:
        source_name (str): Nome da fonte.
        source_config (dict): Configuração específica da fonte.
        global_config (dict): Configuração global do pipeline.
        data (Optional[datetime], optional): Data para processamento. Defaults to None.
    Returns:
        Dict[str,Any]: Dicionário com resultados do processamento.
    """
    logger.info(f"Iniciando processamento para a fonte: {source_name}")
    if not source_config.get("enabled", True):
        logger.info(f"Processamento desabilitado para a fonte: {source_name}. Pulando...")
        return {"status": "skipped", "reason": "disabled in config"}
    # checkpoint_mgr = CheckpointManager(source=source_name)

    # Carrega o processador especifico
    try:
        module_path = f'.{source_config["processor_module"]}'
        processador_module = importlib.import_module(module_path, package='src.silver.processors')
        processador_func = processador_module.processar_vagas
    except (ImportError, AttributeError) as e:
        logger.error(f"Erro ao importar módulo de processamento para a fonte {source_name}: {e}")
        return {"status": "error", "reason": "import_error"}

    # Carrega dados bronze
    df_bronze = carregar_bronze(source_config["bronze_path"], data)
    if df_bronze is None or len(df_bronze) == 0:
        return {"status": "no_data", "reason": "no_bronze_data"}

    # Processa os dados usando a função do processador
    try:
        df_silver = processador_func(df_bronze, global_config['settings'])
    except Exception as e:
        logger.error(f"Erro ao processar dados para a fonte {source_name}: {e}")
        return {"status": "error", "reason": "processing_error"}

    if len(df_silver) == 0:
        logger.warning(f"Nenhum dado processado para a fonte {source_name}.")
        return {"status": "no_data", "reason": "no_processed_data"}

    # Salva os dados processados
    data_dos_dados = data or datetime.now()

    output_dir = Path(global_config['settings']['silver_output_path']) / source_name
    output_dir = output_dir / f"year={data_dos_dados.year}/month={data_dos_dados.month:02d}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f'vagas_{data_dos_dados.strftime("%Y%m%d")}.parquet'
    df_silver.write_parquet(output_file)

    tamanho_mb = output_file.stat().st_size / (1024 * 1024)
    # checkpoint_mgr.salvar_checkpoint(
    #     data_processamento = data_dos_dados.isoformat(),
    #     ultima_data_processada = datetime.now().isoformat(),
    #     total_vagas_processadas = len(df_silver),
    #     metadata = {"arquivo": str(output_file), "tamanho_mb": round(tamanho_mb, 2)}
    # )
    logger.info(f"Processamento concluído para a fonte {source_name}. Dados salvos em {output_file} ({tamanho_mb:.2f} MB).")

    return{
        "status": "sucesso",
        "total_vagas_processadas": df_silver.height,
        "arquivo_salvo": str(output_file),
        "tamanho_mb": round(tamanho_mb, 2)
    }