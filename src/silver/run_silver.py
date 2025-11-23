
import argparse
from datetime import datetime
import logging
from pathlib import Path

from typing import Optional
import yaml

from src.silver.processors.silver_processor import processar_source


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def carregar_config() -> dict:
    """Carrega a configuração do pipeline a partir do arquivo YAML.

    Returns:
        dict: Configuração carregada.
    """
    config_path = Path(__file__).parent.parent / "silver" /  "config" / "silver_config.yaml"
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        logger.error(f"Arquivo de configuração não encontrado: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Erro ao carregar o arquivo de configuração: {e}")
        raise

def processar_silver(data: Optional[datetime] = None):
    """Processa todas as fontes habilitadas para camada silver"""

    logger.info("Iniciando processamento da camada silver...")
    config = carregar_config()
    resultados = {}
    for source_name, source_config in config.get("sources", {}).items():
        try:
            resultado = processar_source(source_name, source_config, config, data)
            resultados[source_name] = resultado
        except Exception as e:
            logger.error(f"Erro ao processar a fonte {source_name}: {e}")
            resultados[source_name] = {"status": "error", "error": str(e)}

    logger.info("Resumo do processamento silver: \t")
    for source, resultado in resultados.items():
        status = resultado.get("status", "desconhecido")
        total = resultado.get("total_vagas_processadas", 0)
        logger.info(f"Fonte: {source} | Status: {status} | Total Processadas: {total}")
    return resultados

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        help="Data para processar no formato YYYY-MM-DD. Se não fornecido, usa a data atual.",
        default=None,

    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    data_ref: Optional[datetime] = None
    if args.date:
        try:
            data_ref = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            logger.error("Formato de data inválido. Use YYYY-MM-DD.")
            exit(1)
    processar_silver(data_ref)