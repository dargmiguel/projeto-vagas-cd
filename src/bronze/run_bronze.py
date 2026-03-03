import yaml
import importlib
import logging
import sys
import os
from pathlib import Path
from typing import Dict, Any, Optional

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [ORCHESTRATOR] - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

CONFIG_FILENAME = "bronze_config.yaml"

def find_config_path() -> Path:
    """
    Tenta localizar o arquivo de configuração procurando em locais prováveis.
    """
    script_dir = Path(__file__).resolve().parent

    project_root = script_dir.parents[1]

    candidates = [
        script_dir / "config" / CONFIG_FILENAME,

        project_root / "config" / CONFIG_FILENAME,

        project_root / "src" / "config" / CONFIG_FILENAME,


        Path.cwd() / "config" / CONFIG_FILENAME
    ]

    checked_paths = []

    for path in candidates:
        checked_paths.append(str(path))
        if path.exists():
            logger.info(f"Arquivo de configuração encontrado em: {path}")
            return path

    logger.critical("NÃO FOI POSSÍVEL ENCONTRAR O ARQUIVO DE CONFIGURAÇÃO.")
    logger.critical(f"Procurado nos seguintes caminhos:\n" + "\n".join(checked_paths))
    sys.exit(1)

CONFIG_PATH = find_config_path()

def load_config() -> Dict[str, Any]:
    """Carrega o arquivo YAML de configuração de forma segura."""
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            if config is None:
                logger.warning(f"O arquivo {CONFIG_PATH} está vazio.")
                return {}
            return config
    except Exception as e:
        logger.critical(f"Erro ao ler o arquivo YAML: {e}")
        sys.exit(1)

def run_module(source_name: str, config: Dict[str, Any]):
    """Importa dinamicamente o módulo e executa a função configurada."""
    module_path = config.get("module")
    function_name = config.get("function")

    if not module_path or not function_name:
        logger.error(f"Configuração incompleta para '{source_name}': module ou function faltando.")
        return

    logger.info(f"Iniciando execução de '{source_name}' ({module_path}.{function_name})...")

    try:
        mod = importlib.import_module(module_path)

        if not hasattr(mod, function_name):
            logger.error(f"Função '{function_name}' não encontrada no módulo '{module_path}'.")
            return

        func = getattr(mod, function_name)
        func()

        logger.info(f"Finalizado '{source_name}' com sucesso.")

    except ModuleNotFoundError:
        logger.error(f"Módulo não encontrado: {module_path}. Verifique se o caminho no YAML está correto.")
    except Exception as e:
        logger.error(f"Erro durante a execução de '{source_name}': {e}", exc_info=True)

def main():
    logger.info("Iniciando Orquestrador Bronze...")

    config = load_config()
    sources = config.get("sources", {})

    if not sources:
        logger.warning("Nenhuma fonte definida no arquivo de configuração.")
        return

    for source_name, source_cfg in sources.items():
        if source_cfg.get("enabled", False):
            run_module(source_name, source_cfg)
        else:
            logger.info(f"Fonte '{source_name}' está desativada. Pulando.")

    logger.info("Orquestrador Bronze finalizado.")

if __name__ == "__main__":
    main()