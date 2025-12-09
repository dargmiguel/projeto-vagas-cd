import argparse
import os  # <--- Faltava esse import
from datetime import datetime, timedelta
import logging
from pathlib import Path
from typing import Optional
import yaml
import importlib # Movi para o topo por boa prática, mas funciona dentro do if também
import polars as pl # Movi para o topo por boa prática

from src.silver.processors.silver_processor import carregar_bronze, processar_source

# 1. Define o diretório base (Mundo dos Dados)
BASE_DIR = Path(os.getenv("DATA_PATH", "data"))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def carregar_config() -> dict:
    # Ajuste o número de .parent dependendo de onde este arquivo run_Silver.py está
    # Se ele está em src/scripts/run_Silver.py:
    # .parent (scripts) -> .parent (src) -> / silver / config
    config_path = Path(__file__).parent.parent / "silver" / "config" / "silver_config.yaml"
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Erro ao carregar config: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Processa camada Silver")
    parser.add_argument("--date", type=str, help="Data específica (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="Força overwrite em todas as fontes")
    parser.add_argument("--data-inicio", type=str, help="Data inicial para --full (YYYY-MM-DD)")
    args = parser.parse_args()

    config = carregar_config()
    config.setdefault("settings", {})

    config["settings"]["silver_overwrite"] = args.full
    logger.info(f"Modo full_reprocess: {args.full}")

    if args.full:
        inicio_str = args.data_inicio or "2025-11-01"
        inicio = datetime.strptime(inicio_str, "%Y-%m-%d")
        datas = [(inicio + timedelta(days=i)) for i in range((datetime.now() - inicio).days + 1)]
        logger.info(f"Full reprocess ativado → {len(datas)} dias desde {inicio_str}")
    else:
        data_especifica = datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.now()
        datas = [data_especifica]
        logger.info(f"Processamento normal → {data_especifica.date()}")

    # --- Bloco Full Reprocess ---
    if args.full:
        todos_silvers = {}
        for dt in datas:
            logger.info(f"Coletando bronze → {dt.date()}")
            for source_name, source_cfg in config.get("sources", {}).items():
                if not source_cfg.get("enabled", True):
                    continue
                try:
                    # DICA: Se bronze_path no yaml for só o nome da pasta (ex: "linkedin"),
                    # você pode fazer: carregar_bronze(BASE_DIR / "bronze" / source_cfg["bronze_path"], dt)
                    df_bronze = carregar_bronze(source_cfg["bronze_path"], dt)

                    if df_bronze is not None and not df_bronze.is_empty():
                        logger.info(f"{source_name}: {df_bronze.height} linhas bronze")

                        module_path = f'.{source_cfg["processor_module"]}'
                        processador_module = importlib.import_module(module_path, package='src.silver.processors')
                        processar = processador_module.processar_vagas

                        df_silver = processar(df_bronze, config['settings'])

                        if source_name not in todos_silvers:
                            todos_silvers[source_name] = []
                        todos_silvers[source_name].append(df_silver)
                except Exception as e:
                    logger.error(f"Erro no full reprocess {source_name}: {e}")

        # Consolidação e Escrita (AQUI ESTA A MUDANÇA PRINCIPAL)
        for source_name, silvers in todos_silvers.items():
            if silvers:
                df_consolidado = (
                    pl.concat(silvers, how="vertical_relaxed")
                    .sort("_horario_ingestao", descending=True)
                    .unique(subset=["vaga_id"], keep="first")
                )
                logger.info(f"{source_name}: {len(silvers)} batches → {df_consolidado.height} vagas únicas")

                # --- CORREÇÃO APLICADA AQUI ---
                # Removemos a dependência do config yaml e usamos a estrutura padrão
                silver_root = BASE_DIR / "silver" / source_name
                silver_root.mkdir(parents=True, exist_ok=True)

                df_consolidado.write_delta(
                    str(silver_root),
                    mode="overwrite",
                    delta_write_options={
                        "schema_mode": "overwrite",
                        "partition_by": ["ano_publicacao", "mes_publicacao"]
                    }
                )
                logger.info(f"{source_name}: {df_consolidado.height} vagas gravadas em {silver_root}")

    # --- Bloco Processamento Normal (Incremental) ---
    else:
        for dt in datas:
            logger.info(f"\n{'='*60}")
            logger.info(f"PROCESSANDO SILVER → {dt.date()} | full_reprocess={args.full}")
            logger.info(f"{'='*60}")

            for source_name, source_cfg in config.get("sources", {}).items():
                if not source_cfg.get("enabled", True):
                    logger.info(f"Fonte {source_name} desabilitada → pulando")
                    continue

                try:
                    logger.info(f"\nProcessando fonte: {source_name}")
                    # ATENÇÃO: Verifique se a função 'processar_source' dentro de silver_processor.py
                    # também está usando BASE_DIR para salvar, ou se ela ainda lê do config['silver_output_path'].
                    # Se ela ler do config, você precisará editar o arquivo silver_processor.py também.
                    resultado = processar_source(source_name, source_cfg, config, dt)

                    logger.info(f"Resultado: {resultado}")
                    status = resultado.get("status", "erro")
                    logger.info(f"Fonte {source_name} processada com status: {status}")
                    total = resultado.get("total_vagas_processadas", 0)
                    logger.info(f"{source_name} → {status} ({total} vagas)")
                except Exception as e:
                    logger.error(f"Erro crítico na fonte {source_name}: {e}")

    logger.info("\nProcessamento Silver concluído!")


if __name__ == "__main__":
    main()