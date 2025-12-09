import logging
import yaml
from pathlib import Path
from datetime import datetime
import polars as pl
from src.gold.processors import gold_aggregators

# Configuração de Logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def carregar_config():
    config_path = Path("src/gold/config/gold_config.yaml")
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def main():
    logger.info("INICIANDO PROCESSAMENTO GOLD LAYER")
    config = carregar_config()
    settings = config['settings']

    # 1. Ler TUDO da Silver (Histórico Completo)
    logger.info(f"Lendo Silver Delta Table: {settings['silver_path']}")
    try:
        lf_silver = pl.scan_delta(settings['silver_path'])
    except Exception as e:
        logger.error(f"Erro ao ler Silver. Verifique se o caminho existe. {e}")
        return

    # Cria pasta de saída se não existir
    output_path = Path(settings['gold_output_path'])
    output_path.mkdir(parents=True, exist_ok=True)

    # --- PROCESSAMENTO DAS TABELAS ---

    # 1. DIM VAGAS
    if settings['tables']['dim_vagas']['enabled']:
        logger.info("Gerando dim_vagas...")
        df_dim = gold_aggregators.criar_dim_vagas(lf_silver)

        path_dim = output_path / "dim_vagas"
        df_dim.write_delta(
            str(path_dim),
            mode="overwrite", # Na Gold, geralmente sobrescrevemos dimensões para refletir o estado atual
            delta_write_options={
                "schema_mode": "overwrite",
                "partition_by": settings['tables']['dim_vagas']['partition_by']
            }
        )
        logger.info(f"dim_vagas salva com {df_dim.height} linhas.")

    # 2. AGG SKILLS MONITOR
    if settings['tables']['agg_skills_monitor']['enabled']:
        logger.info("Gerando agg_skills_monitor...")
        df_skills = gold_aggregators.criar_agg_skills_monitor(lf_silver)

        path_skills = output_path / "agg_skills_monitor"
        # Aqui podemos usar append se quisermos histórico dia a dia do ranking
        df_skills.write_delta(
            str(path_skills),
            mode="overwrite",
            delta_write_options={
                "schema_mode": "overwrite",
                "partition_by": settings['tables']['agg_skills_monitor']['partition_by']
            }
        )
        logger.info(f"agg_skills_monitor salva com {df_skills.height} linhas.")

    # 3. AGG MARKET TRENDS
    if settings['tables']['agg_market_trends']['enabled']:
        logger.info("Gerando agg_market_trends...")
        df_trends = gold_aggregators.criar_agg_market_trends(lf_silver)

        path_trends = output_path / "agg_market_trends"
        df_trends.write_delta(
            str(path_trends),
            mode="overwrite",
            delta_write_options={
                "schema_mode": "overwrite",
                "partition_by": settings['tables']['agg_market_trends']['partition_by']
            }
        )
        logger.info(f"agg_market_trends salva com {df_trends.height} linhas.")

    logger.info("PROCESSAMENTO GOLD CONCLUÍDO COM SUCESSO!")

if __name__ == "__main__":
    main()