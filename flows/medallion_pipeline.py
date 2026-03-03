from prefect import flow, task
import subprocess
import os
import sys
from pathlib import Path

os.environ["PYTHONUNBUFFERED"] = "1"

from dotenv import load_dotenv
load_dotenv()
INTERVAL_PIPELINE = int(os.getenv("INTERVAL_PIPELINE", "1800"))  # Padrão: 1800 segundos (30 minutos)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

def run_command(command_list):
    """
    Função auxiliar para rodar comandos shell/python de forma segura,
    garantindo que o PYTHONPATH inclua a raiz do projeto.
    """
    # Adiciona a raiz do projeto ao PYTHONPATH do ambiente de execução
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")


    result = subprocess.run(
        command_list,
        cwd=str(PROJECT_ROOT),
        env=env,
        check=True,
        text=True
    )
    return result

@task(name="Bronze Layer (Ingestão)")
def bronze_gupy():
    print("--- Iniciando Bronze: Gupy ---")
    run_command([sys.executable, '-m', 'src.bronze.gupy_scraper', '--mode', 'auto'])
    print("Bronze Concluído")

@task(name="Silver Layer (Limpeza)")
def silver_gupy():
    from datetime import date
    hoje = date.today().strftime("%Y-%m-%d")
    print(f"--- Iniciando Silver: Processando {hoje} ---")

    script_path = PROJECT_ROOT / "src" / "silver" / "run_silver.py"

    if not script_path.exists():
        print(f"Script Silver não encontrado em {script_path}. Pulando...")
        return

    run_command([sys.executable, str(script_path), '--date', hoje])
    print("Silver Concluído")

@task(name="Gold Layer (Agregação)")
def gold_aggregates():
    print("--- Iniciando Gold: Filtros Tech ---")

    script_path = PROJECT_ROOT / "src" / "gold" / "run_gold.py"

    if not script_path.exists():
         print(f" cript Gold não encontrado em {script_path}. Pulando...")
         return

    run_command([sys.executable, str(script_path)])
    print("Gold Concluído")

@flow(name="vagas_pipeline")
def medallion_pipeline():
    bronze_gupy()
    silver_gupy()
    gold_aggregates()

if __name__ == "__main__":
    medallion_pipeline.serve(
        name="docker-deployment",
        interval=INTERVAL_PIPELINE,
        tags=["medallion", "vagas"]
    )

