from prefect import flow, task
import subprocess
import os
import sys
from pathlib import Path

# Configuração para garantir que logs apareçam imediatamente
os.environ["PYTHONUNBUFFERED"] = "1"

# Calcula a raiz do projeto (onde está a pasta src)
# Se este arquivo está em src/pipeline.py, a raiz é o parent
PROJECT_ROOT = Path(__file__).resolve().parent.parent

def run_command(command_list):
    """
    Função auxiliar para rodar comandos shell/python de forma segura,
    garantindo que o PYTHONPATH inclua a raiz do projeto.
    """
    # Adiciona a raiz do projeto ao PYTHONPATH do ambiente de execução
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")

    # Executa o comando
    result = subprocess.run(
        command_list,
        cwd=str(PROJECT_ROOT), # Garante que roda a partir da raiz
        env=env,               # Passa o ambiente com PYTHONPATH corrigido
        check=True,            # Lança erro se o script falhar
        text=True              # Garante output de texto
    )
    return result

@task(name="Bronze Layer (Ingestão)")
def bronze_gupy():
    print("--- Iniciando Bronze: Gupy ---")
    # Usa python -m para garantir que imports relativos funcionem
    run_command([sys.executable, '-m', 'src.bronze.gupy_scraper', '--mode', 'auto'])
    print("✅ Bronze Concluído")

@task(name="Silver Layer (Limpeza)")
def silver_gupy():
    from datetime import date
    hoje = date.today().strftime("%Y-%m-%d")
    print(f"--- Iniciando Silver: Processando {hoje} ---")

    script_path = PROJECT_ROOT / "src" / "silver" / "run_silver.py"

    if not script_path.exists():
        print(f"⚠️ Script Silver não encontrado em {script_path}. Pulando...")
        return

    run_command([sys.executable, str(script_path), '--date', hoje])
    print("✅ Silver Concluído")

@task(name="Gold Layer (Agregação)")
def gold_aggregates():
    print("--- Iniciando Gold: Filtros Tech ---")

    script_path = PROJECT_ROOT / "src" / "gold" / "run_gold.py"

    if not script_path.exists():
         print(f"⚠️ Script Gold não encontrado em {script_path}. Pulando...")
         return

    run_command([sys.executable, str(script_path)])
    print("✅ Gold Concluído")

@flow(name="vagas_pipeline")
def medallion_pipeline():
    # Execução sequencial
    bronze_gupy()
    silver_gupy()
    gold_aggregates()

if __name__ == "__main__":
    medallion_pipeline.serve(
        name= "30min",
        interval = 1800,  # Executa a cada 30 minutos
        tags = ["medallion", "vagas"]
    )
