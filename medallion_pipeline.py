from prefect import flow, task
import subprocess
import os
from datetime import date

@task
def bronze_all():
    """Bronze: TODAS as fontes (run_bronze.py)"""
    subprocess.run(['python', 'src/bronze/run_bronze.py'], cwd='/app', check=True)
    print("✅ Bronze (todas fontes)")

@task
def silver_gupy():
    hoje = date.today().strftime("%Y-%m-%d")
    subprocess.run(['python', 'src/silver/run_silver.py', '--date', hoje], cwd='/app', check=True)
    print("✅ Silver")

@task
def gold_aggregates():
    subprocess.run(['python', 'src/gold/run_gold.py'], cwd='/app', check=True)
    print("✅ Gold")

@flow
def medallion_pipeline():
    bronze_all()      # ← MUDOU: orquestrador multi-fonte
    silver_gupy()
    gold_aggregates()

if __name__ == "__main__":
    medallion_pipeline()
