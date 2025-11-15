from extractors.gupy_scraper import coletar_vagas
from airflow import DAG
from airflow.operators import PythonOperator

default_args = {
    'retries': 3,
    'retry_delay': 300,  # in seconds
}

# Airflow chamando coletar_vagas(paginas = 1), se falhar: paginas = 2, se falhar: paginas = 3
with DAG(
    'gupy_job_scraper',
    default_args=default_args,
    description='A DAG to scrape job postings from Gupy',
    schedule_interval='*/30 * * * *', # a cada 30 minutos
):

    task_scrape_gupy = PythonOperator(
        task_id='scrape_gupy_jobs',
        python_callable=coletar_vagas,
        op_kwargs={'paginas': 1},
    )