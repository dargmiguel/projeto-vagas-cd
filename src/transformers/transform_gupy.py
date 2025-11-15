import os
import polars as pl
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def carregar_bronze():
    bronze_path = Path(os.getenv('BRONZE_PATH'))
    return(
        pl.scan_parquet(str(bronze_path) + "/*.parquet")
    )

def normalizar_colunas(df: pl.LazyFrame) -> pl.LazyFrame:

