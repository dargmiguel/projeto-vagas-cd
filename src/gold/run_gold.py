import argparse
import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

def construir_fato_vaga(lf_silver: pl.LazyFrame) -> pl.DataFrame:
    """
    Constrói a tabela fato_vaga a partir do LazyFrame lf_silver.

    Args:
        lf_silver (pl.LazyFrame): LazyFrame contendo os dados da camada silver.

    Returns:
        pl.DataFrame: DataFrame contendo a tabela fato_vaga.
    """
