import polars as pl
from datetime import datetime

def criar_dim_vagas(lf: pl.LazyFrame) -> pl.DataFrame:
    """Cria a dimensão de vagas a partir do LazyFrame fornecido.

    Args:
        lf (pl.LazyFrame): LazyFrame contendo os dados das vagas.

    Returns:
        pl.DataFrame: DataFrame da dimensão de vagas.
    """
    return (
        lf
        .select(["vaga_id", "titulo","descricao_limpa", "empresa", "localizacao", "modalidade",
            "nivel" ,"area_principal", "is_tech","data_expiracao",
            "data_publicacao", "ano_publicacao", "mes_publicacao",
            "url", "skills_tech", "skills_soft"])
            .unique(subset=["vaga_id"])
        .collect()
    )

def criar_agg_skills_monitor(lf: pl.LazyFrame) -> pl.DataFrame:
    """Cria a agregação de skills por área a partir do LazyFrame fornecido.

    Args:
        lf (pl.LazyFrame): LazyFrame contendo os dados das vagas.

    Returns:
        pl.DataFrame: DataFrame com a agregação de skills por área.
    """
    tech = (
        lf
        .select(["vaga_id", "area_principal", "nivel", "skills_tech"])
        .explode("skills_tech")
        .drop_nulls("skills_tech")
        .rename({"skills_tech": "skill"})
        .with_columns(pl.lit("tech").alias("tipo"))
    )

    # 2. Explode Soft Skills
    soft = (
        lf
        .select(["vaga_id", "area_principal", "nivel", "skills_soft"])
        .explode("skills_soft")
        .drop_nulls("skills_soft")
        .rename({"skills_soft": "skill"})
        .with_columns(pl.lit("soft").alias("tipo"))
    )

    # 3. Une e Agrega
    combined = pl.concat([tech, soft])

    return (
        combined
        .group_by(["area_principal", "nivel", "tipo", "skill"])
        .agg([
            pl.len().alias("total_vagas"),
            # Calcula % de penetração (Ex: Python aparece em 80% das vagas de dados)
            # Isso exigiria um join complexo com o total por area, faremos simples por enquanto
        ])
        .with_columns([
            pl.lit(datetime.now().date()).alias("data_referencia")
        ])
        .sort(["area_principal", "total_vagas"], descending=True)
        .collect()
    )

def criar_agg_market_trends(lf: pl.LazyFrame) -> pl.DataFrame:
    """
    Cria tabela de tendências temporais.
    Agrupado por Ano/Mês, Área e Nível.
    """
    return (
        lf
        .group_by(["ano_publicacao", "mes_publicacao", "area_principal", "nivel", "modalidade"])
        .agg([
            pl.len().alias("novas_vagas"),
            pl.col("empresa").n_unique().alias("empresas_contratando"),
        ])
        .rename({"ano_publicacao": "ano", "mes_publicacao": "mes"})
        .sort(["ano", "mes", "novas_vagas"], descending=True)
        .collect()
    )