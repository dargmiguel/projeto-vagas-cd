import polars as pl

from silver.processors.gupy_processor import TextProcessor


def test_limpar_texto():
    df = pl.DataFrame(
        {
            "texto": [
                "Olá&nbsp;Mundo",
                "Foo\u00A0Bar",              # NBSP
                "  muitos   espaços   ",
                None,
            ]
        }
    )

    df_limpo = df.with_columns(
        TextProcessor.limpar_texto(pl.col("texto")).alias("limpo")
    )

    assert df_limpo["limpo"].to_list() == [
        "Olá Mundo",      # remove &nbsp; e normaliza espaços
        "Foo Bar",        # NBSP -> espaço normal
        "muitos espaços", # múltiplos espaços viram um
        "",               # None vira string vazia
    ]


def test_normalizar_texto_minusculo_sem_acentos():
    entrada = "Engenheiro de Dados Sênior"
    saida = TextProcessor.normalizar_texto(entrada)
    assert saida == "engenheiro de dados senior"
