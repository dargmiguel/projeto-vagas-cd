# tests/test_feature_extractor.py
from datetime import datetime

import polars as pl

from silver.processors.gupy_processor import FeatureExtractor


def _config_minimo():
    return {
        "skills_tech": ["python", "sql"],
        "skills_soft": ["comunicacao", "trabalho em equipe"],
        "niveis": {
            "estagio": ["estagio", "estágio", "intern"],
            "junior": ["junior", "jr"],
            "pleno": ["pleno"],
            "senior": ["senior", "sr"],
        },
        "areas": {
            "dados": ["dados", "analytics", "engenheiro de dados"],
            "backend": ["backend", "api"],
        },
    }


def test_feature_extractor_basico():
    config = _config_minimo()
    extractor = FeatureExtractor(config)

    df_bronze = pl.DataFrame(
        {
            "id": ["1"],
            "companyId": ["123"],
            "name": ["Engenheiro de Dados Jr"],
            "careerPageName": ["Empresa X"],
            "careerPageLogo": ["https://logo"],
            "careerPageUrl": ["https://empresa-x"],
            "description": [
                """
                Buscamos Engenheiro de Dados com experiência em Python e SQL.
                Trabalho em equipe é essencial.
                """
            ],
            "city": ["São Paulo"],
            "state": ["SP"],
            "country": ["Brasil"],
            "publishedDate": [datetime(2025, 1, 1).isoformat()],
            "applicationDeadline": [None],
            "jobUrl": ["https://vaga"],
            "disabilities": [False],
            "isRemoteWork": [True],
            "workplaceType": ["remote"],
        }
    )

    lf_silver = extractor.aplicar(df_bronze.lazy())
    df_silver = lf_silver.collect()

    assert df_silver.height == 1

    row = df_silver.row(0, named=True)

    # skills_tech deve conter python e sql
    assert set(row["skills_tech"]) >= {"python", "sql"}

    # skills_soft deve pegar "trabalho em equipe"
    assert "trabalho em equipe" in row["skills_soft"]

    # nível: token encontrado pelo regex (sem padronização ainda)
    assert row["nivel"] == "2"

    # areas: garantir que não está vazio
    assert len(row["areas"]) > 0

    # flags
    assert row["is_tech"] is True
    assert isinstance(row["is_dados"], bool)

    # modalidade e localizacao
    assert row["modalidade"] == "remoto"
    assert row["localizacao"] == "São Paulo, SP"
