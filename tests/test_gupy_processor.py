import polars as pl
from datetime import datetime
from src.silver.processors.gupy_processor import _build_regex, processar_vagas

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

def test_build_regex():
    termos = ["python", "c++", "trabalho em equipe", "a"]
    padrao = _build_regex(termos)

    # "trabalho em equipe" (frase -> sem \b)
    assert "trabalho\\ em\\ equipe" in padrao
    # "python" (palavra simples -> com \b)
    assert r"\bpython\b" in padrao
    # Ignora strings curtas como "a"
    assert r"\ba\b" not in padrao

def test_processar_vagas():
    config = _config_minimo()

    df_bronze = pl.DataFrame(
        {
            "id": ["1"],
            "companyId": [123],
            "name": ["Engenheiro de Dados Jr"],
            "careerPageName": ["Empresa X"],
            "careerPageLogo": ["https://logo"],
            "careerPageUrl": ["https://empresa-x"],
            "description": [
                "Buscamos Engenheiro de Dados com experiência em Python e SQL. Trabalho em equipe é essencial."
            ],
            "city": ["São Paulo"],
            "state": ["SP"],
            "country": ["Brasil"],
            "publishedDate": ["2025-01-01T10:00:00Z"],
            "applicationDeadline": ["2025-02-01"],
            "jobUrl": ["https://vaga"],
            "disabilities": [False],
            "isRemoteWork": [True],
            "workplaceType": ["remote"],
            "_horario_ingestao": [datetime(2025, 1, 1, 12, 0, 0)],
            "_partition_date": [datetime(2025, 1, 1).date()],
        }
    )

    df_silver = processar_vagas(df_bronze, config)

    assert df_silver.height == 1

    row = df_silver.row(0, named=True)

    # skills_tech deve conter python e sql (lower case validation)
    assert set(row["skills_tech"]) >= {"python", "sql"}

    # skills_soft deve pegar "trabalho em equipe"
    assert "trabalho em equipe" in row["skills_soft"]

    # nível: 2 para junior (de acordo com as lógicas no gupy_processor)
    assert row["nivel"] == 2

    # areas principais: dados
    assert row["area_principal"] == "dados"

    # total skills
    assert row["total_skills"] == 3 # python, sql, trabalho em equipe

    # modalidade e localizacao
    assert row["modalidade"] == "remoto"
    assert row["localizacao"] == "São Paulo, SP"
