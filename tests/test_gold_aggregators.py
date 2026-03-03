import polars as pl
from datetime import datetime
from src.gold.processors.gold_aggregators import criar_dim_vagas, criar_agg_skills_monitor, criar_agg_market_trends

def mock_silver_lazyframe():
    df = pl.DataFrame({
        "vaga_id": ["1", "2", "3", "1"],  # "1" duplicado
        "titulo": ["Engenheiro de Dados", "Cientista de Dados", "Desenvolvedor Backend", "Engenheiro de Dados"],
        "descricao_limpa": ["Desc 1", "Desc 2", "Desc 3", "Desc 1"],
        "empresa": ["Empresa A", "Empresa A", "Empresa B", "Empresa A"],
        "localizacao": ["Sao Paulo", "Rio de Janeiro", "Curitiba", "Sao Paulo"],
        "modalidade": ["remoto", "hibrido", "presencial", "remoto"],
        "nivel": [3, 4, 2, 3],
        "area_principal": ["dados", "dados", "backend", "dados"],
        "is_tech": [True, True, True, True],
        "data_expiracao": [datetime(2025, 2, 1).date(), datetime(2025, 2, 1).date(), datetime(2025, 2, 1).date(), datetime(2025, 2, 1).date()],
        "data_publicacao": [datetime(2025, 1, 1), datetime(2025, 1, 2), datetime(2025, 1, 3), datetime(2025, 1, 1)],
        "ano_publicacao": [2025, 2025, 2025, 2025],
        "mes_publicacao": [1, 1, 1, 1],
        "url": ["http://1", "http://2", "http://3", "http://1"],
        "skills_tech": [["python", "sql"], ["python", "r", "sql"], ["java", "sql"], ["python", "sql"]],
        "skills_soft": [["comunicacao"], ["negociacao"], ["comunicacao"], ["comunicacao"]]
    })
    return df.lazy()

def test_criar_dim_vagas():
    lf = mock_silver_lazyframe()
    df_dim = criar_dim_vagas(lf)

    # 1. Deve remover duplicatas pelo vaga_id
    assert df_dim.height == 3

    # 2. Verifica schema base selecionado
    assert "vaga_id" in df_dim.columns
    assert "skills_tech" not in df_dim.columns  # skills não devem ir pra dimensão

def test_criar_agg_skills_monitor():
    lf = mock_silver_lazyframe()
    df_skills = criar_agg_skills_monitor(lf)

    # '1' Python (dados), SQL (dados), Comunicacao (dados)
    # '2' Python (dados), R (dados), SQL (dados), Negociacao (dados)
    # '3' Java (backend), SQL (backend), Comunicacao (backend)
    # Total esperado agrupado. (Lembrando que ID 1 é duplicado, ele repete na explosão, mas o LF original é o mock que será agregado)

    # Python -> 3 (ID 1, ID 2, ID 1)
    # SQL -> 4 (ID 1, ID 2, ID 3, ID 1)
    # R -> 1 (ID 2)

    # Verifica Python na area 'dados' nível '3'
    python_dados_n3 = df_skills.filter(
        (pl.col("skill") == "python") &
        (pl.col("area_principal") == "dados") &
        (pl.col("nivel") == 3)
    )
    assert python_dados_n3["total_vagas"][0] == 2 # Da vaga 1 duplicada

    # Verifica R na area 'dados' nível '4'
    r_dados_n4 = df_skills.filter((pl.col("skill") == "r") & (pl.col("area_principal") == "dados"))
    assert r_dados_n4["total_vagas"][0] == 1

    # Tipagem
    assert "tipo" in df_skills.columns

def test_criar_agg_market_trends():
    lf = mock_silver_lazyframe()
    df_trends = criar_agg_market_trends(lf)

    # Empresa A + Empresa B
    assert "novas_vagas" in df_trends.columns
    assert "empresas_contratando" in df_trends.columns

    # Area de dados, nivel 3, remoto -> Tem vaga 1 e sua duplicata
    tendencia_dados_n3 = df_trends.filter(
        (pl.col("area_principal") == "dados") & (pl.col("nivel") == 3)
    )

    assert tendencia_dados_n3["novas_vagas"][0] == 2
    assert tendencia_dados_n3["empresas_contratando"][0] == 1 # Apenas "Empresa A"
