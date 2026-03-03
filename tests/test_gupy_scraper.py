import pytest
from datetime import datetime
from src.bronze.gupy_scraper import validar_e_enriquecer, decidir_modo

def test_validar_e_enriquecer_valido():
    job_mock = {
        "id": "123",
        "name": "Engenheiro",
        "publishedDate": "2025-01-01T10:00:00Z"
    }

    resultado = validar_e_enriquecer(job_mock, modo="full", offset=0)

    assert resultado is not None
    assert "_horario_ingestao" in resultado
    assert "_partition_date" in resultado
    assert resultado["_source"] == "gupy"
    assert resultado["_modo"] == "full"
    assert resultado["_offset"] == 0
    assert resultado["_partition_date"] == datetime.now().date()

def test_validar_e_enriquecer_invalido():
    job_mock_sem_data = {
        "id": "123",
        "name": "Engenheiro"
    }

    assert validar_e_enriquecer(job_mock_sem_data, modo="full", offset=0) is None

    job_mock_data_invalida = {
        "id": "123",
        "name": "Engenheiro",
        "publishedDate": "data-invalida"
    }

    assert validar_e_enriquecer(job_mock_data_invalida, modo="full", offset=0) is None

def test_decidir_modo_com_e_sem_checkpoint(monkeypatch):
    monkeypatch.setenv("GUPY_API", "http://fake-api")

    modo, data, max_pg = decidir_modo("full", checkpoint={"ultima_data_publicacao": "2025-01-01"})
    assert modo == "full"
    assert data is None

    modo, data, max_pg = decidir_modo("auto", checkpoint=None)
    assert modo == "full"
    assert data is None

    modo, data, max_pg = decidir_modo("auto", checkpoint={"ultima_data_publicacao": "2025-01-01"})
    assert modo == "incremental"
    assert data == "2025-01-01"
