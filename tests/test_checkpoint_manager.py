import pytest
import tempfile
import json
from pathlib import Path
from src.utils.checkpoint_manager import CheckpointManager

@pytest.fixture
def temp_checkpoint_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir

def test_checkpoint_manager_save_and_load(temp_checkpoint_dir):
    manager = CheckpointManager(source="test_source", checkpoint_dir=temp_checkpoint_dir)

    assert manager.carregar_checkpoint() is None

    manager.salvar_checkpoint(ultima_data="2025-01-01", total_vagas=100)

    dados_carregados = manager.carregar_checkpoint()

    assert dados_carregados is not None
    assert dados_carregados["source"] == "test_source"
    assert dados_carregados["ultima_data"] == "2025-01-01"
    assert dados_carregados["total_vagas"] == 100

def test_checkpoint_manager_limpar(temp_checkpoint_dir):
    manager = CheckpointManager(source="test_source", checkpoint_dir=temp_checkpoint_dir)
    manager.salvar_checkpoint(foo="bar")

    assert Path(temp_checkpoint_dir, "test_source_checkpoint.json").exists()

    manager.limpar_checkpoint()

    assert not Path(temp_checkpoint_dir, "test_source_checkpoint.json").exists()
    assert manager.carregar_checkpoint() is None
