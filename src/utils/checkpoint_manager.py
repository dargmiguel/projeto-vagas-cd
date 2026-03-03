"""
Gerenciador de checkpoint para controle de estado do pipeline
"""

import os
from pathlib import Path
import logging
import json
import tempfile

logger = logging.getLogger(__name__)


BASE_DIR = Path(os.getenv("DATA_PATH", "data"))

class CheckpointManager:
    """
    Gerencia checkpoint da ultima execução do pipeline

    Attributes:
        checkpoint_file (str): Caminho do arquivo de checkpoint
    """

    def __init__(self, source: str, checkpoint_dir: str = None):
        """
        Inicializa o gerenciador de checkpoint

        Args:
            source: Nome da fonte de dados (ex: 'gupy', 'linkedin')
            checkpoint_dir: Diretório base para checkpoints (Opcional).
                          Se não informado, usa o padrão do projeto em data/checkpoints.
        """

        self.source = source


        if checkpoint_dir:
            self.checkpoint_dir = Path(checkpoint_dir)
        else:

            self.checkpoint_dir = BASE_DIR / "checkpoints"

        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path = self.checkpoint_dir / f"{source}_checkpoint.json"

    def salvar_checkpoint(self, **kwargs):
        """
        Salva o checkpoint atual de forma atômica.

        Aceita qualquer número de argumentos nomeados, tornando-o flexível
        para diferentes etapas do pipeline (bronze, silver, etc.).

        Exemplos de uso:
            salvar_checkpoint(ultima_data_publicacao="...", total_vagas=100)
            salvar_checkpoint(ultima_data_processada="...", rows_processed=50)

        Args:
            **kwargs: Dados a serem salvos no checkpoint.
        """
        if not kwargs:
            logger.warning("Nenhum dado fornecido para salvar no checkpoint. Operação ignorada.")
            return

        # Monta o dicionário do checkpoint, garantindo que a fonte seja incluída
        checkpoint_data = {"source": self.source, **kwargs}

        try:
            # 1. Cria um arquivo temporário no mesmo diretório do checkpoint final
            fd, temp_path = tempfile.mkstemp(dir=self.checkpoint_dir, prefix=".tmp_")
            os.close(fd) # Fecha o descritor de arquivo, vamos usar o Path

            temp_file_path = Path(temp_path)

            # 2. Escreve os dados no arquivo temporário
            with open(temp_file_path, "w") as f:
                json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)

            # 3. Renomeia o arquivo temporário para o nome final (operação atômica)
            temp_file_path.replace(self.checkpoint_path)

            logger.info(f"Checkpoint salvo com sucesso para a fonte '{self.source}'.")

        except (IOError, OSError) as e:
            logger.error(f"Erro de I/O ao salvar o checkpoint para '{self.source}': {e}")
        except Exception as e:
            logger.error(f"Erro inesperado ao salvar o checkpoint para '{self.source}': {e}", exc_info=True)

    def carregar_checkpoint(self) -> dict | None:
        """
        Carrega o checkpoint do arquivo JSON

        Returns:
            dict: Dados do checkpoint ou None se não existir
        """
        if not self.checkpoint_path.exists():
            logger.info (f"Primeiro run para {self.source}, nenhum checkpoint encontrado.")
            return None
        try:
            with open(self.checkpoint_path, "r") as f:
                checkpoint = json.load(f)
                logger.info(f"Checkpoint carregado: {checkpoint}")
                return checkpoint
        except Exception as e:
            logger.error(f"Erro ao carregar checkpoint: {e}")
            return None

    def limpar_checkpoint(self):
        """
        Remove o arquivo de checkpoint
        """
        if self.checkpoint_path.exists():
            self.checkpoint_path.unlink()
            logger.info(f"Checkpoint removido para a fonte {self.source}")
        else:
            logger.info(f"Nenhum checkpoint para remover para a fonte {self.source}")