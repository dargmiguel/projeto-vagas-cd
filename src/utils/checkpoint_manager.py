"""
Gerenciador de checkpoint para controle de estado do pipeline
"""


from pathlib import Path
import logging
import json

logger = logging.getLogger(__name__)

class CheckpointManager:
    """
    Gerencia checkpoint da ultima execução do pipeline

    Attributes:
        checkpoint_file (str): Caminho do arquivo de checkpoint
    """

    def __init__(self, source: str, checkpoint_dir: str = "data/checkpoints"):
        """
        Inicializa o gerenciador de checkpoint

        Args:
            source: Nome da fonte de dados (ex: 'gupy', 'linkedin')
            checkpoint_dir: Diretório base para checkpoints
        """

        self.source = source
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path = self.checkpoint_dir / f"{source}_checkpoint.json"

    def salvar_checkpoint(self, ultima_data_publicacao: str, total_vagas: int, metadata: dict = None):
        """
        Salva o checkpoint atual em um arquivo JSON

        Args:
            ultima_data_publicacao: Data da última publicação coletada
            total_vagas: Total de vagas coletadas
            metadata: Metadados adicionais a serem salvos
        """


        checkpoint = {
            "source": self.source,
            "ultima_data_publicacao": ultima_data_publicacao,
            "total_vagas": total_vagas,
            "metadata": metadata or {}
        }

        with open(self.checkpoint_path, "w") as f:
            json.dump(checkpoint, f, indent= 2, ensure_ascii= False)
        logger.info(f"Checkpoint salvo: {ultima_data_publicacao}, total_vagas: {total_vagas} para a fonte {self.source}")

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