import logging
import os
import html
import re
import sys
from typing import Any, Dict
import polars as pl
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import yaml
from src.utils.checkpoint_manager import CheckpointManager
import unicodedata

load_dotenv()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Padrão para cargos de dados (titulo/descricao)
PADRAO_CARGO_DADOS = (
    r"(engenheir[ao] de dados|analist[ao] de dados|cientista de dados|"
    r"engenheiro de dados|cientista de dados|bi\b|analytics\b|"
    r"data engineer|data scientist|data analyst)"
)



class TextProcessor:
    """Processador de texto para limpeza e normalização."""

    @staticmethod
    def limpar_texto(col: pl.Expr) -> pl.Expr:
        """Aplica limpeza de texto usando expressões Polars nativas."""
        return (
            col
            .fill_null('')
            # 1) Normalizar NBSP: troca o caractere U+00A0 por espaço normal
            .str.replace_all('\u00A0', ' ')
            # 2) Remover entidades HTML (&nbsp;, &#160;, &#xA0;, etc.)
            .str.replace_all(r'&(?:[a-zA-Z0-9]+|#[0-9]{1,6}|#x[0-9a-fA-F]{1,6});', ' ')
            # 3) Normalizar múltiplos espaços
            .str.replace_all(r'\s+', ' ')
            # 4) Remover espaços das extremidades
            .str.strip_chars()
        )

    @staticmethod
    def normalizar_texto(texto: str) -> str:
        if not texto:
            return ""
        texto = texto.lower()
        texto = unicodedata.normalize('NFD', texto)
        texto = texto.encode('ascii', 'ignore').decode('utf-8')
        return texto


class FeatureExtractor:
    """Extrai features de texto."""

    def __init__(self, config: dict):
        logger.info("🔧 Preparando feature extractor...")

        # Tech skills
        skills_tech = config.get("skills_tech", [])
        escaped_tech = sorted(
            [re.escape(TextProcessor.normalizar_texto(skill)) for skill in skills_tech if skill],
            key=len,
            reverse=True
        )
        self.tech_pattern = r'\b(' + '|'.join(escaped_tech) + r')\b' if escaped_tech else r"$.^"

        # Soft skills
        skills_soft = config.get("skills_soft", [])
        escaped_soft = sorted(
            [re.escape(TextProcessor.normalizar_texto(skill)) for skill in skills_soft if skill],
            key=len,
            reverse=True
        )
        self.soft_pattern = r'\b(' + '|'.join(escaped_soft) + r')\b' if escaped_soft else r"$.^"

        # Níveis - FLATTEN das listas
        niveis_dict = config.get("niveis", {})
        self.nivel_regex_map: Dict[str, str] = {}

        for nivel, termos in niveis_dict.items():
            if isinstance(termos, list) and termos:
                # 1. Normalizar e escapar termos
                normalized_and_escaped = [re.escape(TextProcessor.normalizar_texto(t)) for t in termos if t]

                # 2. Separar frases de palavras únicas para aplicação correta de \b
                phrases = [t for t in normalized_and_escaped if ' ' in t]
                words = [t for t in normalized_and_escaped if ' ' not in t]

                # 3. Construir o padrão (priorizar frases, usar \b nas palavras)
                regex_list = phrases + [r'\b' + w + r'\b' for w in words]

                # 4. Adicionar a flag (?i) para case-insensitive e salvar no mapa
                self.nivel_regex_map[nivel] = r'(?i)' + '|'.join(regex_list)

        # O self.nivel_pattern antigo que extraía apenas o primeiro match é removido/desativado,
        # pois agora usaremos o self.nivel_regex_map na função aplicar.
        self.nivel_pattern = r"$.^"
        logger.debug(f"Regex de Níveis gerado: {list(self.nivel_regex_map.keys())}")

        # Áreas - FLATTEN das listas
                # Áreas — mapa de regex por categoria
        areas_dict = config.get("areas", {})
        self.area_regex_map = {}

        for categoria, termos in areas_dict.items():
            # Flatten
            if isinstance(termos, list):
                termos_norm = [TextProcessor.normalizar_texto(t) for t in termos if t]
            else:
                termos_norm = [TextProcessor.normalizar_texto(termos)]

            # Escape + ordenar maior → menor (melhor match)
            termos_esc = sorted([re.escape(t) for t in termos_norm], key=len, reverse=True)

            # Criar regex único da categoria
            # Observação: frases não levam \b, palavras levam
            frases  = [t for t in termos_esc if " " in t]
            palavras = [rf"\b{t}\b" for t in termos_esc if " " not in t]

            regex_final = r"(?i)(" + "|".join(frases + palavras) + r")"
            self.area_regex_map[categoria] = regex_final

        # Pré-gerar expressões Polars
        self.area_exprs = [
            pl.when(pl.col("texto_analise").str.contains(regex))
            .then(pl.lit([categoria]))
            .otherwise(pl.lit([]))
            for categoria, regex in self.area_regex_map.items()
        ]

        logger.info("Feature extractor preparado.")

    def aplicar(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        logger.info("Aplicando extração de features...")

        # 1. Preparação do texto
        lf = lf.with_columns([
            TextProcessor.limpar_texto(pl.col('description')).alias('descricao_limpa'),
            (
                pl.col('name').fill_null('') + ' ' +
                pl.col('description').fill_null('')
            ).map_elements(lambda x: TextProcessor.normalizar_texto(x), return_dtype=pl.Utf8).alias('texto_analise')
        ])

        # 2. Extração de features - PASSANDO STRINGS, NÃO PATTERNS
        lf = lf.with_columns([
            pl.col('texto_analise').str.extract_all(self.tech_pattern).list.unique().alias('skills_tech'),
            pl.col('texto_analise').str.extract_all(self.soft_pattern).list.unique().alias('skills_soft'),
            pl.when(pl.col('texto_analise').str.contains(self.nivel_regex_map.get('lead', r"$.^")))
            .then(pl.lit(5))

            .when(pl.col('texto_analise').str.contains(self.nivel_regex_map.get('senior', r"$.^")))
            .then(pl.lit(4))

            .when(pl.col('texto_analise').str.contains(self.nivel_regex_map.get('pleno', r"$.^")))
            .then(pl.lit(3))

            .when(pl.col('texto_analise').str.contains(self.nivel_regex_map.get('junior', r"$.^")))
            .then(pl.lit(2))

            .when(pl.col('texto_analise').str.contains(self.nivel_regex_map.get('estagio', r"$.^")))
            .then(pl.lit(1))

            .otherwise(pl.lit(0))
            .cast(pl.Int8)
            .alias('nivel'),
            pl.concat_list(self.area_exprs).list.unique().alias("areas"),
        ])


        # 3. Colunas derivadas

        # 3.1 total_skills e normalização de areas
        lf = lf.with_columns([
            (pl.col('skills_tech').list.len() + pl.col('skills_soft').list.len()).alias('total_skills'),

            # Se areas estiver vazio, coloca ['geral']
            pl.when(pl.col('areas').list.len() == 0)
            .then(pl.lit(['geral']))
            .otherwise(pl.col('areas'))
            .alias('areas'),
        ])


        lf = lf.with_columns([
            # Flag is_tech: se tiver qualquer skill técnica
            (pl.col("skills_tech").list.len() > 0).alias("is_tech"),

            # Flag is_dados mais restritiva
            pl.when(
                pl.col("areas").list.contains("dados")
                & pl.col("texto_analise").str.contains(PADRAO_CARGO_DADOS)
            )
            .then(True)
            .otherwise(False)
            .alias("is_dados"),

            # Modalidade
            pl.when(pl.col('workplaceType') == 'remote')
            .then(pl.lit('remoto'))
            .when(pl.col('workplaceType') == 'hybrid')
            .then(pl.lit('hibrido'))
            .otherwise(pl.lit('presencial'))
            .alias('modalidade'),

            # Localização
            pl.when(pl.col('city').is_not_null() & pl.col('state').is_not_null())
            .then(pl.concat_str([pl.col('city'), pl.lit(', '), pl.col('state')]))
            .otherwise(pl.coalesce([pl.col('city'), pl.col('state')]))
            .alias('localizacao'),
        ])


        # 4. Renomear colunas
        lf = lf.with_columns([
            pl.col('id').cast(pl.Utf8).alias('vaga_id'),
            pl.col('companyId').cast(pl.Int64).alias('company_id'),
            pl.col('name').alias('titulo'),
            pl.col('careerPageName').alias('empresa'),
            pl.col('careerPageLogo').alias('empresa_logo'),
            pl.col('careerPageUrl').alias('empresa_url'),
            pl.col('city').alias('cidade'),
            pl.col('state').alias('estado'),
            pl.col('country').alias('pais'),
            pl.col('publishedDate').str.to_datetime(time_unit='ms',time_zone='UTC', strict=False).alias('data_publicacao'),
            pl.col('applicationDeadline').str.to_date(format='%Y-%m-%d', strict=False).alias('data_expiracao'),
            pl.col('jobUrl').alias('url'),
            pl.col('disabilities').alias('aceita_pcd'),
            pl.col('isRemoteWork').alias('is_remote_work'),
            pl.col('workplaceType').alias('workplace_type'),
            pl.lit(datetime.now().isoformat()).str.to_datetime(time_unit ='ms', time_zone='UTC', strict=False).alias('data_processamento'),
            pl.lit('gupy').alias('fonte'),
        ])


        # 5. Seleciona colunas finais
        colunas_finais = [
            'vaga_id', 'company_id', 'titulo', 'empresa', 'empresa_logo', 'empresa_url',
            'descricao_limpa', 'cidade', 'estado', 'pais', 'localizacao',
            'modalidade', 'workplace_type', 'is_remote_work',
            'nivel', 'areas',
            'skills_tech', 'skills_soft', 'total_skills',
            'is_tech', 'is_dados',
            'url',
            'data_publicacao', 'data_expiracao', 'aceita_pcd',
            'data_processamento', 'fonte',
        ]
        lf = lf.select(colunas_finais)

        logger.info("✅ Extração de features concluída.")
        return lf


def processar_vagas(df_bronze: pl.DataFrame, config: dict) -> pl.DataFrame:
    """Processa vagas aplicando limpeza e extração de features.
    param df: DataFrame de vagas bruto.
    param config: Configuração para extração de features.
    return: DataFrame processado com features extraídas.
    """
    logger.info(f" Processando {len(df_bronze)} vagas da gupy...")
    lf_bronze = df_bronze.lazy()
    extractor = FeatureExtractor(config)
    lf_silver = extractor.aplicar(lf_bronze)
    df_silver = lf_silver.collect()
    logger.info(" Processamento de vagas concluído.")
    return df_silver