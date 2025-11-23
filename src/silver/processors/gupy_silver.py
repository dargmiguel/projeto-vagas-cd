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

AREAS_PRIORIDADE = [
    "dados",
    "backend",
    "frontend",
    "fullstack",
    "mobile",
    "devops",
    "qa",
    "produto",
    "design",
    "seguranca",
]

# Padrão para cargos de dados (titulo/descricao)
PADRAO_CARGO_DADOS = (
    r"(engenheir[ao] de dados|analist[ao] de dados|cientista de dados|"
    r"engenheiro de dados|cientista de dados|bi\b|analytics\b|"
    r"data engineer|data scientist|data analyst)"
)


def escolher_area_principal(areas: list[str] | None) -> str | None:
    """
    Recebe a lista de áreas extraídas e devolve uma área principal.
    Usa uma ordem de prioridade simples.
    """
    if areas is None or not isinstance(areas, list) or len(areas) == 0:
        return None

    # garante tudo normalizado
    areas_norm = [TextProcessor.normalizar_texto(a) for a in areas if a]
    if not areas_norm:
        return None
    for alvo in AREAS_PRIORIDADE:
        if alvo in areas_norm:
            return alvo

    # fallback: primeira área da lista
    return areas_norm[0]


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
        niveis_unicos = set()
        for nivel_lista in niveis_dict.values():
            if isinstance(nivel_lista, list):
                niveis_unicos.update([TextProcessor.normalizar_texto(n) for n in nivel_lista if n])
            else:
                niveis_unicos.add(TextProcessor.normalizar_texto(nivel_lista))

        escaped_niveis = sorted([re.escape(n) for n in niveis_unicos], key=len, reverse=True)
        self.nivel_pattern = r'\b(' + '|'.join(escaped_niveis) + r')\b' if escaped_niveis else r"$.^"

        # Áreas - FLATTEN das listas
        areas_dict = config.get("areas", {})
        areas_unicas = set()
        for area_lista in areas_dict.values():
            if isinstance(area_lista, list):
                areas_unicas.update([TextProcessor.normalizar_texto(a) for a in area_lista if a])
            else:
                areas_unicas.add(TextProcessor.normalizar_texto(area_lista))

        escaped_areas = sorted([re.escape(a) for a in areas_unicas], key=len, reverse=True)
        self.area_pattern = r'\b(' + '|'.join(escaped_areas) + r')\b' if escaped_areas else r"$.^"

        logger.info("✅ Feature extractor preparado.")

    def aplicar(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        logger.info("🔄 Aplicando extração de features...")

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
            pl.col('texto_analise').str.extract(self.nivel_pattern, group_index=0).alias('nivel'),
            pl.col('texto_analise').str.extract_all(self.area_pattern).list.unique().alias('areas'),
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

        # 3.2 área principal (macro-categoria)
        lf = lf.with_columns([
            pl.col("areas")
            .map_elements(escolher_area_principal, return_dtype=pl.Utf8)
            .alias("area_principal")
        ])

        # 3.3 flags is_tech / is_dados + modalidade/localização
        # 3.3 flags is_tech / is_dados + modalidade/localização
        lf = lf.with_columns([
            # Flag is_tech: área principal de tech OU tem skills_tech
            (
                pl.col("area_principal").is_in([
                    "backend", "frontend", "fullstack",
                    "mobile", "dados", "devops",
                    "qa", "produto", "design", "seguranca"
                ])
                | (pl.col("skills_tech").list.len() > 0)
            )
            .alias("is_tech"),

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
            pl.col('companyId').alias('company_id'),
            pl.col('name').alias('titulo'),
            pl.col('careerPageName').alias('empresa'),
            pl.col('careerPageLogo').alias('empresa_logo'),
            pl.col('careerPageUrl').alias('empresa_url'),
            pl.col('city').alias('cidade'),
            pl.col('state').alias('estado'),
            pl.col('country').alias('pais'),
            pl.col('publishedDate').alias('data_publicacao'),
            pl.col('applicationDeadline').alias('data_expiracao'),
            pl.col('jobUrl').alias('url'),
            pl.col('disabilities').alias('aceita_pcd'),
            pl.col('isRemoteWork').alias('is_remote_work'),
            pl.col('workplaceType').alias('workplace_type'),
            pl.lit(datetime.now().isoformat()).alias('data_processamento'),
            pl.lit('gupy').alias('fonte'),
        ])


        # 5. Seleciona colunas finais
        colunas_finais = [
                'vaga_id', 'company_id', 'titulo', 'empresa', 'empresa_logo', 'empresa_url',
                'descricao_limpa', 'cidade', 'estado', 'pais', 'localizacao',
                'modalidade', 'workplace_type', 'is_remote_work',
                'nivel', 'areas', 'area_principal',
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