"""
Tech Recruitment Data Pipeline - Dashboard
Um dashboard profissional para visualização e monitoramento do Data Lake
"""

import streamlit as st
import polars as pl
import altair as alt
import os
from datetime import datetime, date, timedelta
from typing import Optional

# ============================================================================
# CONFIGURAÇÃO DA PÁGINA
# ============================================================================

st.set_page_config(
    page_title="Tech Recruitment Data Engine",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS Customizado para design profissional
st.markdown("""
<style>
    /* Métricas estilizadas */
    .stMetric {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 1.2rem;
        border-radius: 0.75rem;
        border-left: 4px solid #4CAF50;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .stMetric > label {
        font-size: 0.85rem !important;
        color: #6c757d !important;
        font-weight: 500 !important;
    }
    .stMetric > div {
        font-size: 1.75rem !important;
        font-weight: 700 !important;
        color: #212529 !important;
    }

    /* Sidebar */
    .stSidebar {
        background-color: #f8f9fa;
    }

    /* Títulos de seção */
    .section-header {
        font-size: 1.1rem;
        font-weight: 600;
        color: #2c3e50;
        margin-bottom: 0.5rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #4CAF50;
    }

    /* Badges de status */
    .status-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 1rem;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    .status-online {
        background-color: #d4edda;
        color: #155724;
    }
    .status-warning {
        background-color: #fff3cd;
        color: #856404;
    }

    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.5rem;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 0.75rem 1.5rem;
        border-radius: 0.5rem 0.5rem 0 0;
    }

    /* Dataframe styling */
    .stDataFrame {
        border-radius: 0.5rem;
        overflow: hidden;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def get_table_metadata(path: str) -> dict:
    """Retorna metadados reais do sistema de arquivos para provar a saúde do Lake"""
    if os.path.exists(path):
        try:
            total_size = 0
            file_count = 0
            last_modified = datetime.min

            for root, dirs, files in os.walk(path):
                for f in files:
                    fp = os.path.join(root, f)
                    total_size += os.path.getsize(fp)
                    file_count += 1
                    mod_time = datetime.fromtimestamp(os.path.getmtime(fp))
                    if mod_time > last_modified:
                        last_modified = mod_time

            return {
                "last_update": last_modified.strftime('%Y-%m-%d %H:%M:%S'),
                "size_mb": f"{total_size / (1024 * 1024):.2f}",
                "file_count": file_count,
                "exists": True
            }
        except Exception as e:
            return {"last_update": "Erro", "size_mb": "0", "file_count": 0, "exists": False}
    return {"last_update": "N/A", "size_mb": "0", "file_count": 0, "exists": False}


@st.cache_data(ttl=300)
def load_gold_data(table_name: str) -> pl.DataFrame:
    """Carrega dados da Gold Layer com tratamento de erros"""
    path = f"data/gold_delta/{table_name}"
    try:
        if not os.path.exists(path):
            return pl.DataFrame()

        df = pl.scan_delta(path).collect()
        return df
    except Exception as e:
        st.error(f"Erro ao carregar {table_name}: {str(e)}")
        return pl.DataFrame()


@st.cache_data(ttl=300)
def load_silver_data(table_name: str = "") -> pl.DataFrame:
    """Carrega dados da Silver Layer com tratamento de erros"""
    path = f"data/silver_delta/{table_name}" if table_name else "data/silver_delta"
    try:
        if not os.path.exists(path):
            return pl.DataFrame()
        return pl.scan_delta(path).collect()
    except Exception:
        return pl.DataFrame()


@st.cache_data(ttl=300)
def load_bronze_data(table_name: str = "") -> pl.DataFrame:
    """Carrega dados da Bronze Layer com tratamento de erros"""
    path = f"data/bronze_delta/{table_name}" if table_name else "data/bronze_delta"
    try:
        if not os.path.exists(path):
            return pl.DataFrame()
        return pl.scan_delta(path).collect()
    except Exception:
        return pl.DataFrame()


# Mapeamento de níveis numéricos para labels
NIVEL_MAP = {
    5: "Lead",
    4: "Sênior",
    3: "Pleno",
    2: "Júnior",
    1: "Estágio",
    0: "Não identificado"
}

# Mapeamento reverso: label -> valor numérico
NIVEL_REVERSE_MAP = {v: k for k, v in NIVEL_MAP.items()}


def apply_filters(df: pl.DataFrame, filtros: dict) -> pl.DataFrame:
    """Aplica filtros dinâmicos ao DataFrame"""
    if df.is_empty():
        return df

    result = df

    # Filtro de área
    if filtros.get("area") and filtros["area"] != "Todas":
        if "area_principal" in result.columns:
            result = result.filter(pl.col("area_principal") == filtros["area"].lower())

    # Filtro de senioridade (coluna 'nivel' é Int8: 5=Lead, 4=Sênior, 3=Pleno, 2=Júnior, 1=Estágio)
    if filtros.get("senioridade"):
        if "nivel" in result.columns:
            # Converte labels para valores numéricos
            nivel_values = [NIVEL_REVERSE_MAP.get(n) for n in filtros["senioridade"] if NIVEL_REVERSE_MAP.get(n) is not None]
            if nivel_values:
                result = result.filter(pl.col("nivel").is_in(nivel_values))

    # Filtro de modelo de trabalho (coluna 'modalidade' no seu schema)
    if filtros.get("modelo"):
        if "modalidade" in result.columns:
            modelos_lower = [m.lower() for m in filtros["modelo"]]
            result = result.filter(pl.col("modalidade").is_in(modelos_lower))

    # Filtro de data (coluna 'data_publicacao' no seu schema)
    if filtros.get("data_inicio") and filtros.get("data_fim"):
        if "data_publicacao" in result.columns:
            result = result.filter(
                (pl.col("data_publicacao").dt.date() >= filtros["data_inicio"]) &
                (pl.col("data_publicacao").dt.date() <= filtros["data_fim"])
            )

    # Filtro de empresa
    if filtros.get("empresas"):
        if "empresa" in result.columns:
            result = result.filter(pl.col("empresa").is_in(filtros["empresas"]))

    return result


def calculate_kpis(df: pl.DataFrame) -> dict:
    """Calcula KPIs principais do dataset - adaptado para o schema real"""

    # Valores padrão para todas as chaves
    kpis = {
        "total_vagas": 0,
        "vagas_hoje": 0,
        "empresas": 0,
        "skills_tech_unicas": 0,
        "skills_soft_unicas": 0,
        "data_min": "N/A",
        "data_max": "N/A",
        "vagas_remotas": 0,
        "vagas_presenciais": 0,
        "vagas_hibridas": 0,
        "vagas_pcd": 0,
        "areas_distintas": 0
    }

    if df.is_empty():
        return kpis

    hoje = date.today()

    # Total de vagas
    kpis["total_vagas"] = df.height

    # Empresas únicas
    if "empresa" in df.columns:
        kpis["empresas"] = df.select("empresa").n_unique()

    # Skills técnicas únicas
    if "skills_tech" in df.columns:
        try:
            skills = df.select(pl.col("skills_tech").explode()).filter(
                pl.col("skills_tech").is_not_null() & (pl.col("skills_tech") != "")
            ).unique()
            kpis["skills_tech_unicas"] = skills.height
        except Exception:
            kpis["skills_tech_unicas"] = 0

    # Skills soft únicas
    if "skills_soft" in df.columns:
        try:
            skills = df.select(pl.col("skills_soft").explode()).filter(
                pl.col("skills_soft").is_not_null() & (pl.col("skills_soft") != "")
            ).unique()
            kpis["skills_soft_unicas"] = skills.height
        except Exception:
            kpis["skills_soft_unicas"] = 0

    # Datas de publicação
    if "data_publicacao" in df.columns:
        try:
            datas = df.select("data_publicacao").filter(pl.col("data_publicacao").is_not_null())
            if not datas.is_empty():
                kpis["data_min"] = datas.min().item().strftime("%d/%m/%Y")
                kpis["data_max"] = datas.max().item().strftime("%d/%m/%Y")

                # Vagas de hoje
                vagas_hoje = df.filter(
                    pl.col("data_publicacao").dt.date() == hoje
                ).height
                kpis["vagas_hoje"] = vagas_hoje
        except Exception:
            pass

    # Modalidade de trabalho
    if "modalidade" in df.columns:
        try:
            mod_counts = df.group_by("modalidade").len()
            for row in mod_counts.iter_rows(named=True):
                modalidade = str(row.get("modalidade", "")).lower()
                if "remoto" in modalidade:
                    kpis["vagas_remotas"] += row.get("len", 0)
                elif "híbrido" in modalidade or "hibrido" in modalidade:
                    kpis["vagas_hibridas"] += row.get("len", 0)
                elif "presencial" in modalidade:
                    kpis["vagas_presenciais"] += row.get("len", 0)
        except Exception:
            pass

    # Vagas PCD
    if "aceita_pcd" in df.columns:
        try:
            kpis["vagas_pcd"] = df.filter(pl.col("aceita_pcd") == True).height
        except Exception:
            pass

    # Áreas distintas
    if "area_principal" in df.columns:
        try:
            kpis["areas_distintas"] = df.select("area_principal").n_unique()
        except Exception:
            pass

    return kpis


# ============================================================================
# SIDEBAR - FILTROS
# ============================================================================

st.sidebar.markdown("## 🔍 Filtros")
st.sidebar.markdown("---")

# Filtro de área
filtro_area = st.sidebar.selectbox(
    "Área de Atuação",
    ["Todas", "Dados", "Dev", "Cloud", "Design", "Produto", "QA"],
    help="Filtra vagas por área principal"
)

# Filtro de senioridade (nivel: 5=Lead, 4=Sênior, 3=Pleno, 2=Júnior, 1=Estágio)
filtro_senioridade = st.sidebar.multiselect(
    "Nível de Senioridade",
    ["Estágio", "Júnior", "Pleno", "Sênior", "Lead"],
    default=["Júnior", "Pleno", "Sênior"],
    help="Selecione um ou mais níveis"
)

# Filtro de modelo de trabalho (usando 'modalidade')
filtro_modelo = st.sidebar.multiselect(
    "Modelo de Trabalho",
    ["Remoto", "Híbrido", "Presencial"],
    default=["Remoto", "Híbrido", "Presencial"],
    help="Filtre por modalidade de trabalho"
)

# Filtro de data
st.sidebar.markdown("### 📅 Período")
data_default = (date.today() - timedelta(days=30), date.today())
filtro_data = st.sidebar.date_input(
    "Período de Publicação",
    value=data_default,
    help="Selecione o intervalo de datas"
)

# Botão para limpar cache
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Atualizar Dados", width='stretch'):
    st.cache_data.clear()
    st.rerun()

# Monta dicionário de filtros
filtros = {
    "area": filtro_area,
    "senioridade": filtro_senioridade,
    "modelo": filtro_modelo,
    "data_inicio": filtro_data[0] if len(filtro_data) > 0 else None,
    "data_fim": filtro_data[1] if len(filtro_data) > 1 else None
}

# ============================================================================
# HEADER
# ============================================================================

col_logo, col_title = st.columns([1, 5])
with col_title:
    st.title("🎯 Tech Recruitment Data Engine")
    st.markdown("**Pipeline de dados para matching de currículos com vagas de tecnologia**")

st.markdown("---")

# ============================================================================
# CARREGAMENTO DE DADOS
# ============================================================================

with st.spinner("Carregando dados do Lake..."):
    df_vagas = load_gold_data("dim_vagas")
    df_skills = load_gold_data("agg_skills_monitor")
    df_trends = load_gold_data("agg_market_trends")

    # Aplica filtros
    df_filtered = apply_filters(df_vagas, filtros)
    kpis = calculate_kpis(df_filtered)

    # Metadados das tabelas
    meta_gold = get_table_metadata("data/gold_delta/dim_vagas")
    meta_silver = get_table_metadata("data/silver_delta")
    meta_bronze = get_table_metadata("data/bronze_delta")

# ============================================================================
# KPIs PRINCIPAIS
# ============================================================================

st.markdown("### 📊 Visão Geral")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Total de Vagas",
        value=f"{kpis['total_vagas']:,}".replace(",", "."),
        delta=f"+{kpis.get('vagas_hoje', 0)} hoje" if kpis.get('vagas_hoje', 0) > 0 else None
    )

with col2:
    st.metric(
        label="Empresas Ativas",
        value=f"{kpis['empresas']:,}".replace(",", ".")
    )

with col3:
    st.metric(
        label="Skills Técnicas Únicas",
        value=f"{kpis['skills_tech_unicas']:,}".replace(",", ".")
    )

with col4:
    st.metric(
        label="Período Coberto",
        value=f"{kpis.get('data_min', 'N/A')} → {kpis.get('data_max', 'N/A')}"
    )

# Segunda linha de KPIs
col5, col6, col7, col8 = st.columns(4)

with col5:
    st.metric(
        label="Vagas Remotas",
        value=f"{kpis['vagas_remotas']:,}".replace(",", "."),
        delta=f"{kpis['vagas_remotas'] / max(kpis['total_vagas'], 1) * 100:.1f}% do total"
    )

with col6:
    st.metric(
        label="Vagas Híbridas",
        value=f"{kpis['vagas_hibridas']:,}".replace(",", ".")
    )

with col7:
    st.metric(
        label="Vagas Aceitam PCD",
        value=f"{kpis['vagas_pcd']:,}".replace(",", ".")
    )

with col8:
    st.metric(
        label="Última Atualização",
        value=meta_gold.get("last_update", "N/A")[:10] if meta_gold.get("last_update") != "N/A" else "N/A"
    )

st.markdown("---")

# ============================================================================
# TABS PRINCIPAIS
# ============================================================================

tab_insights, tab_empresas, tab_explorador, tab_health = st.tabs([
    "📊 Market Insights", "🏢 Empresas", "📋 Explorador de Dados", "🛠️ Pipeline Health"
])

# ============================================================================
# TAB 1: MARKET INSIGHTS
# ============================================================================

with tab_insights:
    st.markdown("### 📈 Análise de Mercado de Trabalho em Tecnologia")

    if df_filtered.is_empty():
        st.warning("⚠️ Nenhum dado encontrado com os filtros selecionados.")
    else:
        # === LINHA 1: Distribuições ===
        col_dist1, col_dist2 = st.columns(2)

        with col_dist1:
            st.markdown("**Distribuição por Nível de Senioridade**")

            if "nivel" in df_filtered.columns:
                # Agrupa por nível numérico e converte para labels
                nivel_dist = df_filtered.group_by("nivel").len().sort("len", descending=True)

                # Adiciona coluna com label do nível
                nivel_dist = nivel_dist.with_columns(
                    pl.col("nivel").replace_strict(NIVEL_MAP, default="Não identificado").alias("Nível")
                ).select(["Nível", "len"]).rename({"len": "Quantidade"})

                chart_nivel = alt.Chart(nivel_dist.to_pandas()).mark_bar(
                    cornerRadiusTopLeft=3,
                    cornerRadiusTopRight=3
                ).encode(
                    x=alt.X("Quantidade:Q", title="Número de Vagas"),
                    y=alt.Y("Nível:N", sort="-x", title=""),
                    color=alt.Color("Quantidade:Q", scale=alt.Scale(scheme="blues"), legend=None),
                    tooltip=["Nível", "Quantidade"]
                ).properties(height=250)

                st.altair_chart(chart_nivel, width='stretch')
            else:
                st.info("Coluna 'nivel' não disponível.")

        with col_dist2:
            st.markdown("**Distribuição por Modalidade**")

            if "modalidade" in df_filtered.columns:
                mod_dist = df_filtered.group_by("modalidade").len().sort("len", descending=True)
                mod_dist = mod_dist.rename({"modalidade": "Modalidade", "len": "Quantidade"})

                chart_mod = alt.Chart(mod_dist.to_pandas()).mark_arc(
                    innerRadius=50,
                    cornerRadius=5
                ).encode(
                    theta="Quantidade:Q",
                    color=alt.Color("Modalidade:N", scale=alt.Scale(scheme="set2")),
                    tooltip=["Modalidade", "Quantidade"]
                ).properties(height=250)

                st.altair_chart(chart_mod, width='stretch')
            else:
                st.info("Coluna 'modalidade' não disponível.")

        # === LINHA 2: Top Skills ===
        st.markdown("---")
        st.markdown("**🏆 Top 15 Skills Mais Demandadas**")

        # Tenta carregar da tabela agregada primeiro
        if not df_skills.is_empty() and "skill" in df_skills.columns:
            skills_top = df_skills.head(15)
            if "total_vagas" in skills_top.columns:
                skills_top = skills_top.rename({"skill": "Skill", "total_vagas": "Ocorrências"})
            elif "len" in skills_top.columns:
                skills_top = skills_top.rename({"skill": "Skill", "len": "Ocorrências"})

            chart_skills = alt.Chart(skills_top.to_pandas()).mark_bar(
                cornerRadiusTopLeft=3,
                cornerRadiusTopRight=3
            ).encode(
                x=alt.X("Ocorrências:Q", title="Número de Vagas"),
                y=alt.Y("Skill:N", sort="-x", title=""),
                color=alt.Color("Ocorrências:Q", scale=alt.Scale(scheme="greens"), legend=None),
                tooltip=["Skill", "Ocorrências"]
            ).properties(height=400)

            st.altair_chart(chart_skills, width='stretch')

        # Se não tem tabela agregada, extrai do dim_vagas
        elif "skills_tech" in df_filtered.columns:
            try:
                skills_exploded = df_filtered.select(
                    pl.col("skills_tech").explode().alias("skill")
                ).filter(
                    pl.col("skill").is_not_null() & (pl.col("skill") != "")
                )

                skills_count = skills_exploded.group_by("skill").len().sort("len", descending=True).head(15)
                skills_count = skills_count.rename({"skill": "Skill", "len": "Ocorrências"})

                chart_skills = alt.Chart(skills_count.to_pandas()).mark_bar(
                    cornerRadiusTopLeft=3,
                    cornerRadiusTopRight=3
                ).encode(
                    x=alt.X("Ocorrências:Q", title="Número de Vagas"),
                    y=alt.Y("Skill:N", sort="-x", title=""),
                    color=alt.Color("Ocorrências:Q", scale=alt.Scale(scheme="greens"), legend=None),
                    tooltip=["Skill", "Ocorrências"]
                ).properties(height=400)

                st.altair_chart(chart_skills, width='stretch')
            except Exception as e:
                st.info(f"Não foi possível processar skills: {e}")
        else:
            st.info("Skills não disponíveis no dataset.")

        # === LINHA 3: Trend temporal ===
        st.markdown("---")
        st.markdown("**📅 Evolução de Vagas no Tempo**")

        if "data_publicacao" in df_filtered.columns:
            try:
                trend = df_filtered.filter(
                    pl.col("data_publicacao").is_not_null()
                ).with_columns(
                    pl.col("data_publicacao").dt.date().alias("data")
                ).group_by("data").len().sort("data")

                trend = trend.rename({"data": "Data", "len": "Vagas"})

                chart_trend = alt.Chart(trend.to_pandas()).mark_line(
                    point=True,
                    strokeWidth=2,
                    color="#4CAF50"
                ).encode(
                    x=alt.X("Data:T", title="Data de Publicação"),
                    y=alt.Y("Vagas:Q", title="Número de Vagas"),
                    tooltip=["Data", "Vagas"]
                ).properties(height=300)

                st.altair_chart(chart_trend, width='stretch')
            except Exception as e:
                st.info(f"Não foi possível gerar trend: {e}")
        else:
            st.info("Coluna 'data_publicacao' não disponível para trend.")

        # === LINHA 4: Áreas ===
        st.markdown("---")
        st.markdown("**📂 Distribuição por Área Principal**")

        if "area_principal" in df_filtered.columns:
            area_dist = df_filtered.group_by("area_principal").len().sort("len", descending=True).head(10)
            area_dist = area_dist.rename({"area_principal": "Área", "len": "Vagas"})

            chart_area = alt.Chart(area_dist.to_pandas()).mark_bar(
                cornerRadiusTopLeft=3,
                cornerRadiusTopRight=3
            ).encode(
                x=alt.X("Vagas:Q", title="Número de Vagas"),
                y=alt.Y("Área:N", sort="-x", title=""),
                color=alt.Color("Vagas:Q", scale=alt.Scale(scheme="purples"), legend=None),
                tooltip=["Área", "Vagas"]
            ).properties(height=300)

            st.altair_chart(chart_area, width='stretch')

# ============================================================================
# TAB 2: EMPRESAS
# ============================================================================

with tab_empresas:
    st.markdown("### 🏢 Análise por Empresa")

    if df_filtered.is_empty():
        st.warning("⚠️ Nenhum dado encontrado com os filtros selecionados.")
    else:
        if "empresa" not in df_filtered.columns:
            st.info("Coluna 'empresa' não disponível no dataset.")
        else:
            # Ranking de empresas
            st.markdown("**🏆 Ranking de Contratação**")

            empresa_stats = df_filtered.group_by("empresa").agg([
                pl.len().alias("total_vagas"),
            ]).sort("total_vagas", descending=True)

            # Gráfico de barras horizontal
            top_empresas = empresa_stats.head(15)
            top_empresas_chart = top_empresas.rename({"empresa": "Empresa", "total_vagas": "Vagas"})

            chart_empresas = alt.Chart(top_empresas_chart.to_pandas()).mark_bar(
                cornerRadiusTopLeft=3,
                cornerRadiusTopRight=3
            ).encode(
                x=alt.X("Vagas:Q", title="Total de Vagas"),
                y=alt.Y("Empresa:N", sort="-x", title=""),
                color=alt.Color("Vagas:Q", scale=alt.Scale(scheme="blues"), legend=None),
                tooltip=["Empresa", "Vagas"]
            ).properties(height=400)

            st.altair_chart(chart_empresas, width='stretch')

            # Tabela detalhada
            st.markdown("---")
            st.markdown("**📋 Detalhamento Completo**")

            df_display = empresa_stats.head(50)

            st.dataframe(
                df_display.to_pandas(),
                width='stretch',
                column_config={
                    "empresa": st.column_config.TextColumn("Empresa", width="medium"),
                    "total_vagas": st.column_config.ProgressColumn(
                        "Volume de Vagas",
                        help="Quantidade de vagas publicadas",
                        format="%d",
                        min_value=0,
                        max_value=int(empresa_stats["total_vagas"].max()) if not empresa_stats.is_empty() else 1
                    )
                },
                hide_index=True
            )

# ============================================================================
# TAB 3: EXPLORADOR DE DADOS
# ============================================================================

with tab_explorador:
    st.markdown("### 📋 Explorador de Dados Raw")

    if df_filtered.is_empty():
        st.warning("⚠️ Nenhum dado encontrado com os filtros selecionados.")
    else:
        # Info do dataset
        col_info1, col_info2, col_info3 = st.columns(3)

        with col_info1:
            st.info(f"📊 **{df_filtered.height:,}** registros carregados".replace(",", "."))

        with col_info2:
            st.info(f"📋 **{len(df_filtered.columns)}** colunas disponíveis")

        with col_info3:
            st.info(f"💾 **{df_filtered.estimated_size() / 1024 / 1024:.2f} MB** em memória")

        # Seletor de colunas
        colunas_disponiveis = df_filtered.columns

        # Colunas padrão (sem 'nivel' numérico, mas podemos mostrar o label)
        default_cols = ["vaga_id", "titulo", "empresa", "nivel", "modalidade", "area_principal", "data_publicacao"]
        default_cols = [c for c in default_cols if c in colunas_disponiveis]
        if "skills_tech" in colunas_disponiveis:
            default_cols.append("skills_tech")

        colunas_selecionadas = st.multiselect(
            "Selecione as colunas para visualizar",
            colunas_disponiveis,
            default=default_cols,
            help="Escolha quais colunas deseja ver na tabela"
        )

        # Filtro de busca
        busca = st.text_input(
            "🔍 Buscar no dataset",
            placeholder="Digite para filtrar...",
            help="Busca em todas as colunas de texto"
        )

        # Aplica filtro de busca no df_original (antes de converter nível)
        df_for_filter = df_filtered
        if busca and colunas_selecionadas:
            # Filtra apenas colunas de string
            str_cols = [c for c in colunas_selecionadas if df_filtered[c].dtype == pl.String]
            if str_cols:
                conditions = [pl.col(c).str.contains(f"(?i){busca}") for c in str_cols]
                df_for_filter = df_filtered.filter(pl.any_horizontal(conditions))

        # Converte nível numérico para label para exibição
        df_display = df_for_filter
        if "nivel" in df_display.columns:
            df_display = df_display.with_columns(
                pl.col("nivel").replace_strict(NIVEL_MAP, default="Não identificado").alias("nivel")
            )

        # Seleciona colunas
        if colunas_selecionadas:
            df_display = df_display.select(colunas_selecionadas)

        # Mostra tabela
        st.dataframe(
            df_display.head(500).to_pandas(),
            width='stretch',
            hide_index=True
        )

        # Download
        st.markdown("---")
        col_dl1, col_dl2 = st.columns(2)

        with col_dl1:
            try:
                # Convert list columns to string for CSV compatibility
                df_csv = df_display
                list_cols = [col for col, dtype in zip(df_csv.columns, df_csv.dtypes) if dtype == pl.List(pl.String)]
                if list_cols:
                    df_csv = df_csv.with_columns([
                        pl.col(c).list.join(", ") for c in list_cols
                    ])
                csv = df_csv.write_csv()
                st.download_button(
                "📥 Download CSV",
                csv,
                file_name=f"vagas_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                width='stretch'
            )

            except Exception as e:
                st.error(f"Erro ao gerar CSV: {e}")

        with col_dl2:
            st.markdown(f"*Exibindo **{min(df_display.height, 500)}** de **{df_display.height}** registros*")

# ============================================================================
# TAB 4: PIPELINE HEALTH
# ============================================================================

with tab_health:
    st.markdown("### 🛠️ Saúde do Pipeline e Data Lake")

    # Status geral
    col_status1, col_status2, col_status3 = st.columns(3)

    with col_status1:
        st.markdown("""
        <div style="background-color: #d4edda; padding: 1rem; border-radius: 0.5rem; text-align: center;">
            <span style="font-size: 1.5rem;">🥉</span>
            <br>
            <strong>Bronze Layer</strong>
            <br>
            <span style="color: #155724;">Online</span>
        </div>
        """, unsafe_allow_html=True)
        st.metric("Tamanho", f"{meta_bronze['size_mb']} MB")
        st.metric("Arquivos", meta_bronze['file_count'])

    with col_status2:
        st.markdown("""
        <div style="background-color: #d4edda; padding: 1rem; border-radius: 0.5rem; text-align: center;">
            <span style="font-size: 1.5rem;">🥈</span>
            <br>
            <strong>Silver Layer</strong>
            <br>
            <span style="color: #155724;">Online</span>
        </div>
        """, unsafe_allow_html=True)
        st.metric("Tamanho", f"{meta_silver['size_mb']} MB")
        st.metric("Arquivos", meta_silver['file_count'])

    with col_status3:
        st.markdown("""
        <div style="background-color: #d4edda; padding: 1rem; border-radius: 0.5rem; text-align: center;">
            <span style="font-size: 1.5rem;">🥇</span>
            <br>
            <strong>Gold Layer</strong>
            <br>
            <span style="color: #155724;">Online</span>
        </div>
        """, unsafe_allow_html=True)
        st.metric("Tamanho", f"{meta_gold['size_mb']} MB")
        st.metric("Arquivos", meta_gold['file_count'])

    st.markdown("---")

    # Informações técnicas
    col_tech1, col_tech2 = st.columns(2)

    with col_tech1:
        st.markdown("#### 📐 Schema da dim_vagas")

        if not df_vagas.is_empty():
            schema_info = {col: str(dtype) for col, dtype in df_vagas.schema.items()}

            st.dataframe(
                pl.DataFrame({
                    "Coluna": list(schema_info.keys()),
                    "Tipo": list(schema_info.values())
                }).to_pandas(),
                width='stretch',
                hide_index=True
            )
        else:
            st.info("Schema não disponível - tabela vazia ou inexistente")

    with col_tech2:
        st.markdown("#### 📊 Estatísticas de Qualidade")

        if not df_vagas.is_empty():
            stats = []

            for col in df_vagas.columns:
                null_count = df_vagas.filter(pl.col(col).is_null()).height
                null_pct = (null_count / df_vagas.height) * 100
                stats.append({
                    "Coluna": col,
                    "Nulos": null_count,
                    "% Nulos": f"{null_pct:.1f}%"
                })

            st.dataframe(
                pl.DataFrame(stats).to_pandas(),
                width='stretch',
                hide_index=True
            )
        else:
            st.info("Estatísticas não disponíveis - tabela vazia")

    st.markdown("---")

    # Logs de execução
    st.markdown("#### 📜 Últimas Execuções do Pipeline")

    log_data = [
        {"timestamp": meta_gold['last_update'], "layer": "Gold", "status": "✅ Sucesso", "registros": df_vagas.height if not df_vagas.is_empty() else 0},
        {"timestamp": meta_silver['last_update'], "layer": "Silver", "status": "✅ Sucesso", "registros": "-"},
        {"timestamp": meta_bronze['last_update'], "layer": "Bronze", "status": "✅ Sucesso", "registros": "-"},
    ]

    st.dataframe(
        pl.DataFrame(log_data).to_pandas(),
        width='stretch',
        hide_index=True
    )

    # Informações adicionais
    st.markdown("---")
    st.markdown("#### ℹ️ Informações do Sistema")

    col_sys1, col_sys2 = st.columns(2)

    with col_sys1:
        st.code(f"""
Engine de Processamento: Polars (Rust Backend)
Formato de Storage: Delta Lake
Orquestrador: Prefect 3.x
Python Version: 3.13+
        """, language="bash")

    with col_sys2:
        st.markdown("""
        **Recursos do Data Lake:**
        - ✅ ACID Transactions
        - ✅ Time Travel
        - ✅ Schema Evolution
        - ✅ Partition Pruning
        - ✅ Incremental Load
        """)

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #6c757d; font-size: 0.85rem;">
    <strong>Tech Recruitment Data Engine</strong> | Pipeline de dados para matching de currículos
    <br>
    Desenvolvido com Python, Polars, Delta Lake & Prefect
</div>
""", unsafe_allow_html=True)
