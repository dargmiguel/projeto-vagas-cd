import streamlit as st
import polars as pl
from pathlib import Path
import yaml
import logging

# Configuração de Logs (boa prática para identificar erros no app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Funções de Carregamento de Dados e Configuração ---

@st.cache_data
def carregar_config():
    """Carrega o arquivo de configuração YAML.
    O @st.cache_data garante que isso seja executado apenas uma vez por sessão."""
    config_path = Path("src/gold/config/gold_config.yaml")
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        st.error("Arquivo de configuração 'gold_config.yaml' não encontrado.")
        st.stop()

@st.cache_data
def carregar_tabela_gold(nome_tabela: str) -> pl.DataFrame:
    """Lê uma tabela Delta da camada Gold.
    O @st.cache_data é crucial: ele evita recarregar os dados do disco a cada interação do usuário."""
    config = carregar_config()
    settings = config['settings']
    path_base = Path(settings['gold_output_path']) / nome_tabela

    logger.info(f"Tentando carregar a tabela Delta de: {path_base}")

    try:
        # Usamos scan_delta para performance e collect para materializar o DataFrame
        lf = pl.scan_delta(str(path_base))
        return lf.collect()
    except Exception as e:
        st.error(f"Erro ao carregar a tabela '{nome_taga}'. Verifique se o pipeline Gold foi executado.")
        st.error(f"Detalhes do erro: {e}")
        # Retorna um DataFrame vazio para evitar que o app quebre
        return pl.DataFrame()

# --- Interface do Usuário (UI) com Streamlit ---

st.set_page_config(layout="wide") # Usa toda a largura da página
st.title("📊 Dashboard de Vagas de Tecnologia")
st.markdown("Análise exploratória dos dados coletados e processados pela camada Gold.")

# Botão para forçar a recarga dos dados, limpando o cache
if st.button("🔄 Recarregar Dados"):
    st.cache_data.clear()

st.sidebar.header("Controles")
st.sidebar.info("Os dados são atualizados a cada 30 minutos pelo pipeline. Use o botão 'Recarregar Dados' para buscar as informações mais recentes.")

# --- Seções do Dashboard ---

# 1. Tabela Dimensão de Vagas
st.header("1. Catálogo de Vagas (dim_vagas)")
df_dim = carregar_tabela_gold("dim_vagas")

if not df_dim.is_empty():
    st.metric(label="Total de Vagas Únicas", value=df_dim.height)
    st.dataframe(df_dim, use_container_width=True, hide_index=True)
else:
    st.warning("Nenhum dado encontrado para 'dim_vagas'.")

st.divider() # Linha separadora

# 2. Agregação de Skills
st.header("2. Top Skills no Mercado (agg_skills_monitor)")
df_skills = carregar_tabela_gold("agg_skills_monitor")

if not df_skills.is_empty():
    # Assumindo que a tabela tem colunas 'skill' e 'count'
    # Se os nomes forem diferentes, ajuste aqui
    if 'skill' in df_skills.columns and 'count' in df_skills.columns:
        st.bar_chart(df_skills, x='skill', y='count', use_container_width=True)
        st.subheader("Dados Brutos da Agregação")
        st.dataframe(df_skills, use_container_width=True, hide_index=True)
    else:
        st.error("As colunas 'skill' e 'count' não foram encontradas na tabela 'agg_skills_monitor'.")
        st.dataframe(df_skills) # Mostra o que foi carregado para depuração
else:
    st.warning("Nenhum dado encontrado para 'agg_skills_monitor'.")

st.divider()

# 3. Tendências de Mercado
st.header("3. Tendências de Publicação de Vagas (agg_market_trends)")
df_trends = carregar_tabela_gold("agg_market_trends")

if not df_trends.is_empty():
    # Assumindo que a tabela tem colunas de data e contagem
    # Vamos tentar identificar a coluna de data e a de contagem
    date_col = None
    count_col = None
    for col in df_trends.columns:
        if df_trends[col].dtype == pl.Date or df_trends[col].dtype == pl.Datetime:
            date_col = col
        if 'count' in col.lower() or 'vaga' in col.lower():
            count_col = col

    if date_col and count_col:
        # Converte a coluna de data para o formato do Streamlit, se necessário
        df_trends = df_trends.with_columns(pl.col(date_col).cast(pl.Date))
        st.line_chart(df_trends, x=date_col, y=count_col, use_container_width=True)
        st.subheader("Dados Brutos da Agregação")
        st.dataframe(df_trends, use_container_width=True, hide_index=True)
    else:
        st.error(f"Não foi possível identificar as colunas de data e contagem em 'agg_market_trends'. Colunas encontradas: {df_trends.columns}")
        st.dataframe(df_trends)
else:
    st.warning("Nenhum dado encontrado para 'agg_market_trends'.")
