import streamlit as st
import pandas as pd
import os
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import networkx as nx
from pyvis.network import Network

# --- Configuração da página ---
st.set_page_config(page_title="Google Analyzer - Termos e Resultados", layout="wide")

st.title("🔍 Google Analyzer - Termos e Resultados")
st.write("Explore os resultados extraídos pelo scraper do Google via Apify.")

# --- Caminho dos dados ---
DATA_PATH = "/data/"


# --- Carregar arquivos ---
@st.cache_data
def load_data():
    base_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(base_path, "../data")  # tenta primeiro o caminho local

    # Se não encontrar, tenta no mesmo diretório do deploy (Streamlit Cloud)
    if not os.path.exists(data_path):
        data_path = os.path.join(base_path, "data")

    st.write(f"📂 Lendo dados de: {data_path}")

    dfs = {}
    try:
        dfs["organic"] = pd.read_csv(os.path.join(data_path, "organic_results.csv"))
        dfs["paa"] = pd.read_csv(os.path.join(data_path, "people_also_ask.csv"))
        dfs["related"] = pd.read_csv(os.path.join(data_path, "related_queries_clean.csv"))
        st.success("✅ Dados carregados com sucesso!")
    except FileNotFoundError as e:
        st.warning("⚠️ Nenhum dado encontrado. Rode o scraper primeiro.")
        st.error(str(e))
        return None
    return dfs

data = load_data()
if not data:
    st.stop()

organic = data["organic"]
paa = data["paa"]
related = data["related"]

# --- Estatísticas simples ---
st.header("📊 Estatísticas")
col1, col2, col3 = st.columns(3)
col1.metric("Total de resultados orgânicos", len(organic))
col2.metric("Total de perguntas", len(paa))
col3.metric("Total de termos relacionados", len(related))

# --- Seção: resultados orgânicos ---
st.header("🌐 Resultados Orgânicos")
st.dataframe(organic.head(20))

# --- Seção: People Also Ask ---
st.header("💭 Perguntas frequentes (People Also Ask)")

text_paa = " ".join(paa["question"].dropna().astype(str).tolist())
if text_paa.strip():
    wc = WordCloud(width=900, height=400, background_color="white").generate(text_paa)
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.imshow(wc, interpolation="bilinear")
    ax.axis("off")
    st.pyplot(fig)
else:
    st.info("Nenhuma pergunta encontrada.")

# --- Seção: Termos Relacionados ---
st.header("🔗 Termos Relacionados")
st.dataframe(related.head(20))


# --- Filtros interativos ---
# --- Combinar termos de TODAS as tabelas e normalizar ---
st.sidebar.header("🔎 Filtros")
def _norm(s):
    return s.dropna().astype(str).str.strip().str.lower()

series_list = []
for df in (organic, paa, related):
    if "__term" in df.columns:
        series_list.append(_norm(df["__term"]))

if not series_list:
    st.warning("Nenhuma coluna __term encontrada nos dados.")
    st.stop()

all_terms = pd.concat(series_list, ignore_index=True)
termos_unicos = sorted(all_terms.unique().tolist())

termo_selecionado= st.sidebar.multiselect(
    "Filtrar por termo pesquisado",
    termos_unicos,
    default=termos_unicos[:1]  # opcional: seleciona o primeiro automaticamente
)
st.sidebar.write(f"**Termos selecionados:** {', '.join(termo_selecionado)}")

# --- Função de filtro consistente para todas as tabelas ---
def filtra_por_termo(df, termos):
    if "__term" not in df.columns or df.empty:
        return df.iloc[0:0]
    serie_norm = _norm(df["__term"])
    # se o usuário selecionou 1 termo, força pra lista
    if isinstance(termos, str):
        termos = [termos]
    termos_norm = [t.strip().lower() for t in termos]
    return df[serie_norm.isin(termos_norm)]

organic_f = filtra_por_termo(organic, termo_selecionado)
paa_f     = filtra_por_termo(paa, termo_selecionado)
related_f = filtra_por_termo(related, termo_selecionado)


# Mostrar dados filtrados
st.subheader(f"📄 Resultados para {', '.join(termo_selecionado)}")
st.dataframe(organic_f)


# Filtro por termo principal
st.subheader("🔗 Mapa de Palavras Relacionadas")

if related_f.empty:
    st.info("Nenhum termo relacionado encontrado para os termos selecionados.")
else:
# Criar grafo
    G = nx.Graph()
    for _, row in df.iterrows():
        G.add_node(row["__term"], color="#1f77b4", size=30)
        G.add_node(row["query"], color="#ff7f0e", size=20)
        G.add_edge(row["__term"], row["query"])

# Gerar visualização interativa
    net = Network(height="600px", width="100%", bgcolor="#ffffff", font_color="black", notebook=False)
    net.from_nx(G)
    net.repulsion(node_distance=150, spring_length=200)
    net.save_graph("related_terms_graph.html")

    
    with open("related_terms_graph.html", "r", encoding="utf-8") as f:
        html = f.read()
    st.components.v1.html(html, height=600, scrolling=False)