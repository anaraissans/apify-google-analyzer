import os, time, json
import pandas as pd
from urllib.parse import urlparse, parse_qs, unquote_plus
from apify_client import ApifyClient
from dotenv import load_dotenv


# Carrega variáveis de ambiente (.env)
load_dotenv()

# Token Apify
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
if not APIFY_TOKEN:
    raise ValueError("❌ APIFY_TOKEN não encontrado. Configure no .env")

# Carrega config.json
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

# Inicializa cliente Apify
client = ApifyClient(APIFY_TOKEN)

# Lê variáveis da config
ACTOR_ID = CONFIG.get("actor_id", "apify/google-search-scraper")
TERMS = CONFIG.get("terms", [])
COUNTRY = CONFIG.get("country_code", "br")
LANGUAGE = CONFIG.get("language_code", "pt-BR")
MAX_PAGES = CONFIG.get("max_pages_per_query", 2)

# Diretório de saída
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "../data/")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def actor_input_for(term: str):
    return {
        "queries": term,
        "maxPagesPerQuery": MAX_PAGES,
        "countryCode": COUNTRY,
        "languageCode": LANGUAGE,
        "includeUnfilteredResults": CONFIG.get("include_unfiltered_results", False),
        "mobileResults": CONFIG.get("mobile_results", False),
        "saveHtml": CONFIG.get("save_html", False),
        "saveHtmlToKeyValueStore": False,
    }

def export_section(df_raw, section_col, wanted_cols_map, outfile):
    if section_col not in df_raw.columns or df_raw[section_col].isna().all():
        print(f"[skip] {section_col} não existe ou está vazio.")
        return None

    df_exp = df_raw.explode(section_col, ignore_index=True)
    sec_series = df_exp[section_col].dropna()

    sec = pd.json_normalize(
        [x for x in sec_series if isinstance(x, (dict, list))],
        max_level=1
    )

    sec["__term"] = df_exp.loc[sec.index, "__term"].values
    if "searchQuery" in df_exp.columns:
        sec["searchQuery"] = df_exp.loc[sec.index, "searchQuery"].values

    cols = {}
    for outcol, path in wanted_cols_map.items():
        if path in sec.columns:
            cols[outcol] = sec[path]
        else:
            if path == "url":
                cols[outcol] = sec.get("link", pd.Series([""] * len(sec)))
            elif path == "snippet":
                cols[outcol] = sec.get("description", pd.Series([""] * len(sec)))
            else:
                cols[outcol] = pd.Series([""] * len(sec))

    sec_out = pd.DataFrame(cols)
    sec_out["__term"] = sec["__term"]
    if "searchQuery" in sec.columns:
        sec_out["searchQuery"] = sec["searchQuery"]

    def to_scalar(x):
        if isinstance(x, (dict, list)):
            return json.dumps(x, ensure_ascii=False, sort_keys=True)
        return x

    sec_out = sec_out.applymap(to_scalar).fillna("").drop_duplicates()
    sec_out.to_csv(outfile, index=False, encoding="utf-8")
    print(f"[ok] {section_col} → {outfile} ({len(sec_out)} linhas)")
    return sec_out


def extract_query_from_url(u: str) -> str:
    if not isinstance(u, str) or not u:
        return ""
    try:
        parsed = urlparse(u)
        qs = parse_qs(parsed.query)
        q = qs.get("q", [""])[0]
        return unquote_plus(q).strip()
    except Exception:
        return ""


def clean_related_queries(df_related: pd.DataFrame, outfile: str) -> pd.DataFrame:
    if df_related is None or df_related.empty:
        return df_related

    for c in ["query", "url", "__term"]:
        if c not in df_related.columns:
            df_related[c] = ""

    extracted = df_related["url"].map(extract_query_from_url)
    df_related["query"] = df_related["query"].mask(
        df_related["query"].isna() | (df_related["query"].str.strip() == ""), extracted
    )
    df_related["query"] = (
        df_related["query"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    )

    df_related = df_related[df_related["query"] != ""].drop_duplicates(subset=["__term", "query"])
    df_related.to_csv(outfile, index=False, encoding="utf-8")
    print(f"[ok] relatedQueries limpo → {outfile} ({len(df_related)} linhas)")
    return df_related


def main():
    rows = []
    for term in TERMS:
        print(f"Rodando {ACTOR_ID} para termo: {term}")
        run = client.actor(ACTOR_ID).call(run_input=actor_input_for(term))
        dataset_client = client.dataset(run["defaultDatasetId"])

        for item in dataset_client.iterate_items():
            item["__term"] = term
            rows.append(item)

        time.sleep(1)

    df_raw = pd.DataFrame(rows)
    os.makedirs("data", exist_ok=True)
    df_raw.to_csv(os.path.join(OUTPUT_DIR, "news_raw.csv"), index=False, encoding="utf-8")

    organic = export_section(
        df_raw, "organicResults",
        {"title": "title", "url": "url", "snippet": "snippet"},
        os.path.join(OUTPUT_DIR, "organic_results.csv")
    )

    paa = export_section(
        df_raw, "peopleAlsoAsk",
        {"question": "question", "answer": "answer", "url": "link"},
        os.path.join(OUTPUT_DIR, "people_also_ask.csv")
    )

    related = export_section(
        df_raw, "relatedQueries",
        {"query": "query", "url": "url"},
        os.path.join(OUTPUT_DIR, "related_queries.csv")
    )

    clean_related_queries(related, os.path.join(OUTPUT_DIR, "related_queries_clean.csv"))


if __name__ == "__main__":
    main()