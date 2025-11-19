import re
import pendulum
import numpy as np
import pandas as pd

import torch
import faiss
from rapidfuzz import fuzz, process
from sentence_transformers import SentenceTransformer, CrossEncoder

from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# --- Configuration ---
# We centralize all settings here so the pipeline is portable.
SNOWFLAKE_CONN_ID = "snowflake_default"

DB = "DATA_PROJECT"
SCHEMA = "PUBLIC"

# The tables we are reading from and writing to.
CRM_TABLE = "CRM_DETAILS"
TRX_TABLE = "TRANSACTIONS"
OUTPUT_TABLE = "TRANSACTIONS_MATCHED"

# Column mapping for our input data.
SOURCE_COL = "MERCHANT_NAME"
MASTER_COL = "COMPANY_NAME"
MARKET_COL = "MARKET"
ID_COL = "ID"

# These thresholds control how strict the matching is.
# We balance precision (being right) and recall (finding enough matches).
FUZZY_THRESHOLD = 90           # Minimum score for simple spelling matches.
SEMANTIC_THRESHOLD = 0.85      # Minimum confidence for AI/Vector matches.
SEMANTIC_WEIGHT = 0.35         # How much the AI score counts towards the total.
FUZZY_WEIGHT = 0.65            # How much the spelling score counts towards the total.
JACCARD_THRESHOLD = 0.60       # A safety check to ensure words actually overlap.
K_CANDIDATES = 10              # How many similar companies the AI should look for.

# --- Preprocessing & Helper Functions ---

def clean_text(t: str) -> str:
    """
    This function cleans up the company names to make matching easier.
    
    Real-world examples of what this solves:
    1. Legal Suffixes: "Spotify USA, Ltd." -> "spotify"
    2. Transaction Noise: "AMZN Mktp US" -> "amzn mktp us" (later matched to Amazon)
    3. Special Characters: "O'Reilly Auto Parts" -> "oreilly auto parts"
    
    It removes common business suffixes (like LLC, Inc, GmbH) so we are 
    comparing the actual brand names, not the legal entities.
    """
    if not isinstance(t, str): 
        return ""
    t = t.lower().strip()
    t = t.replace("_", " ").replace("-", " ").replace("#", " ")
    t = re.sub(r"\s\d{4,}$", "", t)

    suffixes = [
        "inc", "corp", "co", "ltd", "llc", "lp", "llp", "pllc",
        "gmbh", "enterprises", "industries", "solutions", "ventures",
        "holdings", "group", "services", "technology", "systems",
    ]
    suffix_re = r"\s(" + "|".join(suffixes) + r")$"
    t = re.sub(suffix_re, "", t)

    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def jaccard(a, b):
    """
    Calculates the overlap between two strings based on their words.
    We use this as a cheap, fast filter to reject matches that look similar
    to the AI but are actually completely different companies.
    
    Example: "Delta Air Lines" vs "Delta Faucets" might be close in vector space
    due to the word 'Delta', but this filter helps distinguish them if the context differs.
    """
    a, b = clean_text(a), clean_text(b)
    set1, set2 = set(a.split()), set(b.split())
    if not set1 and not set2:
        return 1.0
    if not set1 or not set2:
        return 0.0
    return len(set1 & set2) / len(set1 | set2)


def ultimate_validation(a, b, jaccard_threshold=0.6):
    """
    This is our final safety check. Even if the AI thinks two names match,
    we run them through this logic to verify the spelling and word overlap
    are strong enough to be a confirmed match.
    """
    a_clean, b_clean = clean_text(a), clean_text(b)
    if not a_clean or not b_clean:
        return False

    if jaccard(a, b) >= jaccard_threshold:
        return True

    if fuzz.WRatio(a_clean, b_clean) >= 95:
        return True

    set_a, set_b = set(a_clean.split()), set(b_clean.split())
    # Check if one name is inside the other (e.g., "Apple" vs "Apple Computers")
    if min(len(set_a), len(set_b)) > 1:
        if set_a.issubset(set_b) or set_b.issubset(set_a):
            if fuzz.WRatio(a_clean, b_clean) >= 88:
                return True

    short, long_ = sorted([a_clean, b_clean], key=len)
    if long_.startswith(short) and fuzz.WRatio(short, long_) >= 90:
        return True

    return False

# --- AI / Vector Search Engine ---

def semantic_pass(source_names, master_names):
    """
    This function uses Deep Learning to find matches based on meaning.
    
    Why we need this:
    Traditional string matching fails on abbreviations or synonyms.
    - "The Home Depot" vs "Home Dept" (String matching works)
    - "Intl Bus Mach" vs "IBM" (String matching fails, AI succeeds)
    
    1. We convert company names into numbers (embeddings) using a Transformer model.
    2. We use FAISS to instantly find the closest matches in that numerical space.
    3. We re-rank the best candidates to get a final confidence score.
    """
    # Check if we have a GPU available for faster processing.
    device = "cuda" if torch.cuda.is_available() else "cpu"

    retriever = SentenceTransformer("Snowflake/snowflake-arctic-embed-l", device=device)
    cross = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2", device=device)

    src_clean = [clean_text(s) for s in source_names]
    mst_clean = [clean_text(m) for m in master_names]

    src_emb = retriever.encode(src_clean, convert_to_tensor=False).astype("float32")
    mst_emb = retriever.encode(mst_clean, convert_to_tensor=False).astype("float32")

    faiss.normalize_L2(src_emb)
    faiss.normalize_L2(mst_emb)

    # We use FAISS for efficient similarity search.
    index = faiss.IndexFlatIP(mst_emb.shape[1])
    index.add(mst_emb)

    top_k = min(K_CANDIDATES, len(master_names))
    _, idxs = index.search(src_emb, top_k)

    final_matches = {}

    for i, src in enumerate(source_names):
        candidates = [master_names[j] for j in idxs[i] if j != -1]
        if not candidates:
            continue

        pairs = [[clean_text(src), clean_text(c)] for c in candidates]
        semantic_scores = cross.predict(pairs)

        fuzzy_scores = np.array([fuzz.WRatio(src, c) / 100 for c in candidates])
        
        # We combine the AI score and the spelling score into one "Hybrid" score.
        hybrid = SEMANTIC_WEIGHT * semantic_scores + FUZZY_WEIGHT * fuzzy_scores

        valid = []
        for c, score in zip(candidates, hybrid):
            # Only keep matches that pass both our score threshold and safety checks.
            if score >= SEMANTIC_THRESHOLD and ultimate_validation(src, c, JACCARD_THRESHOLD):
                valid.append((c, score))

        if valid:
            best = max(valid, key=lambda x: x[1])
            final_matches[src] = best[0]

    return final_matches

# --- Main Matching Pipeline ---

def hybrid_match(source_df, master_df):
    """
    This is the main driver for the matching process.
    
    To improve performance, we group data by 'Market' first. This ensures
    we only compare companies within the same region.
    
    We use a waterfall approach:
    1. Fast Pass: Check for simple spelling matches (e.g., "Uber" vs "Uber *Trip").
    2. Slow Pass: Use the AI model on whatever is left over (e.g., tricky abbreviations).
    """
    df_s = source_df.copy()
    df_m = master_df.copy()

    # Standardize the market column to ensure our grouping works correctly.
    df_s[MARKET_COL] = df_s[MARKET_COL].astype(str).str.upper()
    df_m[MARKET_COL] = df_m[MARKET_COL].astype(str).str.upper()

    df_s[SOURCE_COL] = df_s[SOURCE_COL].astype(str)
    df_m[MASTER_COL] = df_m[MASTER_COL].astype(str)

    master_by_market = df_m.groupby(MARKET_COL)
    source_by_market = df_s.groupby(MARKET_COL)

    matches = {}
    
    # Find markets that exist in both datasets.
    markets = sorted(set(source_by_market.groups.keys()) & set(master_by_market.groups.keys()))

    for m in markets:
        s_group = source_by_market.get_group(m)
        m_group = master_by_market.get_group(m)

        master_names = m_group[MASTER_COL].dropna().unique().tolist()
        if not master_names:
            continue

        source_names = s_group[SOURCE_COL].dropna().unique().tolist()
        if not source_names:
            continue

        # Phase 1: Fuzzy Matching
        # This catches typos and small variations very quickly.
        fuzzy_hits = {}
        for s in source_names:
            best = process.extractOne(s, master_names, scorer=fuzz.WRatio)
            if best:
                name, score, _ = best
                if score >= FUZZY_THRESHOLD and ultimate_validation(s, name, JACCARD_THRESHOLD):
                    fuzzy_hits[s] = name

        # Phase 2: Semantic Matching
        # We only run the expensive AI model on the names we couldn't match in Phase 1.
        remaining = [s for s in source_names if s not in fuzzy_hits]
        semantic_hits = semantic_pass(remaining, master_names)

        # Combine results from both phases.
        all_hits = {**fuzzy_hits, **semantic_hits}

        for s_name, m_name in all_hits.items():
            matches[(m, s_name)] = m_name

    # Map the matched IDs back to the original dataframe.
    crm_lookup = {(row[MARKET_COL], row[MASTER_COL]): row[ID_COL] for _, row in df_m.iterrows()}

    def get_crm(row):
        key = (row[MARKET_COL], row[SOURCE_COL])
        if key not in matches:
            return None
        matched_master = matches[key]
        return crm_lookup.get((row[MARKET_COL], matched_master))

    df_s["crm_id"] = df_s.apply(get_crm, axis=1)
    return df_s

# --- Airflow DAG Definition ---

@dag(
    dag_id="crm_ai_match_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ml", "entity-resolution", "snowflake"],
)
def crm_ai_match_pipeline():

    @task
    def run_pipeline():
        """
        This task coordinates the entire process:
        1. It pulls the raw data from Snowflake.
        2. It runs the Python matching logic.
        3. It writes the results back to Snowflake efficiently.
        """
        hook = SnowflakeHook(SNOWFLAKE_CONN_ID)

        # Extract data
        crm_df = hook.get_pandas_df(
            f"SELECT id, company_name, market FROM {DB}.{SCHEMA}.{CRM_TABLE}"
        )

        trx_df = hook.get_pandas_df(
            f"SELECT * FROM {DB}.{SCHEMA}.{TRX_TABLE}"
        )

        # Run the matching algorithm
        matched = hybrid_match(trx_df, crm_df)

        # Ensure column names match the database expectations
        matched = matched.rename(columns={"crm_id": "CRM_ID"})

        # Create the output table if it doesn't exist yet
        hook.run(f"""
            CREATE TABLE IF NOT EXISTS {DB}.{SCHEMA}.{OUTPUT_TABLE}
            LIKE {DB}.{SCHEMA}.{TRX_TABLE};
        """)

        # Add the new ID column if it is missing
        hook.run(f"""
            ALTER TABLE {DB}.{SCHEMA}.{OUTPUT_TABLE}
            ADD COLUMN IF NOT EXISTS CRM_ID VARCHAR;
        """)

        # Clear out old data so we can perform a clean insert
        hook.run(f"TRUNCATE TABLE {DB}.{SCHEMA}.{OUTPUT_TABLE}")

        # Bulk insert the results using a cursor for better performance
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            cols = list(matched.columns)
            col_list_sql = ", ".join([f'"{c}"' for c in cols])
            placeholders = ", ".join(["%s"] * len(cols))
            
            insert_sql = f"""
                INSERT INTO {DB}.{SCHEMA}.{OUTPUT_TABLE}
                ({col_list_sql})
                VALUES ({placeholders})
            """

            rows = matched.to_records(index=False).tolist()
            cursor.executemany(insert_sql, rows)
            conn.commit()

        finally:
            cursor.close()
            conn.close()

        return f"Completed matching. Wrote {len(matched)} rows."

    run_pipeline()


crm_ai_match_pipeline()