# processor.py

import pandas as pd
import re
from mappings import CATEGORY_CONFIG, VACCINE_ORDER_PATTERNS

# --- CSV PROCESSING LOGIC ---

def add_health_system_mapping(df, mapping_dict):
    df = df.copy()
    df.insert(1, "Health System Name", df["customer"].map(mapping_dict).fillna(""))
    return df

def compile_contact_validity(df):
    records = []
    contact_suffixes = [v for v in CATEGORY_CONFIG.values() if v != ""]
    
    den_cols = [c for c in df.columns if c.startswith("den_")]
    num_cols = [
        c for c in df.columns 
        if c.startswith("num_") and not any(c.endswith(s) for s in contact_suffixes)
    ]

    for _, row in df.iterrows():
        for category, suffix in CATEGORY_CONFIG.items():
            out = {"customer": row["customer"], "Category": category}
            for den in den_cols:
                out[den] = row[den] if category == "Total" else 0
            for num in num_cols:
                col = num + suffix if suffix else num
                out[num] = row[col] if col in df.columns else 0
            records.append(out)

    return pd.DataFrame(records)

def get_vaccine_sort_key(col_name):
    for index, vaccine in enumerate(VACCINE_ORDER):
        if vaccine in col_name.lower():
            prefix_priority = 0 if col_name.startswith("den_") or col_name.startswith("a_b_den") else 1
            return (index, prefix_priority, col_name)
    return (999, 0, col_name)

# --- JSON/SQL PROCESSING LOGIC ---

def clean_sql_query(file_content):
    # Remove multi-line comments
    content = re.sub(r"/\*.*?\*/", "", file_content, flags=re.DOTALL)
    # Remove single-line comments
    content = re.sub(r"--.*?$", "", content, flags=re.MULTILINE)
    # Collapse whitespace
    return re.sub(r"\s+", " ", content).strip()

def create_final_json(uploaded_files):
    metrics = []
    excluded_cust = [
        "kaiser-staging","kpphm-prod","kpphmi-prod",
        "kpphmi-staging","kpwa-prod","kpwa-staging","jhah-prod"
    ]
    
    for idx, uploaded_file in enumerate(uploaded_files, start=1):
        raw_content = uploaded_file.read().decode("utf-8")
        cleaned_query = clean_sql_query(raw_content)
        
        metrics.append({
            "id": idx,
            "metric": f"DER_{uploaded_file.name.replace('.sql', '')}",
            "level": "l2",
            "supported_customers": {
                "included": [],
                "excluded": excluded_cust
            },
            "queries": {
                "snowflake": {"database": "DAP", "schema": "L2", "query": cleaned_query},
                "postgres": {"database": "postgres", "schema": "l2", "query": cleaned_query}
            }
        })
    return {"metrics": metrics}

