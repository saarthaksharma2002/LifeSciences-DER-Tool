# processor.py
import pandas as pd
import re
from mappings import CATEGORY_CONFIG, VACCINE_ORDER_PATTERNS

def add_health_system_mapping(df, mapping_dict):
    df = df.copy()
    if "customer" in df.columns:
        df.insert(1, "Health System Name", df["customer"].map(mapping_dict).fillna(""))
    return df

def get_vaccine_sort_key(col_name):
    col_lower = col_name.lower()
    if "total_attributed_lives" in col_lower:
        return (-1, 0, col_name)
    
    for index, pattern in enumerate(VACCINE_ORDER_PATTERNS):
        if pattern in col_lower:
            prefix_priority = 0 if any(col_name.startswith(p) for p in ["den_", "count_", "a_b_den"]) else 1
            return (index, prefix_priority, col_name)
    return (999, 0, col_name)

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

def process_provider_long_format(df):
    rows = []
    for _, row in df.iterrows():
        m_str, pt_raw, val, cust = str(row['metric']), str(row['provider_type']), row['value'], row['customer']
        found_cat, base_m = None, None
        for cat_name, suffix in CATEGORY_CONFIG.items():
            if suffix == "" and "_patients_total" in m_str:
                found_cat, base_m = "Total", m_str.replace("_patients_total", "")
                break
            elif suffix != "" and m_str.lower().endswith(suffix):
                found_cat, base_m = cat_name, m_str[:-len(suffix)]
                break
        if not found_cat: continue
        p_type = pt_raw[len(base_m):].strip("_") if pt_raw.lower().startswith(base_m.lower()) else pt_raw
        rows.append({'customer': cust, 'Category': found_cat, 'Provider Type': p_type.title(), base_m: val})
    
    res = pd.DataFrame(rows)
    if res.empty: return res
    return res.groupby(['customer', 'Category', 'Provider Type'], as_index=False).sum()

def process_age_format(uploaded_files):
    all_pivoted = []
    for f in uploaded_files:
        df = pd.read_csv(f)
        if 'current_age' in df.columns and 'customer' in df.columns:
            metric_col = [c for c in df.columns if c not in ['current_age', 'customer']][0]
            pivoted = df.pivot_table(index='customer', columns='current_age', values=metric_col, aggfunc='sum').reset_index()
            all_pivoted.append(pivoted)
    
    if not all_pivoted: return pd.DataFrame()
    final = all_pivoted[0]
    for d in all_pivoted[1:]:
        final = pd.merge(final, d, on='customer', how='outer')
    return final.fillna(0)

def clean_sql_query(file_content):
    content = re.sub(r"/\*.*?\*/", "", file_content, flags=re.DOTALL)
    content = re.sub(r"--.*?$", "", content, flags=re.MULTILINE)
    return re.sub(r"\s+", " ", content).strip()

def create_final_json(uploaded_files):
    metrics = []
    excluded_cust = ["kaiser-staging","kpphm-prod","kpphmi-prod","kpphmi-staging","kpwa-prod","kpwa-staging","jhah-prod"]
    for idx, f in enumerate(uploaded_files, start=1):
        content = f.read().decode("utf-8")
        cleaned = clean_sql_query(content)
        metrics.append({
            "id": idx, "metric": f"DER_{f.name.replace('.sql','')}", "level": "l2",
            "supported_customers": {"included": [], "excluded": excluded_cust},
            "queries": {
                "snowflake": {"database": "DAP", "schema": "L2", "query": cleaned},
                "postgres": {"database": "postgres", "schema": "l2", "query": cleaned}
            }
        })
    return {"metrics": metrics}
