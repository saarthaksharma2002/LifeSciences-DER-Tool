# processor.py
import pandas as pd
import re
from mappings import CATEGORY_CONFIG, VACCINE_ORDER_PATTERNS, POWERBI_ORDER

def add_health_system_mapping(df, mapping_dict):
    df = df.copy()
    if "customer" in df.columns:
        df.insert(1, "Health System Name", df["customer"].map(mapping_dict).fillna(""))
    return df

def get_vaccine_sort_key(col_name):
    # Fix: Convert col_name to string to avoid AttributeError on integers (Ages)
    col_str = str(col_name)
    col_lower = col_str.lower()
    
    # Handle numeric column names for Age Format Compiler
    if col_str.isdigit():
        return (1000, int(col_str), col_str)
        
    if "total_attributed_lives" in col_lower:
        return (-1, 0, col_str)
    
    for index, pattern in enumerate(VACCINE_ORDER_PATTERNS):
        if pattern in col_lower:
            # den_ or count_ gets priority 0, num_ gets 1
            prefix_priority = 0 if any(col_str.startswith(p) for p in ["den_", "count_", "a_b_den"]) else 1
            return (index, prefix_priority, col_str)
            
    return (999, 0, col_str)

def reorder_powerbi_columns(df):
    """
    Specifically for the PowerBi feature: orders columns exactly as requested.
    """
    fixed_headers = ["customer", "Health System Name", "Category"]
    
    # Identify which columns from our requested PowerBi order actually exist in the data
    ordered_metrics = [c for c in POWERBI_ORDER if c in df.columns]
    
    # Identify any columns that weren't in our list but are in the DF (safety catch)
    other_cols = [c for c in df.columns if c not in fixed_headers and c not in ordered_metrics]
    
    return df[fixed_headers + ordered_metrics + other_cols]

def compile_contact_validity(df):
    records = []
    
    # Identify unique base metric names
    all_cols = df.columns
    base_metrics = set()
    for c in all_cols:
        if "_patients_" in c:
            base_metrics.add(c.split("_patients_")[0])
    
    # Standalone metrics like den_ or num_ not part of contact breakdown
    standalone_metrics = [c for c in all_cols if (c.startswith("den_") or c.startswith("num_") or c.startswith("a_b_den") or c.startswith("count_") or c.startswith("common_")) and "_patients_" not in c]

    for _, row in df.iterrows():
        for category, suffix in CATEGORY_CONFIG.items():
            out = {"customer": row.get("customer", ""), "Category": category}
            
            for bm in base_metrics:
                search_suffix = suffix
                if category == "Total":
                    search_suffix = "_patients_total" if f"{bm}_patients_total" in df.columns else ""
                
                col_name = f"{bm}{search_suffix}"
                out[bm] = row[col_name] if col_name in df.columns else 0
            
            for sm in standalone_metrics:
                # Standalone metrics (totals) only appear in the 'Total' category row
                out[sm] = row[sm] if category == "Total" else 0
                
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
    final = pd.concat(all_pivoted, ignore_index=True).fillna(0)
    return final.groupby('customer').sum().reset_index()

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
