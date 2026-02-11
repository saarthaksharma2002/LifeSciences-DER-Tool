# processor.py
import pandas as pd
import re
from mappings import CATEGORY_CONFIG, VACCINE_ORDER

def add_health_system_mapping(df, mapping_dict):
    df = df.copy()
    if "customer" in df.columns:
        df.insert(1, "Health System Name", df["customer"].map(mapping_dict).fillna(""))
    return df

def get_vaccine_sort_key(col_name):
    # Specific priority for common base columns
    if "total_attributed_lives" in col_name.lower():
        return (-1, 0, col_name)
    
    for index, vaccine in enumerate(VACCINE_ORDER):
        if vaccine in col_name.lower():
            # den_ or count_ gets 0, num_ gets 1
            prefix_priority = 0 if any(col_name.startswith(p) for p in ["den_", "count_", "a_b_den"]) else 1
            return (index, prefix_priority, col_name)
    return (999, 0, col_name)

def process_provider_long_format(df):
    """Logic for Provider Type + Contact (Long Format)"""
    rows = []
    # Identify which columns are metrics vs categories
    for _, row in df.iterrows():
        m_str, pt_raw, val, cust = str(row['metric']), str(row['provider_type']), row['value'], row['customer']
        
        found_cat, base_m = None, None
        for cat_name, suffix in CATEGORY_CONFIG.items():
            if suffix == "" and "_patients_total" in m_str: # Handling the 'Total' logic
                if m_str.endswith("_patients_total"):
                    found_cat = "Total"
                    base_m = m_str.replace("_patients_total", "")
                    break
            elif suffix != "" and m_str.lower().endswith(suffix):
                found_cat = cat_name
                base_m = m_str[:-len(suffix)]
                break
        
        if not found_cat: continue
        
        # Clean provider type (remove base metric prefix if present)
        p_type = pt_raw[len(base_m):].strip("_") if pt_raw.lower().startswith(base_m.lower()) else pt_raw
        rows.append({'customer': cust, 'Category': found_cat, 'Provider Type': p_type.title(), base_m: val})
    
    res = pd.DataFrame(rows)
    return res.groupby(['customer', 'Health System Name', 'Category', 'Provider Type'], as_index=False).sum() if not res.empty else res

def process_age_format(uploaded_files):
    """Logic for Age Compiler: Rows are ages, columns are counts"""
    combined_df = pd.DataFrame()
    for f in uploaded_files:
        df = pd.read_csv(f)
        # Pivot so age becomes columns
        if 'current_age' in df.columns:
            # Get the metric name (the column that isn't age or customer)
            metric_col = [c for c in df.columns if c not in ['current_age', 'customer']][0]
            pivoted = df.pivot(index='customer', columns='current_age', values=metric_col).reset_index()
            combined_df = pd.concat([combined_df, pivoted], axis=0)
    
    return combined_df.groupby('customer').sum().reset_index()

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


