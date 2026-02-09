# processor.py

import pandas as pd
from mappings import VACCINE_ORDER_PATTERNS, CATEGORY_CONFIG, CATEGORY_ORDER

def get_vaccine_sort_key(col_name):
    """Sorts columns based on vaccine pattern and Denominator vs Numerator."""
    col_lower = col_name.lower()
    for index, pattern in enumerate(VACCINE_ORDER_PATTERNS):
        if pattern in col_lower:
            is_den = 0 if (col_lower.startswith("den_") or col_lower.startswith("count_")) else 1
            return (index, is_den, col_name)
    return (999, 0, col_name)

def process_file_universal(df, mode):
    """Handles both Long (SQL export) and Wide formats."""
    df = df.fillna(0)
    is_provider_mode = (mode == "provider type + contact")

    # CASE A: LONG FORMAT
    if {'provider_type', 'metric', 'value', 'customer'}.issubset(df.columns):
        rows = []
        for _, row in df.iterrows():
            m_str, pt_raw, val, cust = str(row['metric']), str(row['provider_type']), row['value'], row['customer']
            
            found_cat, base_m = None, None
            for cat_name, suffix in CATEGORY_CONFIG.items():
                if m_str.lower().endswith(suffix):
                    found_cat, base_m = cat_name, m_str[:-len(suffix)]
                    break
            
            if not found_cat: continue
            
            p_type = pt_raw[len(base_m):].strip("_") if pt_raw.lower().startswith(base_m.lower()) else pt_raw
            entry = {'customer': cust, 'Category': found_cat, base_m: val}
            if is_provider_mode: entry['Provider Type'] = p_type.title()
            rows.append(entry)
        
        long_df = pd.DataFrame(rows)
        if long_df.empty: return long_df
        
        group_keys = ['customer', 'Category']
        if is_provider_mode: group_keys.append('Provider Type')
        return long_df.groupby(group_keys, as_index=False).sum()

    # CASE B: WIDE FORMAT
    total_suffix = CATEGORY_CONFIG["Total"]
    base_metrics = [c.replace(total_suffix, "") for c in df.columns if c.endswith(total_suffix)]
    
    if not base_metrics:
        exclude = ["customer", "Health System Name", "prid", "prnm", "plid", "plnm", "Provider Type"]
        base_metrics = [c for c in df.select_dtypes(include="number").columns if c not in exclude]
    
    if not base_metrics: return pd.DataFrame()

    all_cat_chunks = []
    for category in CATEGORY_ORDER:
        suffix = CATEGORY_CONFIG[category]
        temp = df.copy()
        temp["Category"] = category
        for m in base_metrics:
            src = m + suffix
            if src in df.columns: temp[m] = df[src]
            elif category == "Total" and m in df.columns: temp[m] = df[m]
            else: temp[m] = 0
        all_cat_chunks.append(temp)
    
    df_expanded = pd.concat(all_cat_chunks, ignore_index=True)
    group_keys = ["customer", "Category"]
    if is_provider_mode:
        if "plnm" in df_expanded.columns: df_expanded = df_expanded.rename(columns={"plnm": "Provider Type"})
        if "Provider Type" not in df_expanded.columns: df_expanded["Provider Type"] = "All"
        group_keys.append("Provider Type")
        
    return df_expanded.groupby(group_keys, as_index=False)[base_metrics].sum()
