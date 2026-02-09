# app.py

import streamlit as st
import pandas as pd
import io
from mappings import MAPPING, CATEGORY_ORDER
from processor import process_file_universal, get_vaccine_sort_key

st.set_page_config(page_title="LifeSciences DER Automation Tool", layout="wide")

def run_zip_compiler():
    st.header("📦 DER ZIP Data Compiler")
    mode = st.selectbox("Select processing mode", [
        "Contact Validity Compilation", "provider type + contact", 
        "Customer level aggregation", "Multiple columns in one csv format"
    ])
    uploaded_files = st.file_uploader("Upload CSV files", type=["csv"], accept_multiple_files=True)

    if uploaded_files:
        with st.spinner("Compiling data..."):
            if mode in ["Contact Validity Compilation", "provider type + contact"]:
                processed_dfs = []
                for f in uploaded_files:
                    df_raw = pd.read_csv(f)
                    res = process_file_universal(df_raw, mode)
                    if not res.empty: processed_dfs.append(res)
                
                if processed_dfs:
                    final_df = pd.concat(processed_dfs, ignore_index=True)
                    group_cols = ["customer", "Category"]
                    if mode == "provider type + contact": group_cols.append("Provider Type")
                    
                    final_df = final_df.groupby(group_cols, as_index=False).sum()
                    final_df.insert(1, "Health System Name", final_df["customer"].map(MAPPING).fillna(""))
                    final_df["Category"] = pd.Categorical(final_df["Category"], categories=CATEGORY_ORDER, ordered=True)
                    final_df = final_df.sort_values(group_cols)
                    
                    id_cols = ["customer", "Health System Name"]
                    if mode == "provider type + contact": id_cols.append("Provider Type")
                    id_cols.append("Category")
                    
                    metrics = [c for c in final_df.columns if c not in id_cols]
                    sorted_metrics = sorted(metrics, key=get_vaccine_sort_key)
                    final_df = final_df[id_cols + sorted_metrics]
                    
                    st.success("Compilation Successful.")
                    st.dataframe(final_df, use_container_width=True)
                    st.download_button("⬇️ Download CSV", final_df.to_csv(index=False), "compiled_der.csv")

            elif mode == "Customer level aggregation":
                master_df = pd.concat([pd.read_csv(f) for f in uploaded_files]).fillna(0)
                nums = master_df.select_dtypes(include="number").columns
                master_df = master_df.groupby("customer", as_index=False)[nums].sum()
                master_df.insert(1, "Health System Name", master_df["customer"].map(MAPPING).fillna(""))
                st.dataframe(master_df)

def run_json_creator():
    st.header("📄 DER JSON Creator")
    uploaded_sql = st.file_uploader("Upload SQL files", type=["sql"], accept_multiple_files=True)
    if uploaded_sql:
        reports = []
        for f in uploaded_sql:
            content = f.read().decode("utf-8")
            slug = f.name.replace(".sql", "")
            reports.append({
                "slug": slug,
                "name": slug.replace("_", " ").title(),
                "query": content
            })
        st.json({"reports": reports})

def main():
    st.title("🧬 LifeSciences DER Automation Tool")
    choice = st.sidebar.radio("Navigation", ["Data Compiler", "JSON Creator"])
    
    if choice == "Data Compiler":
        run_zip_compiler()
    else:
        run_json_creator()

if __name__ == "__main__":
    main()
