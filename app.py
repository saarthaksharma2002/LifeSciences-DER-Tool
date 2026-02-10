# app.py

import streamlit as st
import pandas as pd
import json
from mappings import MAPPING, CATEGORY_ORDER
import processor as proc

st.set_page_config(page_title="LifeSciences DER Automation Tool", layout="wide")

def main():
    st.title("🧬 LifeSciences DER Automation Tool")
    app_choice = st.sidebar.radio("Choose Operation", ["DER JSON Creator", "DER ZIP Data Compiler"])
    st.divider()

    if app_choice == "DER JSON Creator":
        run_json_creator()
    else:
        run_zip_compiler()

def run_json_creator():
    st.header("📄 DER JSON Creator")
    uploaded_files = st.file_uploader("Upload SQL files", type=["sql"], accept_multiple_files=True)

    if uploaded_files:
        final_json = proc.create_final_json(uploaded_files)
        st.json(final_json)
        
        st.download_button(
            "⬇️ Download JSON",
            json.dumps(final_json, indent=4),
            "DER_JSON_FINAL.json",
            "application/json"
        )

def run_zip_compiler():
    st.header("📦 DER ZIP Data Compiler")
    mode = st.selectbox("Select processing mode", [
        "Aggregated (Customer level)",
        "Use this for more than 2 columns",
        "Contact Validity Compilation"
    ])
    uploaded_files = st.file_uploader("Upload CSV files", type=["csv"], accept_multiple_files=True)

    if uploaded_files:
        if mode == "Aggregated (Customer level)":
            df = pd.concat((pd.read_csv(f) for f in uploaded_files), ignore_index=True)
            num_cols = df.select_dtypes(include="number").columns
            final_df = df.groupby("customer", as_index=False)[num_cols].sum()
            final_df = proc.add_health_system_mapping(final_df, MAPPING)
            st.dataframe(final_df, use_container_width=True)

        elif mode == "Use this for more than 2 columns":
            dfs = [pd.read_csv(f) for f in uploaded_files]
            final_df = dfs[0]
            for i in range(1, len(dfs)):
                common_cols = [col for col in final_df.columns if col in dfs[i].columns]
                final_df = pd.merge(final_df, dfs[i], on=common_cols, how='outer')
            
            final_df = proc.add_health_system_mapping(final_df, MAPPING)
            id_cols = ["customer", "Health System Name", "prid", "prnm", "plid", "plnm"]
            existing_ids = [c for c in id_cols if c in final_df.columns]
            metric_cols = [c for c in final_df.columns if c not in existing_ids]
            
            sorted_metrics = sorted(metric_cols, key=proc.get_vaccine_sort_key)
            final_df = final_df[existing_ids + sorted_metrics]
            st.dataframe(final_df, use_container_width=True)
            st.download_button("⬇️ Download CSV", final_df.to_csv(index=False), "joined_data.csv")

        elif mode == "Contact Validity Compilation":
            dfs = [pd.read_csv(f) for f in uploaded_files]
            merged_df = dfs[0]
            for d in dfs[1:]:
                merged_df = merged_df.merge(d, on="customer", how="outer")
            
            merged_df = merged_df.fillna(0)
            res = proc.compile_contact_validity(merged_df)
            res = proc.add_health_system_mapping(res, MAPPING)
            
            # Column reordering for Contact Validity
            fixed = ["customer", "Health System Name", "Category"]
            metrics = sorted([c for c in res.columns if c not in fixed], key=proc.get_vaccine_sort_key)
            res = res[fixed + metrics]
            
            st.dataframe(res, use_container_width=True)
            st.download_button("⬇️ Download CSV", res.to_csv(index=False), "contact_validity.csv")

if __name__ == "__main__":
    main()
