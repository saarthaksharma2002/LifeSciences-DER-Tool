# app.py
import streamlit as st
import pandas as pd
import json
import processor as proc
from mappings import MAPPING

st.set_page_config(page_title="LifeSciences DER Automation Tool", layout="wide")

def main():
    st.sidebar.title("Navigation")
    app_choice = st.sidebar.radio("Choose Operation", ["DER JSON Creator", "DER ZIP Data Compiler"])
    st.title("🧬 LifeSciences DER Automation Tool")
    st.divider()

    if app_choice == "DER JSON Creator":
        run_json_ui()
    else:
        run_zip_compiler()

def run_json_ui():
    st.header("📄 DER JSON Creator")
    uploaded_files = st.file_uploader("Upload SQL files", type=["sql"], accept_multiple_files=True)
    if uploaded_files:
        final_json = proc.create_final_json(uploaded_files)
        st.json(final_json)
        st.download_button("⬇️ Download JSON", json.dumps(final_json, indent=4), "DER_JSON_FINAL.json")

def run_zip_compiler():
    st.header("📦 DER ZIP Data Compiler")
    feature = st.selectbox("Select Compiler Feature", [
        "1. Basic Output Compiler",
        "2. Email + Telephone Format",
        "3. Provider Type + Email + Contact",
        "4. Payer + Plan Format",
        "5. Age Format Compiler"
    ])

    # User Guidance
    if feature == "1. Basic Output Compiler":
        st.info("**Example:** Use when CSV1 has `total_attributed_lives, customer` and CSV2 has `den_shingles_50_59, customer`.")
    elif feature == "2. Email + Telephone Format":
        st.info("**Example:** Use when CSV has columns like `count_men_b_actual_patients_with_email`.")
    elif feature == "3. Provider Type + Email + Contact":
        st.info("**Example:** Use when CSV has `provider_type, metric, value, customer`.")
    elif feature == "4. Payer + Plan Format":
        st.info("**Example:** Use when CSV has `prid, prnm, plid, plnm, customer`.")
    elif feature == "5. Age Format Compiler":
        st.info("**Example:** Use when CSV has `current_age, eligible_patient_count, customer`.")

    uploaded_files = st.file_uploader("Upload CSV files", type=["csv"], accept_multiple_files=True)

    if uploaded_files and st.button("Generate Output"):
        with st.spinner("Processing..."):
            final_df = pd.DataFrame()
            
            if feature == "1. Basic Output Compiler":
                dfs = [pd.read_csv(f) for f in uploaded_files]
                final_df = dfs[0]
                for d in dfs[1:]:
                    final_df = pd.merge(final_df, d, on="customer", how="outer")
                final_df = proc.add_health_system_mapping(final_df, MAPPING)

            elif feature == "2. Email + Telephone Format":
                combined = pd.concat([pd.read_csv(f) for f in uploaded_files], axis=0).fillna(0)
                final_df = proc.compile_contact_validity(combined)
                final_df = proc.add_health_system_mapping(final_df, MAPPING)

            elif feature == "3. Provider Type + Email + Contact":
                raw_df = pd.concat([pd.read_csv(f) for f in uploaded_files], axis=0)
                final_df = proc.process_provider_long_format(raw_df)
                final_df = proc.add_health_system_mapping(final_df, MAPPING)

            elif feature == "4. Payer + Plan Format":
                dfs = [pd.read_csv(f) for f in uploaded_files]
                final_df = dfs[0]
                for d in dfs[1:]:
                    common = [c for c in ["prid", "prnm", "plid", "plnm", "customer"] if c in final_df.columns and c in d.columns]
                    final_df = pd.merge(final_df, d, on=common, how="outer")
                final_df = final_df.rename(columns={"prid":"Payer ID", "prnm":"Payer Name", "plid":"Plan ID", "plnm":"Plan Name"})

            elif feature == "5. Age Format Compiler":
                final_df = proc.process_age_format(uploaded_files)
                final_df = proc.add_health_system_mapping(final_df, MAPPING)

            if not final_df.empty:
                # Universal Column Ordering
                id_cols = ["customer", "Health System Name", "Category", "Provider Type", "Payer ID", "Payer Name", "Plan ID", "Plan Name"]
                existing_ids = [c for c in id_cols if c in final_df.columns]
                metric_cols = sorted([c for c in final_df.columns if c not in existing_ids], key=proc.get_vaccine_sort_key)
                final_df = final_df[existing_ids + metric_cols]
                
                st.dataframe(final_df, use_container_width=True)
                st.download_button("⬇️ Download Result", final_df.to_csv(index=False), "der_output.csv")
            else:
                st.error("No data processed. Please check file formats.")

if __name__ == "__main__":
    main()
