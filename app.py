# app.py
import streamlit as st
import pandas as pd
import processor as proc
from mappings import MAPPING, CATEGORY_ORDER

st.set_page_config(page_title="LifeSciences DER Automation Tool", layout="wide")

def main():
    st.title("🧬 LifeSciences DER Automation Tool")
    app_choice = st.sidebar.radio("Navigation", ["DER JSON Creator", "DER ZIP Data Compiler"])
    st.divider()

    if app_choice == "DER JSON Creator":
        proc.run_json_creator_ui() # Assuming logic moved to processor or kept here
    else:
        run_zip_compiler()

def run_zip_compiler():
    st.header("📦 DER ZIP Data Compiler")
    
    feature = st.selectbox("Select Compiler Feature", [
        "1. Basic Output Compiler",
        "2. Email + Telephone Format",
        "3. Provider Type + Email + Contact",
        "4. Payer + Plan Format",
        "5. Age Format Compiler"
    ])

    # Dynamic Help Text
    help_box = st.container()
    if feature == "1. Basic Output Compiler":
        help_box.info("**Use this when:** You have multiple CSVs with standard metric columns (den_..., num_...) and a customer column. \n\n **Input:** `customer`, `den_shingles_50_59` \n **Output:** `customer`, `Health System`, `Sorted Metrics...`")
    elif feature == "2. Email + Telephone Format":
        help_box.info("**Use this when:** Your CSV has columns ending in suffixes like `_patients_with_email`. It will expand these into a 'Category' row format. \n\n **Input:** `count_men_b_actual_patients_with_email`, `customer` \n **Output:** `customer`, `Category`, `count_men_b_actual`")
    elif feature == "3. Provider Type + Email + Contact":
        help_box.info("**Use this when:** You have a long-format SQL export with provider_type and metric/value columns. \n\n **Input:** `provider_type`, `metric`, `value`, `customer`")
    elif feature == "4. Payer + Plan Format":
        help_box.info("**Use this when:** Your data includes Payer (prnm) and Plan (plnm) names and IDs. \n\n **Input:** `prid`, `prnm`, `plid`, `plnm`, `metric_col`, `customer`")
    elif feature == "5. Age Format Compiler":
        help_box.info("**Use this when:** You want to see age (0, 1, 2...) as columns. \n\n **Input:** `current_age`, `eligible_patient_count`, `customer` \n **Output:** `customer`, `0`, `1`, `2`... (Age columns)")

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
                cols = ["customer", "Health System Name"] + sorted([c for c in final_df.columns if c not in ["customer", "Health System Name"]], key=proc.get_vaccine_sort_key)
                final_df = final_df[cols]

            elif feature == "2. Email + Telephone Format":
                combined = pd.concat([pd.read_csv(f) for f in uploaded_files], axis=0).fillna(0)
                final_df = proc.compile_contact_validity(combined)
                final_df = proc.add_health_system_mapping(final_df, MAPPING)
                # Sort columns
                fixed = ["customer", "Health System Name", "Category"]
                metrics = sorted([c for c in final_df.columns if c not in fixed], key=proc.get_vaccine_sort_key)
                final_df = final_df[fixed + metrics]

            elif feature == "3. Provider Type + Email + Contact":
                raw_df = pd.concat([pd.read_csv(f) for f in uploaded_files], axis=0)
                raw_df = proc.add_health_system_mapping(raw_df, MAPPING)
                final_df = proc.process_provider_long_format(raw_df)
                # Order columns
                fixed = ["customer", "Health System Name", "Category", "Provider Type"]
                metrics = sorted([c for c in final_df.columns if c not in fixed], key=proc.get_vaccine_sort_key)
                final_df = final_df[fixed + metrics]

            elif feature == "4. Payer + Plan Format":
                dfs = [pd.read_csv(f) for f in uploaded_files]
                final_df = dfs[0]
                for d in dfs[1:]:
                    common = [c for c in final_df.columns if c in d.columns and c in ["prid", "prnm", "plid", "plnm", "customer"]]
                    final_df = pd.merge(final_df, d, on=common, how="outer")
                
                # Reorder to put Payer/Plan first
                payer_cols = ["prid", "prnm", "plid", "plnm", "customer"]
                existing_payer = [c for c in payer_cols if c in final_df.columns]
                metrics = sorted([c for c in final_df.columns if c not in existing_payer], key=proc.get_vaccine_sort_key)
                final_df = final_df[existing_payer + metrics]
                final_df = final_df.rename(columns={"prid": "Payer ID", "prnm": "Payer Name", "plid": "Plan ID", "plnm": "Plan Name"})

            elif feature == "5. Age Format Compiler":
                final_df = proc.process_age_format(uploaded_files)

            st.dataframe(final_df, use_container_width=True)
            st.download_button("⬇️ Download Result", final_df.to_csv(index=False), "der_compiled_data.csv")

if __name__ == "__main__":
    main()
