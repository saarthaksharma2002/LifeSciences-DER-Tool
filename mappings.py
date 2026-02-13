# mappings.py
import streamlit as st
import json


try:
    # Access the secret as if it were an environment variable or dictionary
    mapping_json_str = st.secrets["MAPPING_JSON"]
    MAPPING = json.loads(mapping_json_str)
    APP_PASSWORD = st.secrets["APP_PASSWORD"]
except Exception as e:
    st.error("Secrets not found. Please check Streamlit Cloud Settings.")
    MAPPING = {}
    APP_PASSWORD = "fallback_password_for_local_only"



CATEGORY_CONFIG = {
    "Total": "",
    "With Contact": "_patients_with_contact_number",
    "With Email": "_patients_with_email",
    "Only Contact": "_patients_with_only_contact",
    "Only Email": "_patients_with_only_email",
    "Both Contact and Email": "_patients_with_both_contact_and_email",
    "None Available": "_patients_with_neither_contact_nor_email"
}

POWERBI_ORDER = [
    "total_attributed_lives", "den_hpv_9_17", "num_hpv_9_17", "den_shingles_50_59", 
    "num_shingles_50_59", "den_shingles_60_64", "num_shingles_60_64", 
    "den_shingles_65_plus", "num_shingles_65_plus", "den_rsv_covid_60_64", 
    "num_rsv_covid_60_64", "den_rsv_covid_65_74", "num_rsv_covid_65_74", 
    "den_rsv_covid_75_plus", "num_rsv_covid_75_plus", "den_pneumococcal_50_plus", 
    "num_pneumococcal_50_plus", "den_pneumococcal_50_64", "num_pneumococcal_50_64", 
    "den_pneumococcal_65_plus", "num_pneumococcal_65_plus", "den_influenza_50_plus", 
    "num_influenza_50_plus", "den_covid_65_plus", "num_covid_65_plus", 
    "den_rsv_60_plus", "num_rsv_60_plus", "den_meningococcal_16_18", 
    "num_men_b_acwy_abcwy_16_18", "den_meningococcal_19_23", "num_men_b_acwy_abcwy_19_23", 
    "num_men_acwy_abcwy_16_18", "num_men_acwy_abcwy_19_23", "num_men_b_16_18", 
    "num_men_b_19_23", "num_men_acwy_16_18", "num_men_acwy_19_23", 
    "num_men_abcwy_16_18", "num_men_abcwy_19_23", "a_b_den_paxlovid", 
    "b_num_paxlovid", "count_shingles_ls", "count_men_acwy_actual", 
    "count_men_b_ls", "common_patients_eligible_for_both"
]

VACCINE_ORDER_PATTERNS = [
    "total_attributed_lives", "age_16", "age_50", "hpv_9_17", "shingles_50_59", 
    "shingles_60_64", "shingles_65_plus", "rsv_covid_60_64", "rsv_covid_65_74", 
    "rsv_covid_75_plus", "pneumococcal_50_plus", "pneumococcal_50_64", 
    "pneumococcal_65_plus", "influenza_50_plus", "covid_65_plus", "rsv_60_plus",
    "men_b_acwy_abcwy_16_18", "men_acwy_abcwy_16_18", "men_b_16_18", "men_acwy_16_18",
    "men_abcwy_16_18", "paxlovid", "shingles_actual", "shingles_ls", 
    "men_acwy_actual", "men_b_actual", "men_b_ls"
]
AGE_COLUMNS = [str(i) for i in range(101)]    



