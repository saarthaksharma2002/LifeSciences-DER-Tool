# mappings.py
import os
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get the JSON string from .env and parse it
mapping_json_str = os.getenv("MAPPING_JSON", "{}")
MAPPING = json.loads(mapping_json_str)

CATEGORY_CONFIG = {
    "Total": "",
    "With Contact": "_patients_with_contact_number",
    "With Email": "_patients_with_email",
    "Only Contact": "_patients_with_only_contact",
    "Only Email": "_patients_with_only_email",
    "Both Contact and Email": "_patients_with_both_contact_and_email",
    "None Available": "_patients_with_neither_contact_nor_email"
}

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

