MAPPING = {
    'advantasure-prod': 'Advantasure (Env 1)', 'advantasureapollo-prod': 'Advantasure (Env 2)',
    'adventist-prod': 'Adventist Healthcare', 'alameda-prod': 'Alameda County',
    'alo-prod': 'Alo solutions', 'arkansashealth-prod': 'Chi St Vincent',
    'ascension-preprod': 'Ascension Health (Env 1)', 'ascension-prod': 'Ascension Health (Env 2)',
    'atlantichealth-prod': 'Atlantic Health', 'bannerapollo-prod': 'Banner Health',
    'bhsf-prod': 'Baptist Health South Florida', 'bsim-prod': 'BSIM Healthcare services',
    'careabout-prod': 'CareAbout Health', 'ccmcn-prod': 'Colorado Community Managed Care Network',
    'ccnc-prod': 'Community Care of North Carolina', 'chessapollo-prod': 'CHESS Health Solutions',
    'childrenshealthapollo-prod': 'Children Health Alliance', 'christianacare-prod': 'Christiana Care Health System',
    'cmhcapollo-prod': 'Central Maine Healthcare', 'cmicsapollo-prod': 'Childrens Mercy Hospital And Clinics',
    'coa-prod': 'Colorado Access', 'concare-prod': 'ConcertoCare',
    'connecticutchildrens-prod': "Connecticut Children's Health", 'cshnational-new-prod': 'CSH National (Env 1)',
    'cshnational-prod': 'CSH National (Env 2)', 'curana-prod': 'Curana Health',
    'dhmsapollo-prod': 'Dignity Health', 'dock-cmicsapollo-prod': 'Childrens Mercy Hospital And Clinics (Nexus)',
    'dock-embrightapollo-prod': 'Embright (Nexus)', 'dock-nemoursapollo-prod': 'Nemours Childrens Health System (Nexus)',
    'dock-risehealth-prod': 'Rise Health (Nexus)', 'dock-tccn-prod': 'Childrens Healthcare Of Atlanta (Nexus)',
    'embrightapollo-prod': 'Embright', 'evergreen-prod': 'Evergreen Nephrology',
    'falliance-prod': 'Franciscan Health', 'flmedicaid-prod': 'Florida Medicaid',
    'franciscan-staging': 'Franciscan Health (staging)', 'govcloud-prod': 'govcloud-prod',
    'gravitydemo-prod': 'Gravity', 'impacthealth-prod': 'Impact Primary Care Network/Impact Health',
    'innohumana-prod': 'Longevity Health Plan(LHP) - HUMANA', 'innolhp-prod': 'Longevity Health Plan(LHP) - Core)',
    'innovaetna-prod': 'Longevity Health Plan(LHP) - Aetna', 'integration-preprod': 'internal env',
    'integris-prod': 'integris-prod', 'intjuly-prod': 'internal account',
    'longitudegvt-prod': 'LongitudeRx (Env 1)', 'longituderx-prod': 'LongitudeRx (Env 2)',
    'mcs-prod': 'Medical Card System', 'mercyoneapollo-prod': 'MercyOne',
    'mercypit-prod': 'Pittsburgh Mercy', 'mgm-prod': 'Mgm Resorts',
    'mhcn-prod': 'Chi Memorial', 'nemoursapollo-prod': 'Nemours Childrens Health System',
    'novanthealth-prod':'Novant Health', 'nwm-prod': 'Northwestern Medicine',
    'orlandoapollo-prod': 'Orlando Health', 'pedassoc-prod': 'Pediatric Associates',
    'phlc-prod': 'Population Health Learning Center', 'php-prod': 'P3 Health Partners',
    'pophealthcare-prod': 'Emcara / POP Health / Guidewell Mutual Holding Company',
    'prismah-prod': 'Prisma Health', 'pswapollo-prod': 'Physicians Of Southwest Washington',
    'risehealth-prod': 'Rise Health', 'sacramento-prod': 'Sacramento SHIE (Env 1)',
    'sacramentoshie-prod': 'Sacramento SHIE (Env 2)', 'sentara-prod': 'Sentara Health',
    'smch-prod': 'San Mateo County Health ', 'stewardapollo-prod': 'Steward Health Care System',
    'strivehealth-prod': 'Strive Health', 'tccn-prod': 'Childrens Healthcare Of Atlanta',
    'thnapollo-prod': 'Cone Health', 'trinity-prod': 'Trinity Health National',
    'uninet-prod': 'CHI Health Partners', 'usrc-prod': 'US RenalCare',
    'walgreens-prod': 'Walgreens', 'champion-prod' : 'Champion Health Plan',
    'mdxhawaii-prod' : 'MDX Hawaii', 'recuro-prod' : 'Recuro Health',
    'mhpartner-prod' : 'Mission Health Partners', 'dock-olyortho-prod' : 'Oly Ortho (Nexus)'
}


CATEGORY_ORDER = ["Total", "With Telephone", "With Email", "With only Telephone", "With only Email", "With Both Telephone and Email", "None"]

CATEGORY_CONFIG = {
    "Total": "_patients_total", 
    "With Telephone": "_patients_with_contact_number", 
    "With Email": "_patients_with_email", 
    "With only Telephone": "_patients_with_only_contact", 
    "With only Email": "_patients_with_only_email", 
    "With Both Telephone and Email": "_patients_with_both_contact_and_email", 
    "None": "_patients_with_neither_contact_nor_email"
}

VACCINE_ORDER_PATTERNS = [
    "total_attributed_lives", "age_16", "age_50", "age_analysis", 
    "hpv_9_17", "shingles_50_59", "shingles_60_64", "shingles_65_plus",
    "rsv_covid_60_64", "rsv_covid_65_74", "rsv_covid_75_plus",
    "pneumococcal_50_plus", "pneumococcal_50_64", "pneumococcal_65_plus",
    "influenza_50_plus", "covid_65_plus", "rsv_60_plus",
    "men_b_acwy_abcwy_16_18", "men_acwy_abcwy_16_18", "men_b_16_18", "men_acwy_16_18",
    "men_abcwy_16_18", "paxlovid", "shingles_actual", "shingles_ls", 
    "men_acwy_actual", "men_b_actual", "men_b_ls"
]
