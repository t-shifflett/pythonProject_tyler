#!/usr/bin/env bash

cd /app1/PROD-env/IEX_PYTHON/

source /app1/PROD-env/bin/activate

python3 /app1/PROD-env/IEX_PYTHON/IEX_ETL_SERFF_DATA_BENEFITS_BENEFITS_PACKAGE_BENEFIT_INFORMATION.py
python3 /app1/PROD-env/IEX_PYTHON/IEX_ETL_SERFF_DATA_BENEFITS_BENEFITS_PACKAGE_PLAN_IDENTIFIERS.py
python3 /app1/PROD-env/IEX_PYTHON/IEX_ETL_SERFF_DATA_BENEFITS_COST_SHARE_VARIANCES.py
python3 /app1/PROD-env/IEX_PYTHON/IEX_ETL_SERFF_DATA_NETWORK_ID.py
python3 /app1/PROD-env/IEX_PYTHON/IEX_ETL_SERFF_DATA_PRESCRIPTION_DRUG_DRUG_LISTS.py
python3 /app1/PROD-env/IEX_PYTHON/IEX_ETL_SERFF_DATA_PRESCRIPTION_DRUG_FORMULARY_TIERS.py
python3 /app1/PROD-env/IEX_PYTHON/IEX_ETL_SERFF_DATA_RATING_RULES_BUSINESS_RULES.py
python3 /app1/PROD-env/IEX_PYTHON/IEX_ETL_SERFF_DATA_RATING_TABLE_RATE_TABLE.py
python3 /app1/PROD-env/IEX_PYTHON/IEX_ETL_SERFF_DATA_SERVICE_AREA_SERVICE_AREAS.py