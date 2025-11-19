-- Staging model for CRM Details
-- Purpose: Clean, rename, and standardize raw CRM data for downstream use.

SELECT
    id AS crm_id,
    company_name,
    region AS crm_region,
    market AS crm_market,
    account_owner,
    status AS crm_status,
    notes,
    account_status,
    acv,
    tenure_months,
    last_contacted
FROM
    {{ source('bd_data', 'crm_details') }}