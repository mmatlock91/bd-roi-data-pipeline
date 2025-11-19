-- Staging model for Transactions
-- Purpose: Clean, type-cast, and standardize raw transaction data.

SELECT
    channel,
    date,
    merchant_category,
    merchant_subcategory,
    merchant_name,
    merchant_domain,
    currency,
    spend,
    market AS transaction_market,
    region AS transaction_region,
    crm_id
FROM
    {{ source('bd_data', 'transactions_matched') }}