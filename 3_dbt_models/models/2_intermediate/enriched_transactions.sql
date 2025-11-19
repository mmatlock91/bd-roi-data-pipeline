-- Intermediate model for Enriched Transactions
-- Purpose: Join standardized transaction data with standardized CRM data to create a single,
--          comprehensive view of every transaction, flagged by ownership status.

SELECT
    -- Transaction Attributes (from Staging):
    t.channel,
    t.date,
    t.merchant_category,
    t.merchant_subcategory,
    t.merchant_name,
    t.merchant_domain,
    t.currency,
    t.spend,
    t.transaction_market,
    t.transaction_region,
    t.crm_id, -- The foreign key.

    -- CRM Attributes (from Staging):
    -- These columns will be NULL for unmanaged transactions (where t.crm_id is NULL).
    c.company_name,
    c.crm_region,
    c.crm_market,
    c.account_owner, -- Crucial for assigning credit to BD reps.
    c.crm_status,
    c.account_status,
    c.acv,           -- Annual Contract Value from the CRM.
    c.tenure_months,
    c.last_contacted,

    -- Ownership Logic:
    -- This is the core business logic of the pipeline.
    -- If a transaction has a valid link to the CRM (t.crm_id is not null), it is 'Managed'.
    -- Otherwise, it is 'Unmanaged' (representing the unmanaged opportunity).
    -- UPDATED: Changed 'Owned'/'Unowned' to 'Managed'/'Unmanaged' for clarity.
    CASE
        WHEN t.crm_id IS NOT NULL THEN 'Managed'
        ELSE 'Unmanaged'
    END AS ownership_status

FROM
    -- Left Source: The staging transactions table.
    {{ ref('stg_transactions') }} t

-- Join Logic:
-- A LEFT JOIN is used to preserve ALL transactions, even those without a matching CRM record.
-- This allows us to calculate the total "Unmanaged" opportunity size.
LEFT JOIN
    {{ ref('stg_crm_details') }} c
    ON t.crm_id = c.crm_id