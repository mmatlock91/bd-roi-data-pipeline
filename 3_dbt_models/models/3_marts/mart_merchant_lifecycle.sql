-- Mart Model: Merchant Lifecycle
-- Purpose: Create a merchant-level view to track the lifecycle status (Active, At Risk, Churned)
--          of every merchant. This drives the "Lead Generation" dashboard by identifying
--          high-value unmanaged targets and churn risks.

WITH source_data AS (
    -- Step 1: Select relevant columns from the enriched transactions table.
    SELECT
        merchant_domain,
        merchant_category,
        merchant_name, -- Critical for the sales team to know WHO to call.
        date,
        spend,
        account_owner,
        ownership_status
    FROM
        {{ ref('enriched_transactions') }}
    WHERE
        merchant_domain IS NOT NULL -- Exclude rows where we can't identify the merchant entity.
),

merchant_summary AS (
    -- Step 2: Aggregate transactions to the merchant level.
    -- We group by both Domain and Name to ensure uniqueness and readability.
    SELECT
        merchant_domain,
        merchant_name,
        
        -- Use the most recent category (simplification for reporting).
        MAX(merchant_category) AS merchant_category,
        
        -- Lifecycle Dates: Determine the very first and very last time this merchant transacted.
        MIN(date) AS first_transaction_date,
        MAX(date) AS last_transaction_date,
        
        -- Lifetime Value (LTV): Total spend across all history.
        SUM(spend) AS total_lifetime_spend,
        COUNT(*) AS total_transactions,
        
        -- Account Details:
        -- If *any* transaction had an owner, we treat the merchant as 'Managed' here for safety.
        MAX(account_owner) AS account_owner,
        CASE
            WHEN MAX(account_owner) IS NOT NULL THEN 'Managed'
            ELSE 'Unmanaged'
        END AS ownership_status
    FROM source_data
    GROUP BY 1, 2 -- Grouping by merchant_domain (1) and merchant_name (2)
),

final_mart AS (
    -- Step 3: Calculate Recency.
    -- Determine how many days have passed since the merchant's last transaction.
    SELECT
        *,
        DATEDIFF(day, last_transaction_date, CURRENT_DATE()) AS days_since_last_transaction
    FROM merchant_summary
)

-- Step 4: Define Lifecycle Status based on Recency logic.
SELECT
    *,
    CASE
        -- "Active": Transacted in the last 30 days.
        WHEN days_since_last_transaction <= 30 THEN 'Active'
        -- "At Risk": Transacted 31-90 days ago. These are prime candidates for re-engagement campaigns.
        WHEN days_since_last_transaction <= 90 THEN 'At Risk'
        -- "Churned": No transaction in over 90 days.
        ELSE 'Churned'
    END AS merchant_status
FROM final_mart
ORDER BY
    total_lifetime_spend DESC -- Prioritize high-value merchants at the top of the list.