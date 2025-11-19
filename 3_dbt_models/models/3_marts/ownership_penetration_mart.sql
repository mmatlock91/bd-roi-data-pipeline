-- Mart Model: Ownership Penetration
-- Purpose: Calculate the "Market Share" of the BD team. This pivots spend into
--          'Managed' vs. 'Unmanaged' columns to calculate a penetration rate % per month.

WITH monthly_spend AS (
    -- Step 1: Aggregate total spend by month and ownership status.
    SELECT
        DATE_TRUNC('month', date) AS transaction_month,
        merchant_category,
        transaction_region,
        ownership_status,
        SUM(spend) AS total_spend
    FROM
        {{ ref('enriched_transactions') }}
    GROUP BY
        1, 2, 3, 4
)

-- Step 2: Pivot the data and calculate the Penetration Rate.
SELECT
    transaction_month,
    merchant_category,
    transaction_region,

    -- Pivot 'Managed' spend into its own column.
    SUM(CASE 
            WHEN ownership_status = 'Managed' THEN total_spend 
            ELSE 0 
        END) AS managed_spend,
    
    -- Pivot 'Unmanaged' spend into its own column (The Opportunity Size).
    SUM(CASE 
            WHEN ownership_status = 'Unmanaged' THEN total_spend 
            ELSE 0 
        END) AS unmanaged_spend,

    -- Total Market Size (Managed + Unmanaged).
    SUM(total_spend) AS total_spend,

    -- KPI: Managed Penetration Rate
    -- Formula: Managed Spend / Total Spend
    CASE
        WHEN SUM(total_spend) > 0 THEN
            (SUM(CASE WHEN ownership_status = 'Managed' THEN total_spend ELSE 0 END) / SUM(total_spend))
        ELSE 0
    END AS managed_penetration_rate

FROM
    monthly_spend
GROUP BY
    1, 2, 3
ORDER BY
    transaction_month DESC, 
    managed_penetration_rate DESC