-- Mart Model: Post-Conversion Lift
-- Purpose: Analyze the financial impact of converting a merchant.
--          This model compares spend *before* the conversion date vs. *after* the conversion date
--          to calculate the "Lift" (increase in spend) driven by BD management.

WITH merchant_firsts AS (
    -- Step 1: Identify the "Conversion Date" for every merchant.
    -- This is the first date a transaction appeared with a valid account_owner (Managed status).
    SELECT
        merchant_domain,
        merchant_name,
        MIN(date) AS first_transaction_date,
        MIN(CASE
            WHEN ownership_status = 'Managed' THEN date
            ELSE NULL
            END) AS first_managed_date,
        MAX(account_owner) AS converting_account_owner
    FROM
        {{ ref('enriched_transactions') }}
    WHERE
        merchant_domain IS NOT NULL
    GROUP BY
        1, 2
),

monthly_spend AS (
    -- Step 2: Calculate total monthly spend for every merchant (regardless of status).
    SELECT
        merchant_domain,
        DATE_TRUNC('month', date) AS transaction_month,
        SUM(spend) AS total_monthly_spend
    FROM
        {{ ref('enriched_transactions') }}
    GROUP BY
        1, 2
),

joined_data AS (
    -- Step 3: Join monthly spend with conversion dates to create a timeline relative to conversion.
    SELECT
        m.merchant_domain,
        m.merchant_name,
        m.first_transaction_date,
        m.first_managed_date,
        m.converting_account_owner,
        s.transaction_month,
        s.total_monthly_spend,
        
        -- Calculate 'Months From Conversion'.
        -- Negative values = Months BEFORE conversion.
        -- Positive values = Months AFTER conversion.
        DATEDIFF('month', m.first_managed_date, s.transaction_month) AS months_from_conversion
    FROM
        monthly_spend s
    JOIN
        merchant_firsts m ON s.merchant_domain = m.merchant_domain
    WHERE
        m.first_managed_date IS NOT NULL -- Only include merchants that were eventually converted.
)

-- Step 4: Label the periods for easy analysis in Tableau.
SELECT
    merchant_domain,
    merchant_name,
    converting_account_owner,
    transaction_month,
    first_transaction_date,
    first_managed_date,
    total_monthly_spend,
    months_from_conversion,
    
    -- Create readable labels for the timeline.
    CASE
        WHEN months_from_conversion < 0 THEN 'Pre-Conversion'
        WHEN months_from_conversion = 0 THEN 'Conversion Month'
        WHEN months_from_conversion > 0 THEN 'Post-Conversion'
        ELSE 'Error'
    END AS conversion_period
FROM
    joined_data
WHERE
    -- Filter to a relevant window (e.g., +/- 12 months) to focus the analysis.
    months_from_conversion BETWEEN -12 AND 12