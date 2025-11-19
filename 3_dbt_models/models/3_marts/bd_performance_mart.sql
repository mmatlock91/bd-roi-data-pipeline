-- Mart Model: BD Performance Mart
-- Purpose: Aggregate data to analyze the performance of the Business Development (BD) team.
--          This model focuses on spend, account management metrics, and team performance.
--          It aggregates data to the monthly level, segmented by region, merchant category, and account owner.

SELECT
    -- Temporal Dimension:
    -- Truncate the transaction date to the first of the month to enable monthly trend analysis.
    DATE_TRUNC('month', date) AS transaction_month,

    -- Key Segmentation Dimensions:
    -- 'ownership_status' ('Managed' vs. 'Unmanaged') is the primary pivot for ROI analysis.
    ownership_status,
    transaction_region,
    merchant_category,
    account_owner, -- Included to allow drill-down into individual rep performance.

    -- Financial Metrics (KPIs):
    -- Total spend is the headline metric for both opportunity (Unmanaged) and revenue (Managed).
    SUM(spend) AS total_spend,

    -- Account Counts:
    -- Count distinct CRM IDs to track the number of managed relationships.
    COUNT(DISTINCT crm_id) AS unique_managed_merchants,
    
    -- Count distinct merchant names for unmanaged accounts (since they lack a CRM ID).
    -- This serves as a proxy for the volume of unmanaged leads.
    COUNT(DISTINCT CASE WHEN ownership_status = 'Unmanaged' THEN merchant_name END) AS unique_unmanaged_merchants,

    -- CRM & Retention Metrics (Scoped to Managed Accounts):
    -- Average Annual Contract Value (ACV) helps measure the "quality" of managed accounts.
    AVG(CASE WHEN ownership_status = 'Managed' THEN acv END) AS avg_acv_managed,
    
    -- Average Tenure helps measure retention and account stability.
    AVG(CASE WHEN ownership_status = 'Managed' THEN tenure_months END) AS avg_tenure_months_managed,
    
    -- Total ACV Sum allows for "Book of Business" value analysis per owner.
    SUM(CASE WHEN ownership_status = 'Managed' THEN acv END) AS total_acv_managed

FROM
    -- Source: The intermediate table that joins transactions with CRM data.
    {{ ref('enriched_transactions') }}

GROUP BY
    1, 2, 3, 4, 5 -- Group by Month, Status, Region, Category, and Owner.

ORDER BY
    1, 2, 3, 4, 5 -- Order ensures consistent output for reporting tools.