import pendulum
from airflow.decorators import dag
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator

# --- Configuration ---
# We keep these configuration variables at the top so it is easy to switch
# between development, staging, and production environments without changing the code.
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_DATABASE = "DATA_PROJECT"
SNOWFLAKE_SCHEMA = "PUBLIC"

SOURCE_TABLE = "UNCLEANED_CRM_DETAILS"
FINAL_TABLE = "CRM_DETAILS"
STAGE_TABLE = "STG_COMPANY_DATA"

# This list acts as the source of truth for our schema.
# Defining it here ensures that our Python logic and the SQL we generate
# stay in sync if we add or remove columns later.
COLUMNS_TO_KEEP = [
    "company_name",
    "region",
    "market",
    "account_owner",
    "status",
    "notes",
    "account_status",
    "acv",
    "tenure_months",
    "last_contacted"
]

# --- Dynamic SQL Generation ---

# Here we dynamically build the SQL logic for our "Completeness Score."
# This loops through every column and creates a sum. If a column has data,
# it adds 1 to the score. We use this later to ensure we keep the record
# with the most data.
COMPLETENESS_SCORE_SQL = " + ".join(
    [f"(CASE WHEN {col} IS NOT NULL THEN 1 ELSE 0 END)" for col in COLUMNS_TO_KEEP]
)

# This formats the list of columns so they fit cleanly into our final SELECT statement.
FINAL_COLUMNS_SQL = ",\n                ".join(COLUMNS_TO_KEEP)

# --- SQL Queries ---

# This query builds the temporary staging table.
# 1. It calculates the completeness score we defined above.
# 2. It uses a window function to rank duplicates. We partition by company name
#    and keep the one with the highest score.
# 3. It generates a secure, unique ID (SHA2 hash) for each company.
SQL_CREATE_STAGE_TABLE = f"""
CREATE OR REPLACE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{STAGE_TABLE} AS
WITH completeness_ranked AS (
    SELECT
        *,
        (
            {COMPLETENESS_SCORE_SQL}
        ) AS completeness_score,
        ROW_NUMBER() OVER(
            PARTITION BY company_name
            ORDER BY completeness_score DESC
        ) as rn
    FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SOURCE_TABLE}
)
SELECT
    SUBSTR(SHA2(CONCAT_WS('', company_name, market), 256), 1, 16) AS id,
    {FINAL_COLUMNS_SQL}
FROM completeness_ranked
WHERE rn = 1;
"""

# This performs a "Blue/Green" style deployment for the data.
# Instead of deleting data from the live table, we build the new table properly
# and then use Snowflake's SWAP command. This switches the tables instantly,
# so users never experience downtime or see incomplete data.
SQL_LOAD_FINAL_TABLE = f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{FINAL_TABLE}
LIKE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{STAGE_TABLE};

ALTER TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{FINAL_TABLE}
SWAP WITH {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{STAGE_TABLE};
"""

# Once the swap is done, the old data is now in the staging table.
# We drop it to keep our environment clean.
SQL_CLEANUP_STAGE_TABLE = f"""
DROP TABLE IF EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{STAGE_TABLE};
"""

@dag(
    dag_id="snowflake_crm_cleaning_pipeline",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["snowflake", "data-quality", "etl"],
)
def snowflake_crm_cleaning_dag():
    """
    This DAG handles the cleaning and deduplication of our CRM data.
    It uses an atomic swap pattern to ensure the live tables are always accessible.
    """
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    create_staging_table = SnowflakeOperator(
        task_id="create_staging_table",
        sql=SQL_CREATE_STAGE_TABLE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    load_final_table = SnowflakeOperator(
        task_id="load_final_table",
        sql=SQL_LOAD_FINAL_TABLE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    cleanup_staging_table = SnowflakeOperator(
        task_id="cleanup_staging_table",
        sql=SQL_CLEANUP_STAGE_TABLE,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # Set the task order to ensure they run sequentially.
    begin >> create_staging_table >> load_final_table >> cleanup_staging_table >> end

snowflake_crm_cleaning_dag()