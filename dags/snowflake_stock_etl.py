"""
Airflow DAG for Snowflake ETL stock pipeline .
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Snowflake Connection Details
SNOWFLAKE_CONN_ID = 'snowflake_conn_2'
SOURCE_DATABASE = 'US_STOCK_DAILY'
SOURCE_SCHEMA = 'DCCM'
TARGET_DATABASE = 'AIRFLOW0210'
TARGET_SCHEMA = 'BF_DEV'
SNOWFLAKE_ROLE = 'BF_DEVELOPER0210'
SNOWFLAKE_WAREHOUSE = 'BF_ETL0210'

DAG_ID = "snowflake_stock_etl"

# SQL Commands
CREATE_DIM_COMPANY_PROFILE = f"""
CREATE OR REPLACE TABLE {TARGET_DATABASE}.{TARGET_SCHEMA}.dim_Company_Profile_team2 (
    SYMBOL VARCHAR(16) PRIMARY KEY,
    COMPANYNAME VARCHAR(512),
    EXCHANGE VARCHAR(64),
    INDUSTRY VARCHAR(64),
    SECTOR VARCHAR(64),
    CEO VARCHAR(64),
    WEBSITE VARCHAR(64),
    DESCRIPTION VARCHAR(2048)
);
"""

CREATE_FACT_STOCK_HISTORY = f"""
CREATE OR REPLACE TABLE {TARGET_DATABASE}.{TARGET_SCHEMA}.fact_Stock_History_team2 (
    HISTORY_ID INT AUTOINCREMENT PRIMARY KEY,
    SYMBOL VARCHAR(16) NOT NULL,
    TRADE_DATE DATE NOT NULL,
    OPEN NUMBER(18,8),
    HIGH NUMBER(18,8),
    LOW NUMBER(18,8),
    CLOSE NUMBER(18,8),
    VOLUME NUMBER(38,8),
    ADJCLOSE NUMBER(18,8),
    FOREIGN KEY (SYMBOL) REFERENCES {TARGET_DATABASE}.{TARGET_SCHEMA}.dim_Company_Profile_team2(SYMBOL)
);
"""

INSERT_DIM_COMPANY_PROFILE = f"""
INSERT INTO {TARGET_DATABASE}.{TARGET_SCHEMA}.dim_Company_Profile_team2
SELECT DISTINCT SYMBOL, COMPANYNAME, EXCHANGE, INDUSTRY, SECTOR, CEO, WEBSITE, DESCRIPTION
FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.Company_Profile;
"""

INSERT_FACT_STOCK_HISTORY = f"""
INSERT INTO {TARGET_DATABASE}.{TARGET_SCHEMA}.fact_Stock_History_team2
(SYMBOL, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE)
SELECT SYMBOL, DATE AS TRADE_DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE
FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.Stock_History;
"""

MERGE_FACT_STOCK_HISTORY = f"""
MERGE INTO {TARGET_DATABASE}.{TARGET_SCHEMA}.fact_Stock_History_team2 AS target
USING (
    SELECT SYMBOL, DATE AS TRADE_DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE
    FROM (
        SELECT SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE,
               ROW_NUMBER() OVER (PARTITION BY SYMBOL, DATE ORDER BY DATE DESC) AS rn
        FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.Stock_History
    ) AS filtered
    WHERE rn = 1  -- Only keep the first row per SYMBOL, TRADE_DATE
) AS source
ON target.SYMBOL = source.SYMBOL AND target.TRADE_DATE = source.TRADE_DATE
WHEN MATCHED THEN 
    UPDATE SET 
        target.OPEN = source.OPEN,
        target.HIGH = source.HIGH,
        target.LOW = source.LOW,
        target.CLOSE = source.CLOSE,
        target.VOLUME = source.VOLUME,
        target.ADJCLOSE = source.ADJCLOSE
WHEN NOT MATCHED THEN 
    INSERT (SYMBOL, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE)
    VALUES (source.SYMBOL, source.TRADE_DATE, source.OPEN, source.HIGH, 
            source.LOW, source.CLOSE, source.VOLUME, source.ADJCLOSE);
"""

# Airflow DAG Definition
with DAG(
    DAG_ID,
    start_date=datetime(2024, 3, 12),
    schedule_interval=None,  # Runs daily at 2 AM
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['snowflake', 'stock_data'],
    catchup=False,
) as dag:
    
    # Task to create `dim_Company_Profile`
    create_dim_company_profile = SnowflakeOperator(
        task_id='create_dim_company_profile',
        sql=CREATE_DIM_COMPANY_PROFILE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=TARGET_DATABASE,
        schema=TARGET_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    # Task to create `fact_Stock_History`
    create_fact_stock_history = SnowflakeOperator(
        task_id='create_fact_stock_history',
        sql=CREATE_FACT_STOCK_HISTORY,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=TARGET_DATABASE,
        schema=TARGET_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    # Task to insert initial data for `dim_Company_Profile`
    insert_dim_company_profile = SnowflakeOperator(
        task_id='insert_dim_company_profile',
        sql=INSERT_DIM_COMPANY_PROFILE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=TARGET_DATABASE,
        schema=TARGET_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    # Task to insert initial data for `fact_Stock_History`
    insert_fact_stock_history = SnowflakeOperator(
        task_id='insert_fact_stock_history',
        sql=INSERT_FACT_STOCK_HISTORY,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=TARGET_DATABASE,
        schema=TARGET_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    # Task for incremental updates
    merge_fact_stock_history = SnowflakeOperator(
        task_id='merge_fact_stock_history',
        sql=MERGE_FACT_STOCK_HISTORY,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=TARGET_DATABASE,
        schema=TARGET_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    # Define DAG dependencies
    (
        create_dim_company_profile
        >> create_fact_stock_history
        >> insert_dim_company_profile
        >> insert_fact_stock_history
        >> merge_fact_stock_history
    )