from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

SNOWFLAKE_CONN_ID = 'snowflake_conn'  # Ensure this matches your Airflow connection

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 12),
    'catchup': False
}

# Create DAG
with DAG(
    dag_id='test_snowflake_connection',
    default_args=default_args,
    schedule_interval=None  # Run manually
) as dag:

    # Snowflake Test Query
    test_snowflake = SnowflakeOperator(
        task_id='test_snowflake_query',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="SELECT CURRENT_VERSION();"
    )

    test_snowflake