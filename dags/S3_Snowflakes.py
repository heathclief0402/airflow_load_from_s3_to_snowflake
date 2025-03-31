from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from datetime import datetime, timedelta

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER'

default_args = {
    'owner': 'airflow',
    'snowflake_conn_id': SNOWFLAKE_CONN_ID,
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 12),  
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    "s3_to_snowflake_etl",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag: 

    load_data_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id = 'S3_load_to_snowflakes',
        #files = ['sale_data_team2_2025-03-12.csv', 'sale_data_team2_2025-03-13.csv', 'sale_data_team2_2025-03-14.csv'],
        pattern='.*sale_data_team2_2025-03-(12|13|14).csv',
        table = 'PRESTAGE_SALE_DATA_TEAM2', 
        stage=SNOWFLAKE_STAGE,
        file_format="(TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY='\"', SKIP_HEADER=1, NULL_IF=('NULL', 'null', ''))",
    )
    load_data_to_snowflake



    
    # check if the file in s3 bucket , cannot do it since the key are nit provided 
    """
        check_s3_file = (
        task_id='check_s3_file',
        bucket_name='febde2025',
        bucket_key=[
        'aiflow_project/sale_data_team2_2025-03-12.csv ',
        'aiflow_project/sale_data_team2_2025-03-13.csv ',
        'aiflow_project/sale_data_team2_2025-03-14.csv '
        ],
        aws_conn_id='aws_default',
        timeout=600,
        poke_interval=60,
        mode='poke',  # Ensures it waits until all files are found
        dag=dag
        )
    
    """

    
    
