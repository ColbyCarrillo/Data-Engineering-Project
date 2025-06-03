import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='weather_pipeline_dag',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',  # Run daily
    catchup=False,
    tags=['noaa', 'weather'],
) as dag:

    init_schema = BashOperator(
    task_id='init_schema',
    bash_command='python /opt/airflow/init_schema.py'
    )

    download_data = BashOperator(
        task_id='download_data',
        bash_command='python /opt/airflow/ingestion/noaa_downloader.py'
    )

    parse_data = BashOperator(
        task_id='parse_data',
        bash_command='python /opt/airflow/ingestion/noaa_parser.py'
    )

    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /opt/airflow/noaa_dbt && dbt run',
        env={
            'DBT_HOST': 'my_postgres',
            'POSTGRES_USER': os.getenv('POSTGRES_USER'),
            'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD'),
            'POSTGRES_PORT': os.getenv('POSTGRES_PORT'),
            'POSTGRES_DB': os.getenv('POSTGRES_DB'),
            **os.environ,  # include existing env vars for safety
        }
    )

    init_schema >> download_data >> parse_data >> run_dbt
