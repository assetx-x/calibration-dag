from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
)
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # Set the start date as per your requirement
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bigquery_data_transfer',
    default_args=default_args,
    schedule_interval=timedelta(
        days=1
    ),  # Set the schedule interval as per your requirement
)

extract_data_task = BigQueryGetDataOperator(
    task_id='extract_data_task',
    dataset_id='dcm-prod-ba2f',
    table_id='dcm-prod-ba2f.marketdata.daily_equity_prices',
    gcp_conn_id='google-cloud-conn',
    dag=dag,
)

insert_data_task = BigQueryInsertJobOperator(
    task_id='insert_data_task',
    configuration={
        'query': {
            'query': f'SELECT * FROM dcm-prod-ba2f.marketdata.daily_equity_prices',
            'destination_table_name': 'ax-prod-393101.marketdata.daily_equity_prices',
            'write_disposition': 'WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        }
    },
    location='US',
    dag=dag,
)

extract_data_task >> insert_data_task
