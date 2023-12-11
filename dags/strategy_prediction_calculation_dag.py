from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

from plugins.DataCalculations.strategies.main import PerformanceCalculator

GS_BUCKET_NAME = 'api_v2_storage'


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'calculate_strategy_performance_dag',
    default_args=default_args,
    description='List Google Cloud Storage files',
    schedule_interval='@once',
    catchup=False,
)


def list_files_in_bucket(bucket_name, **kwargs):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()

    print("Files in bucket '{}':".format(bucket_name))
    for blob in blobs:
        print(blob.name)
        file_content = blob.download_as_string()
        data = {
            'file_name': blob.name,
            'file_content': file_content,
        }
        print(f'[*] Calculating performance for {data["file_name"]}')
        s_performance = PerformanceCalculator(data['file_name'], data['file_content']).run()
        print(f'[*] Result: {s_performance}')
        endpoint = 'http://localhost:8000/api/v2/strategies/'
        print(f'[*] Endpoint: {endpoint}')
        response = requests.post(
            endpoint,
            data={'name': data['file_name'], 'performance': s_performance},
            headers={'Content-Type': 'application/json'},
        )
        print(f'[*] Response: {response} - {response.text} - {response.status_code}')
        return response


list_files_task = PythonOperator(
    task_id='list_files_task',
    python_callable=list_files_in_bucket,
    op_kwargs={'bucket_name': GS_BUCKET_NAME},
    provide_context=True,
    dag=dag,
)

list_files_task
