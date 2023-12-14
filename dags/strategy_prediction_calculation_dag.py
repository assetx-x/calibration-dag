import json
import os
from datetime import datetime
from pprint import pprint

import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
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
    schedule='@hourly',
    catchup=False,
)


API_ENDPOINT = 'https://ax-api-v2-2dywgqiasq-uk.a.run.app/'


def authenticate():
    print('Authenticating...')
    print(f'[*] Endpoint: {API_ENDPOINT}')
    response = requests.post(
        f'{API_ENDPOINT}/auth/token/',
        json={'username': 'string', 'password': 'string'},
        headers={'Content-Type': 'application/json'},
    )
    jwt_token = response.json()['access']
    print(f'[*] Response: {response} - {response.text} - {response.status_code}')
    return jwt_token


def list_files_in_bucket(bucket_name, **kwargs):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()

    print("Files in bucket '{}':".format(bucket_name))
    for blob in blobs:
        file_content = blob.download_as_string()
        data = {
            'file_name': blob.name,
            'file_content': file_content,
        }
        print(f'[*] Calculating performance for {data["file_name"]}')
        s_performance = PerformanceCalculator(data['file_name'], data['file_content']).run()
        print(f'[*] Result: {s_performance}')
        token = authenticate()
        s_performance_json = json.dumps({'data': s_performance})
        response = requests.post(
            f'{API_ENDPOINT}/strategy_performance/',
            json=s_performance_json,
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {token}',
            },
        )
        print(f'[*] Response: {response} - {response.text} - {response.status_code}')
        return response


list_files_task = PythonOperator(
    task_id='list_files_task',
    python_callable=list_files_in_bucket,
    op_kwargs={'bucket_name': GS_BUCKET_NAME},
    dag=dag,
)

empty_operator = EmptyOperator(task_id='empty_operator', dag=dag)

empty_operator >> list_files_task


if __name__ == '__main__':
    dag.run()
