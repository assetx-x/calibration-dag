import json
import os
from datetime import datetime
from pprint import pprint

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage

from plugins.DataCalculations.strategies.main import PerformanceCalculator


GS_BUCKET_NAME = Variable.get('GS_BUCKET_NAME')
API_ENDPOINT = Variable.get('API_ENDPOINT')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'calculate_strategy_performance_dag',
    default_args=default_args,
    description='List Google Cloud Storage files',
    schedule_interval='@daily',
    catchup=False,
)




def authenticate():
    print('Authenticating...')
    print(f'[*] Endpoint: {API_ENDPOINT}')
    response = requests.post(
        f'{API_ENDPOINT}/auth/token/',
        json={'username': 'gcloud', 'password': 'gcloud'},
        headers={'Content-Type': 'application/json'},
    )
    jwt_token = response.json()['access']
    print(f'[*] Response: {response} - {response.text} - {response.status_code}')
    print(f'[*] JWT Token: {jwt_token}')
    return jwt_token


def recursive_str(data):
    """Function to recursively convert all values in a structure to string"""
    if isinstance(data, dict):
        return {key: recursive_str(value) for key, value in data.items()}
    if isinstance(data, list):
        return [recursive_str(element) for element in data]
    return str(data)


# def process_blob(data):
#     s_performance = PerformanceCalculator(data['file_name'], data['file_content']).run()
#     parsed_s_performance = recursive_str(s_performance)
#     token = authenticate()
#     response = requests.post(
#         f'{API_ENDPOINT}/strategy_performance/',
#         json={
#             'strategy': data['file_name'],
#             'data': parsed_s_performance
#         },
#         headers={
#             'X-User-Agent': f'airflow-task-{data["file_name"]}',
#             'Content-Type': 'application/json',
#             'accept': 'application/json',
#             'Authorization': f'Bearer {token}',
#         },
#     )
#     print(f'[*] API POST response: {response.content}')


def list_files_in_bucket(bucket_name, **kwargs):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()

    print(f"Files in bucket '{bucket_name}':")
    for blob in blobs:
        file_content = blob.download_as_string()
        blob_name = blob.name
        print(f' name: {blob_name}')
        data = {
            'file_name': blob.name,
            'file_content': file_content,
        }
        print(f'[*] Calculating performance for {data["file_name"]}')
        s_performance = PerformanceCalculator(data['file_name'], data['file_content']).run()
        parsed_s_performance = recursive_str(s_performance)
        token = authenticate()
        response = requests.post(
            f'{API_ENDPOINT}/strategy_performance/',
            json={
                'strategy': data['file_name'],
                'data': parsed_s_performance
            },
            headers={
                'X-User-Agent': f'airflow-task-{data["file_name"]}',
                'Content-Type': 'application/json',
                'accept': 'application/json',
                'Authorization': f'Bearer {token}',
            },
        )
        print(f'[*] API POST response: {response.content}')


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
