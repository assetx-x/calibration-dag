import os
from datetime import datetime
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor, GCSUploadSessionCompleteSensor
from google.cloud import storage

from plugins.DataCalculations.strategies.main import PerformanceCalculator

GS_BUCKET_NAME = 'api_v2_storage'
DEFAULT_FILE_PREFIX = 'csv'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'sensor_strategy_dag',
    default_args=default_args,
    description='Calculate strategy performance when a new file is uploaded',
    schedule_interval=None,  # Set to None to trigger manually or through external events
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


def execute(bucket_name, **kwargs):
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
        token = authenticate()
        response = requests.post(
            f'{API_ENDPOINT}/strategy_performance/',
            json={
                'name': data['file_name'],
                'performance': s_performance,
            },
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {token}',
            },
        )
        print(f'[*] Response: {response} - {response.text} - {response.status_code}')
        return response


def check_for_files(**kwargs):
    client = storage.Client()
    bucket = client.get_bucket(GS_BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=DEFAULT_FILE_PREFIX)

    return len(list(blobs)) > 0


list_files_sensor = GCSUploadSessionCompleteSensor(
    bucket=GS_BUCKET_NAME,
    prefix=DEFAULT_FILE_PREFIX,
    inactivity_period=2,
    min_objects=1,
    allow_delete=False,
    previous_objects=set(),
    task_id="execute",
)

list_files_sensor

# check_for_files_task = PythonOperator(
#     task_id='check_for_files_task',
#     python_callable=check_for_files,
#     provide_context=True,
#     dag=dag,
# )
#
# list_files_task = PythonOperator(
#     task_id='list_files_task',
#     python_callable=execute,
#     op_kwargs={'bucket_name': GS_BUCKET_NAME},
#     dag=dag,
# )
#
# # Additional PythonOperator for calculation task
# calculate_task = PythonOperator(
#     task_id='calculate_task',
#     python_callable=execute,  # Replace this with the calculation function
#     op_kwargs={'bucket_name': GS_BUCKET_NAME},
#     dag=dag,
# )
#
# list_files_sensor >> check_for_files_task >> list_files_task >> calculate_task

if __name__ == '__main__':
    dag.run()
