from datetime import datetime

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.context import Context
from google.cloud import storage

from plugins.DataCalculations.strategies.main import PerformanceCalculator

GS_BUCKET_NAME = Variable.get('GS_BUCKET_NAME', 'api_v2_storage')
API_ENDPOINT = Variable.get(
    'API_ENDPOINT', 'https://ax-api-v2-2dywgqiasq-uk.a.run.app/'
)

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
    """
    Authenticate to the specified API endpoint.

    This method sends a POST request to the authentication endpoint with the provided username and password to obtain a JWT token.

    Parameters:
    None

    Returns:
    str: The JWT token obtained from the authentication response.

    Example Usage:
    jwt_token = authenticate()
    """
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
    """
    Transforms the dictionary into a strings values

    :param data: The data to be converted to a recursive string representation
    :type data: dict, list, or any other data type

    :return: The data converted to a recursive string representation
    :rtype: dict, list, or str

    """
    if isinstance(data, dict):
        return {key: recursive_str(value) for key, value in data.items()}
    if isinstance(data, list):
        return [recursive_str(element) for element in data]
    return str(data)


def post_data_recursively(f_name, to_iterate_dict, t):
    """
    Recursively post data to an API endpoint with given parameters.

    Parameters:
    - f_name (str): The name of the strategy.
    - to_iterate_dict (dict): The dictionary containing the data to be posted.
    - t (str): The authorization token.

    Returns:
    None

    Example usage:
    post_data_recursively("my_strategy", {"key1": "value", "key2": "value2"}, "my_token")
    """
    for key, value in to_iterate_dict.items():
        if isinstance(value, dict):
            post_data_recursively(f_name, value, t)
        else:
            response = requests.post(
                f'{API_ENDPOINT}/strategy_performance/',
                json={'strategy': f_name, 'data': {key: value}},
                headers={
                    'X-User-Agent': f'airflow-task-{f_name}',
                    'Content-Type': 'application/json',
                    'accept': 'application/json',
                    'Authorization': f'Bearer {t}',
                },
            )
            print(f'[*] API POST response: {response.content}')
            print(f' calculator {key}')


def list_files_in_bucket(dag_context: Context, *args, **kwargs):
    """
    Open the method documentation for list_files_in_bucket.

    Parameters:
    - bucket_name (str): The name of the bucket to list files from.
    - **kwargs: Additional keyword arguments that may be passed.

    Returns:
    None

    Description:
    This method retrieves a list of files in the specified bucket. It prints the names of the files and then performs performance calculations on each file. Finally, it sends the calculated
    * performance data to a server using an authentication token.

    Note:
    - The method assumes that the necessary dependencies are already installed and imported.
    - The method requires valid credentials and permissions to access the selected bucket and perform the necessary operations.

    Example usage:
    list_files_in_bucket("my-bucket")
    """
    client = storage.Client()
    bucket = client.get_bucket(GS_BUCKET_NAME)
    blobs = bucket.list_blobs()

    print(f"Files in bucket '{GS_BUCKET_NAME}':")
    for blob in blobs:
        file_content = blob.download_as_string()
        blob_name = blob.name
        print(f' name: {blob_name}')
        data = {
            'file_name': blob.name,
            'file_content': file_content,
        }
        print(f'[*] Calculating performance for {data["file_name"]}')
        s_performance = PerformanceCalculator(
            data['file_name'], data['file_content']
        ).run()
        parsed_s_performance = recursive_str(s_performance)
        token = authenticate()

        post_data_recursively(data["file_name"], parsed_s_performance, token)


# list_files_task = PythonOperator(
#     task_id='list_files_task',
#     python_callable=list_files_in_bucket,
#     op_kwargs={'bucket_name': GS_BUCKET_NAME},
#     dag=dag,
# )
#
# empty_operator = EmptyOperator(task_id='empty_operator', dag=dag)

# empty_operator >> list_files_task

if __name__ == '__main__':
    dag.test()
