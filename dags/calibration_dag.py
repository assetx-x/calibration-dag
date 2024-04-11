import logging

from dotenv import load_dotenv

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator

import pandas as pd
from datetime import timedelta
import gcsfs

import os

from parameters_file import PARAMS_DICTIONARY

load_dotenv()

# Add the path to the "plugins" folder to sys.path
# Assuming the "calibration-dag" directory is the parent directory of your DAGs folder.
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
plugins_folder = os.path.join(parent_directory, "plugins")
data_processing_folder = os.path.join(plugins_folder, "data_processing")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(data_processing_folder, 'dcm-prod.json')
os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'

JUMP_DATES_CSV = os.path.join(data_processing_folder, 'intervals_for_jump.csv')


def read_csv_in_chunks(gcs_path, batch_size=10000, project_id='dcm-prod-ba2f'):
    """
    Reads a CSV file from Google Cloud Storage in chunks.
    Parameters:
    - gcs_path (str): The path to the CSV file on GCS.
    - batch_size (int, optional): The number of rows per chunk. Default is 10,000.
    - project_id (str, optional): The GCP project id. Default is 'dcm-prod-ba2f'.
    Yields:
    - pd.DataFrame: The data chunk as a DataFrame.
    """
    # Create GCS file system object
    fs = gcsfs.GCSFileSystem(project=project_id)

    # Open the GCS file for reading
    with fs.open(gcs_path, 'r') as f:
        # Yield chunks from the CSV
        for chunk in pd.read_csv(f, chunksize=batch_size, index_col=0):
            yield chunk


def airflow_wrapper(**kwargs):
    params = kwargs['params']

    # Read all required data into step_action_args dictionary
    step_action_args = {
        k: pd.read_csv(v.format(os.environ['GCS_BUCKET']), index_col=0) for k, v in kwargs['required_data'].items()
    }
    print(f'Executing step action with args {step_action_args}')

    # Execute do_step_action method
    data_outputs = kwargs['class'](**params).do_step_action(**step_action_args)

    # If the method doesn't return a dictionary (for classes returning just a single DataFrame)
    # convert it into a dictionary for consistency
    if not isinstance(data_outputs, dict):
        data_outputs = {list(kwargs['provided_data'].keys())[0]: data_outputs}

    # Save each output data to its respective path on GCS
    for data_key, data_value in data_outputs.items():
        if data_key in kwargs['provided_data']:
            gcs_path = kwargs['provided_data'][data_key].format(os.environ['GCS_BUCKET'], data_key)
            print(gcs_path)
            data_value.to_csv(gcs_path)


def transform_params(params_dictionary):
    params = {}
    for key, data_formatter_instance in params_dictionary.items():
        # Call the DataFormatter instance to get the formatted data dictionary
        formatted_data = data_formatter_instance()
        # Add the formatted data dictionary to the params under the same key
        params[key] = formatted_data

    return params


class TaskParamsManager:
    def __init__(self):
        self.params = {}

    def process_and_add_data_formatters(self, params_dictionary):
        # Iterate through each key and DataFormatter instance in the dictionary
        for key, data_formatter_instance in params_dictionary.items():
            # Call the DataFormatter instance to get the formatted data dictionary
            formatted_data = data_formatter_instance()
            # Add the formatted data dictionary to the params under the same key
            self.params[key] = formatted_data

    def add_data_formatter(self, name, data_formatter):
        # This method remains for adding individual DataFormatter instances if needed
        formatted_data = data_formatter()
        self.params[name] = formatted_data


task_params_manager = transform_params(PARAMS_DICTIONARY)

""" Calibration Process"""
with DAG(dag_id="calibration", start_date=days_ago(1)) as dag:


    with TaskGroup("PopulationSplit", tooltip="PopulationSplit") as PopulationSplit:
            FilterRussell1000AugmentedWeekly = PythonOperator(
                task_id="FilterRussell1000AugmentedWeekly",
                python_callable=airflow_wrapper,
                op_kwargs=task_params_manager['FilterRussell1000AugmentedWeekly'],
            )

    with TaskGroup("Residualization", tooltip="Residualization") as Residualization:
            FactorNeutralizationForStackingWeekly = PythonOperator(
                task_id="FactorNeutralizationForStackingWeekly",
                python_callable=airflow_wrapper,
                op_kwargs=task_params_manager['FactorNeutralizationForStackingWeekly'],
            )


    with TaskGroup("ResidualizedStandardization", tooltip="ResidualizedStandardization") as ResidualizedStandardization:
            FactorStandardizationNeutralizedForStackingWeekly = PythonOperator(
                task_id="FactorStandardizationNeutralizedForStackingWeekly",
                python_callable=airflow_wrapper,
                op_kwargs=task_params_manager['FactorStandardizationNeutralizedForStackingWeekly'],
            )

    with TaskGroup("AddFinalFoldId", tooltip="AddFinalFoldId") as AddFinalFoldId:
            AddFoldIdToNormalizedDataPortfolioWeekly = PythonOperator(
                task_id="AddFoldIdToNormalizedDataPortfolioWeekly",
                python_callable=airflow_wrapper,
                op_kwargs=task_params_manager['AddFoldIdToNormalizedDataPortfolioWeekly'],
            )



    PopulationSplit >> Residualization >> ResidualizedStandardization >> AddFoldIdToNormalizedDataPortfolioWeekly





if __name__ == '__main__':
    dag.test()
