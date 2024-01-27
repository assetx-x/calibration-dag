from airflow.decorators import task
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.docker_operator import DockerOperator
import pandas as pd
from datetime import timedelta
import gcsfs

import os
import sys

from plugins.generate_gan_results_step import ExtractGANFactors, extract_gan_factors_params

load_dotenv()

# Add the path to the "plugins" folder to sys.path
# Assuming the "calibration-dag" directory is the parent directory of your DAGs folder.
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
plugins_folder = os.path.join(parent_directory, "plugins")
data_processing_folder = os.path.join(plugins_folder, "data_processing")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(
    data_processing_folder, 'dcm-prod.json'
)
os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'

##### DATA PULL STEP INSTRUCTIONS #####

### ECON DATA STEP INSTRUCTIONS ####

####### FundamentalCleanup INSTRUCTIONS #####

### TARTGET INSTRUCTIONS #######
from targets_step import TARGETS_PARAMS

####### DerivedFundamentalDataProcessing ######
from derived_fundamental_data_process_step import CalculateDerivedQuandlFeatures_PARAMS

######## DerivedTechnicalDataProcessing #######

from merge_step import QuantamentalMerge_params

from merge_econ_step import QuantamentalMergeEconIndustryWeekly_params
from active_matrix_step import GenerateActiveMatrixWeekly_params
from additional_gan_features_step import GenerateBMEReturnsWeekly_params

## edits


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
        k: pd.read_csv(v.format(os.environ['GCS_BUCKET']), index_col=0)
        for k, v in kwargs['required_data'].items()
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
            gcs_path = kwargs['provided_data'][data_key].format(
                os.environ['GCS_BUCKET'], data_key
            )
            print(gcs_path)
            data_value.to_csv(gcs_path)


""" Calibration Process"""
with DAG(dag_id="calibration", start_date=days_ago(1)) as dag:

    with TaskGroup(
        "GenerateGANResults", tooltip="GenerateGANResults"
    ) as GenerateGANResults:

        # @task.external_python(
        #     task_id="external_python", python='/.pyenv/versions/3.7.17/bin/python'
        # )
        # def callable_external_python():
        #
        #     egf = ExtractGANFactors(**extract_gan_factors_params)
        #     egf.do_step_action()
        #
        # callable_external_python()

        define_docker_task = DockerOperator(
            task_id='docker_command',
            image='ubuntu:latest',
            api_version='auto',
            auto_remove=True,
            command='/bin/sleep 30',
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge'
        )

    # start_dag = DummyOperator(
    #     task_id='start_dag'
    # )

    # ExtractGANFactors = BashOperator(
    #     task_id="ExtractGANFactors",
    #     # bash_command=f"echo -n 'Im alive'"
    #     bash_command="$(pyenv shims | grep '3.6' | tail -n 1) src/generate_gan_results.py"
    # )

    # ExtractGANFactors = DockerOperator(
    #     task_id="ExtractGANFactors",
    #     # docker_url='unix://var/run/docker.sock',
    #     container_name='task__generate_gan',
    #     command='/bin/sleep 10',
    #     # command=f"python generate_gan_results.py",
    #     api_version='auto',
    #     auto_remove='success',
    #     # image='ubuntu',
    #     image='gan_image',
    #     network_mode='host',
    #     # mounts=['/var/run/docker.sock:/var/run/docker.sock']
    # )

    # ExtractGANFactors
