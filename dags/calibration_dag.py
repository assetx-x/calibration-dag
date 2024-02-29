from airflow.decorators import task
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator

import pandas as pd
from datetime import timedelta
import gcsfs

import os
import sys


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


from parameters_file import PARAMS_DICTIONARY

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


#task_params_manager = TaskParamsManager()
#task_params_manager.process_and_add_data_formatters(PARAMS_DICTIONARY)

task_params_manager = transform_params(PARAMS_DICTIONARY)



""" Calibration Process"""
with DAG(
        dag_id="calibration",
        start_date=days_ago(1),
        schedule_interval="0 0 1 * ? *",  # Schedule to run at 00:00 on the first day of every month
) as dag:

    with TaskGroup("DerivedSimplePriceFeatureProcessing",
                   tooltip="DerivedSimplePriceFeatureProcessing") as DerivedSimplePriceFeatureProcessing:
        ComputeBetaQuantamental = PythonOperator(
            task_id="ComputeBetaQuantamental",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['ComputeBetaQuantamental']
        )

        CalculateMACD = PythonOperator(
            task_id="CalculateMACD",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateMACD']
        )

        CalcualteCorrelation = PythonOperator(
            task_id="CalcualteCorrelation",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalcualteCorrelation']
        )

        CalculateDollarVolume = PythonOperator(
            task_id="CalculateDollarVolume",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateDollarVolume']
        )

        CalculateOvernightReturn = PythonOperator(
            task_id="CalculateOvernightReturn",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateOvernightReturn']
        )

        CalculatePastReturnEquity = PythonOperator(
            task_id="CalculatePastReturnEquity",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculatePastReturnEquity']
        )

        CalculateTaLibSTOCH = PythonOperator(
            task_id="CalculateTaLibSTOCH",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateTaLibSTOCH']
        )

        CalculateTaLibSTOCHF = PythonOperator(
            task_id="CalculateTaLibSTOCHF",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateTaLibSTOCHF']
        )

        CalculateTaLibTRIX = PythonOperator(
            task_id="CalculateTaLibTRIX",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateTaLibTRIX']
        )

        CalculateTaLibULTOSC = PythonOperator(
            task_id="CalculateTaLibULTOSC",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateTaLibULTOSC']
        )

        ComputeBetaQuantamental >> CalculateMACD >> CalcualteCorrelation >> CalculateDollarVolume >> CalculateOvernightReturn >> CalculatePastReturnEquity >> CalculateTaLibSTOCH >> CalculateTaLibSTOCHF >> CalculateTaLibULTOSC

    with TaskGroup("MergeStep", tooltip="MergeStep") as MergeStep:
        QuantamentalMerge = PythonOperator(
            task_id="QuantamentalMerge",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['QuantamentalMerge'],
            execution_timeout=timedelta(minutes=150)
        )

    with TaskGroup("FilterDatesSingleNames", tooltip="FilterDatesSingleNames") as FilterDatesSingleNames:
        FilterMonthlyDatesFullPopulationWeekly = PythonOperator(
            task_id="FilterMonthlyDatesFullPopulationWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['FilterMonthlyDatesFullPopulationWeekly']
        )

        CreateMonthlyDataSingleNamesWeekly = PythonOperator(
            task_id="CreateMonthlyDataSingleNamesWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CreateMonthlyDataSingleNamesWeekly']
        )

        FilterMonthlyDatesFullPopulationWeekly >> CreateMonthlyDataSingleNamesWeekly

    with TaskGroup("Transformation", tooltip="Transformation") as Transformation:
        CreateYahooDailyPriceRolling = PythonOperator(
            task_id="CreateYahooDailyPriceRolling",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CreateYahooDailyPriceRolling']
        )

        TransformEconomicDataWeekly = PythonOperator(
            task_id="TransformEconomicDataWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['TransformEconomicDataWeekly']
        )

        CreateIndustryAverageWeekly = PythonOperator(
            task_id="CreateIndustryAverageWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CreateIndustryAverageWeekly']
        )

        CreateYahooDailyPriceRolling >> TransformEconomicDataWeekly >> CreateIndustryAverageWeekly

    with TaskGroup("MergeEcon", tooltip="MergeEcon") as MergeEcon:
        QuantamentalMergeEconIndustryWeekly = PythonOperator(
            task_id="QuantamentalMergeEconIndustryWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['QuantamentalMergeEconIndustryWeekly']
        )

    with TaskGroup("Standarization", tooltip="Standarization") as Standarization:
        FactorStandardizationFullPopulationWeekly = PythonOperator(
            task_id="FactorStandardizationFullPopulationWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['FactorStandardizationFullPopulationWeekly']
        )

    with TaskGroup("ActiveMatrix", tooltip="ActiveMatrix") as ActiveMatrix:
        GenerateActiveMatrixWeekly = PythonOperator(
            task_id="GenerateActiveMatrixWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['GenerateActiveMatrixWeekly']
        )

    with TaskGroup("AdditionalGanFeatures", tooltip="AdditionalGanFeatures") as AdditionalGanFeatures:
        GenerateBMEReturnsWeekly = PythonOperator(
            task_id="GenerateBMEReturnsWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['GenerateBMEReturnsWeekly']
        )

    with TaskGroup("SaveGANInputs", tooltip="SaveGANInputs") as SaveGANInputs:
        GenerateDataGANWeekly = PythonOperator(
            task_id="GenerateDataGANWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['GenerateDataGANWeekly']
        )




    DerivedSimplePriceFeatureProcessing >> MergeStep >> FilterDatesSingleNames >> Transformation >> MergeEcon >> Standarization >> ActiveMatrix >> AdditionalGanFeatures >> SaveGANInputs




