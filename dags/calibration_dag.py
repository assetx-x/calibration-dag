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

from derived_simple_price_step import (
    ComputeBetaQuantamental_params,
    CalculateMACD_params,
    CalcualteCorrelation_params,
    CalculateDollarVolume_params,
    CalculateOvernightReturn_params,
    CalculatePastReturnEquity_params,
    CalculateTaLibSTOCH_params,
    CalculateTaLibSTOCHF_params,
    CalculateTaLibTRIX_params,
    CalculateTaLibULTOSC_params,
)
from transformation_step import (
    CreateYahooDailyPriceRolling_params,
    TransformEconomicDataWeekly_params,
    CreateIndustryAverageWeekly_params,
)

from save_gan_inputs_step import GenerateDataGANWeekly_params


##### DATA PULL STEP INSTRUCTIONS #####
from data_pull_step import (
    CALIBRATIONDATEJUMP_PARAMS,
    S3_SECURITY_MASTER_READER_PARAMS,
    S3_INDUSTRY_MAPPER_READER_PARAMS,
    S3_ECON_TRANSFORMATION_PARAMS,
    YAHOO_DAILY_PRICE_READER_PARAMS,
    S3_RUSSELL_COMPONENT_READER_PARAMS,
    S3_RAW_SQL_READER_PARAMS,
    SQL_MINUTE_TO_DAILY_EQUITY_PRICES_PARAMS,
    current_gan_universe_params,
)

### ECON DATA STEP INSTRUCTIONS ####
from econ_data_step import DOWNLOAD_ECONOMIC_DATA_PARAMS

####### FundamentalCleanup INSTRUCTIONS #####
from fundamental_cleanup_step import QuandlDataCleanup_PARAMS

### TARTGET INSTRUCTIONS #######
from targets_step import TARGETS_PARAMS

####### DerivedFundamentalDataProcessing ######
from derived_fundamental_data_process_step import CalculateDerivedQuandlFeatures_PARAMS

######## DerivedTechnicalDataProcessing #######
from derived_technical_data_processing_step import (
    CalculateTaLibSTOCHRSIMultiParam_PARAMS,
    CalculateVolatilityMultiParam_params,
    CalculateTaLibWILLRMultiParam_params,
    CalculateTaLibPPOMultiParam_params,
    CalculateTaLibADXMultiParam_params,
)

from merge_step import QuantamentalMerge_params

from filter_dates_single_names import (
    FilterMonthlyDatesFullPopulationWeekly_params,
    CreateMonthlyDataSingleNamesWeekly_params,
)

from merge_econ_step import QuantamentalMergeEconIndustryWeekly_params
from standarization_step import FactorStandardizationFullPopulationWeekly_params
from active_matrix_step import GenerateActiveMatrixWeekly_params
from additional_gan_features_step import GenerateBMEReturnsWeekly_params
from merge_gan_results_step import (
    AddFoldIdToGANResultDataWeekly_params,
    ConsolidateGANResultsWeekly_params,
)

from merge_signal_step import QuantamentalMergeSignalsWeekly_params
from intermediate_model_training import TrainIntermediateModelsWeekly_params
from get_adjustment_factors import SQLReaderAdjustmentFactors_params
from get_raw_prices_step import CalculateRawPrices_params

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
    with TaskGroup("MergeGANResults", tooltip="MergeGANResults") as MergeGANResults:
        ConsolidateGANResultsWeekly = PythonOperator(
            task_id="ConsolidateGANResultsWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=ConsolidateGANResultsWeekly_params,
        )

        AddFoldIdToGANResultDataWeekly = PythonOperator(
            task_id="AddFoldIdToGANResultDataWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=AddFoldIdToGANResultDataWeekly_params,
        )

    with TaskGroup("IntermediateModelTraining", tooltip="IntermediateModelTraining") as IntermediateModelTraining:
        TrainIntermediateModelsWeekly = PythonOperator(
            task_id="TrainIntermediateModelsWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=TrainIntermediateModelsWeekly_params,
        )


    with TaskGroup("MergeSignal", tooltip="MergeSignal") as MergeSignal:
        QuantamentalMergeSignalsWeekly = PythonOperator(
            task_id="QuantamentalMergeSignalsWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=QuantamentalMergeSignalsWeekly_params,
        )

    with TaskGroup("GetAdjustmentFactors", tooltip="GetAdjustmentFactors") as GetAdjustmentFactors:
            SQLReaderAdjustmentFactors = PythonOperator(
                task_id="SQLReaderAdjustmentFactors",
                python_callable=airflow_wrapper,
                op_kwargs=SQLReaderAdjustmentFactors_params,
            )

    with TaskGroup("GetRawPrices", tooltip="GetRawPrices") as GetRawPrices:
            CalculateRawPrices = PythonOperator(
                task_id="CalculateRawPrices",
                python_callable=airflow_wrapper,
                op_kwargs=CalculateRawPrices_params,
            )

    MergeGANResults >> IntermediateModelTraining >> MergeSignal >> GetAdjustmentFactors >> GetRawPrices


