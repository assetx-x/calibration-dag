from dotenv import load_dotenv

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

load_dotenv()
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from datetime import timedelta
import gcsfs

import os
import sys

# Add the path to the "plugins" folder to sys.path
# Assuming the "calibration-dag" directory is the parent directory of your DAGs folder.
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
plugins_folder = os.path.join(parent_directory, "plugins")
data_processing_folder = os.path.join(plugins_folder, "data_processing")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(
    data_processing_folder, 'dcm-prod.json'
)
os.environ[' '] = 'dcm-prod-ba2f-us-dcm-data-test'

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

# PARAMS
END_DATE = '2023-06-28'
JUMP_DATES_CSV = os.path.join(data_processing_folder, 'intervals_for_jump.csv')


def read_csv_in_chunks(gcs_path, batch_size=10000):
    """
    Reads a CSV file from Google Cloud Storage in chunks and returns a concatenated DataFrame.

    Parameters:
    - gcs_path (str): The path to the CSV file on GCS.
    - batch_size (int, optional): The number of rows per chunk. Default is 10,000.

    Returns:
    - pd.DataFrame: The concatenated DataFrame.
    """
    # Create GCS file system object
    fs = gcsfs.GCSFileSystem(project='dcm-prod-ba2f')

    # Use a list to store each chunk as a DataFrame
    dfs = []

    # Open the GCS file for reading
    with fs.open(gcs_path, 'r') as f:
        # Iterate through the CSV in chunks
        for batch in pd.read_csv(f, chunksize=batch_size, index_col=0):
            dfs.append(batch)

    # Concatenate all the chunks into a single DataFrame
    final_df = pd.concat(dfs, ignore_index=True)

    return final_df


def airflow_wrapper(**kwargs):
    params = kwargs['params']

    # Read all required data into step_action_args dictionary
    try:
        step_action_args = {
            k: read_csv_in_chunks(
                v.format(os.environ['GCS_BUCKET'], kwargs['start_date'])
            )
            for k, v in kwargs['required_data'].items()
        }
    except Exception as e:
        print(f"Error reading data: {e}")
        step_action_args = {}

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
                os.environ['GCS_BUCKET'], kwargs['start_date'], data_key
            )
            data_value.to_csv(gcs_path)


""" Calibration Process"""

with DAG(dag_id="test_calibration", start_date=days_ago(1)) as dag:

    CalibrationDatesJump = PythonOperator(
        task_id="CalibrationDatesJump",
        python_callable=airflow_wrapper,
        provide_context=True,
        op_kwargs=CALIBRATIONDATEJUMP_PARAMS,
        execution_timeout=timedelta(minutes=25),
    )

    S3SecurityMasterReader = PythonOperator(
        task_id="S3SecurityMasterReader",
        python_callable=airflow_wrapper,
        op_kwargs=S3_SECURITY_MASTER_READER_PARAMS,
        execution_timeout=timedelta(minutes=25),
    )

    S3IndustryMappingReader = PythonOperator(
        task_id="S3IndustryMappingReader",
        python_callable=airflow_wrapper,
        op_kwargs=S3_INDUSTRY_MAPPER_READER_PARAMS,
        execution_timeout=timedelta(minutes=25),
    )

    S3EconTransformationReader = PythonOperator(
        task_id="S3EconTransformationReader",
        python_callable=airflow_wrapper,
        op_kwargs=S3_ECON_TRANSFORMATION_PARAMS,
        execution_timeout=timedelta(minutes=25),
    )

    YahooDailyPriceReader = PythonOperator(
        task_id="YahooDailyPriceReader",
        python_callable=airflow_wrapper,
        op_kwargs=YAHOO_DAILY_PRICE_READER_PARAMS,
        execution_timeout=timedelta(minutes=25),
    )

    S3RussellComponentReader = PythonOperator(
        task_id="S3RussellComponentReader",
        python_callable=airflow_wrapper,
        op_kwargs=S3_RUSSELL_COMPONENT_READER_PARAMS,
        execution_timeout=timedelta(minutes=25),
    )

    S3RawQuandlDataReader = PythonOperator(
        task_id="S3RawQuandlDataReader",
        python_callable=airflow_wrapper,
        op_kwargs=S3_RAW_SQL_READER_PARAMS,
        execution_timeout=timedelta(minutes=25),
    )

    DownloadEconomicData = PythonOperator(
        task_id="DownloadEconomicData",
        python_callable=airflow_wrapper,
        op_kwargs=DOWNLOAD_ECONOMIC_DATA_PARAMS,
    )

    QuandlDataCleanup = PythonOperator(
        task_id="QuandlDataCleanup",
        python_callable=airflow_wrapper,
        op_kwargs=QuandlDataCleanup_PARAMS,
    )

    CalculateTargetReturns = PythonOperator(
        task_id="CalculateTargetReturns",
        python_callable=airflow_wrapper,
        op_kwargs=TARGETS_PARAMS,
    )

    CalculateDerivedQuandlFeatures = PythonOperator(
        task_id="CalculateDerivedQuandlFeatures",
        python_callable=airflow_wrapper,
        op_kwargs=CalculateDerivedQuandlFeatures_PARAMS,
    )

    CalculateTaLibSTOCHRSIMultiParam = PythonOperator(
        task_id="CalculateTaLibSTOCHRSIMultiParam",
        python_callable=airflow_wrapper,
        op_kwargs=CalculateTaLibSTOCHRSIMultiParam_PARAMS,
    )

    CalculateVolatilityMultiParam = PythonOperator(
        task_id="CalculateVolatilityMultiParam",
        python_callable=airflow_wrapper,
        op_kwargs=CalculateVolatilityMultiParam_params,
    )

    CalculateTaLibWILLRMultiParam = PythonOperator(
        task_id="CalculateTaLibWILLRMultiParam",
        python_callable=airflow_wrapper,
        op_kwargs=CalculateTaLibWILLRMultiParam_params,
    )

    CalculateTaLibPPOMultiParam = PythonOperator(
        task_id="CalculateTaLibPPOMultiParam",
        python_callable=airflow_wrapper,
        op_kwargs=CalculateTaLibPPOMultiParam_params,
    )

    CalculateTaLibADXMultiParam = PythonOperator(
        task_id="CalculateTaLibADXMultiParam",
        python_callable=airflow_wrapper,
        op_kwargs=CalculateTaLibADXMultiParam_params,
    )

    ComputeBetaQuantamental = PythonOperator(
        task_id="ComputeBetaQuantamental",
        python_callable=airflow_wrapper,
        op_kwargs=ComputeBetaQuantamental_params,
    )

    CalculateMACD = PythonOperator(
        task_id="CalculateMACD",
        python_callable=airflow_wrapper,
        op_kwargs=CalculateMACD_params,
    )

    (
        CalibrationDatesJump
        >> S3SecurityMasterReader
        >> S3IndustryMappingReader
        >> S3EconTransformationReader
        >> YahooDailyPriceReader
        >> S3RussellComponentReader
        >> S3RawQuandlDataReader
        >> DownloadEconomicData
        >> QuandlDataCleanup
        >> CalculateTargetReturns
        >> CalculateDerivedQuandlFeatures
        >> CalculateTaLibSTOCHRSIMultiParam
        >> CalculateVolatilityMultiParam
        >> CalculateTaLibWILLRMultiParam
        >> CalculateTaLibPPOMultiParam
        >> CalculateTaLibADXMultiParam
        >> ComputeBetaQuantamental
        >> CalculateMACD
    )
