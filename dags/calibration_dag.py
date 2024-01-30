from airflow.decorators import task
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator
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
    with TaskGroup("DataPull", tooltip="DataPull") as DataPull:
        CalibrationDatesJump = PythonOperator(
            task_id="CalibrationDatesJump",
            python_callable=airflow_wrapper,
            provide_context=True,
            op_kwargs=CALIBRATIONDATEJUMP_PARAMS,
            execution_timeout=timedelta(minutes=25)
        )

        S3SecurityMasterReader = PythonOperator(
            task_id="S3SecurityMasterReader",
            python_callable=airflow_wrapper,
            op_kwargs=S3_SECURITY_MASTER_READER_PARAMS,
            execution_timeout=timedelta(minutes=25)
        )

        S3GANUniverseReader = PythonOperator(
            task_id="S3GANUniverseReader",
            python_callable=airflow_wrapper,
            op_kwargs=current_gan_universe_params,
            execution_timeout=timedelta(minutes=25)
        )

        S3IndustryMappingReader = PythonOperator(
            task_id="S3IndustryMappingReader",
            python_callable=airflow_wrapper,
            op_kwargs=S3_INDUSTRY_MAPPER_READER_PARAMS,
            execution_timeout=timedelta(minutes=25)
        )

        S3EconTransformationReader = PythonOperator(
            task_id="S3EconTransformationReader",
            python_callable=airflow_wrapper,
            op_kwargs=S3_ECON_TRANSFORMATION_PARAMS,
            execution_timeout=timedelta(minutes=25)
        )

        YahooDailyPriceReader = PythonOperator(
            task_id="YahooDailyPriceReader",
            python_callable=airflow_wrapper,
            op_kwargs=YAHOO_DAILY_PRICE_READER_PARAMS,
            execution_timeout=timedelta(minutes=25)
        )

        S3RussellComponentReader = PythonOperator(
            task_id="S3RussellComponentReader",
            python_callable=airflow_wrapper,
            op_kwargs=S3_RUSSELL_COMPONENT_READER_PARAMS,
            execution_timeout=timedelta(minutes=25)
        )

        S3RawQuandlDataReader = PythonOperator(
            task_id="S3RawQuandlDataReader",
            python_callable=airflow_wrapper,
            op_kwargs=S3_RAW_SQL_READER_PARAMS,
            execution_timeout=timedelta(minutes=25)
        )

        SQLMinuteToDailyEquityPrices = PythonOperator(
            task_id="SQLMinuteToDailyEquityPrices",
            python_callable=airflow_wrapper,
            op_kwargs=SQL_MINUTE_TO_DAILY_EQUITY_PRICES_PARAMS,
            execution_timeout=timedelta(minutes=150)
        )

        CalibrationDatesJump >> S3SecurityMasterReader >> S3GANUniverseReader >> S3IndustryMappingReader >> S3EconTransformationReader >> YahooDailyPriceReader >> S3RussellComponentReader >> S3RawQuandlDataReader >> SQLMinuteToDailyEquityPrices

    with TaskGroup("EconData", tooltip="EconData") as EconData:
        DownloadEconomicData = PythonOperator(
            task_id="DownloadEconomicData",
            python_callable=airflow_wrapper,
            op_kwargs=DOWNLOAD_ECONOMIC_DATA_PARAMS
        )

    with TaskGroup("FundamentalCleanup", tooltip="FundamentalCleanup") as FundamentalCleanup:
        QuandlDataCleanup = PythonOperator(
            task_id="QuandlDataCleanup",
            python_callable=airflow_wrapper,
            op_kwargs=QuandlDataCleanup_PARAMS
        )

    with TaskGroup("Targets", tooltip="Targets") as Targets:
        CalculateTargetReturns = PythonOperator(
            task_id="CalculateTargetReturns",
            python_callable=airflow_wrapper,
            op_kwargs=TARGETS_PARAMS
        )

    with TaskGroup("DerivedFundamentalDataProcessing",
                   tooltip="DerivedFundamentalDataProcessing") as DerivedFundamentalDataProcessing:
        CalculateDerivedQuandlFeatures = PythonOperator(
            task_id="CalculateDerivedQuandlFeatures",
            python_callable=airflow_wrapper,
            op_kwargs=CalculateDerivedQuandlFeatures_PARAMS
        )

    with TaskGroup("DerivedTechnicalDataProcessing",
                   tooltip="DerivedTechnicalDataProcessing") as DerivedTechnicalDataProcessing:
        CalculateTaLibSTOCHRSIMultiParam = PythonOperator(
            task_id="CalculateTaLibSTOCHRSIMultiParam",
            python_callable=airflow_wrapper,
            op_kwargs=CalculateTaLibSTOCHRSIMultiParam_PARAMS
        )

        CalculateVolatilityMultiParam = PythonOperator(
            task_id="CalculateVolatilityMultiParam",
            python_callable=airflow_wrapper,
            op_kwargs=CalculateVolatilityMultiParam_params
        )

        CalculateTaLibWILLRMultiParam = PythonOperator(
            task_id="CalculateTaLibWILLRMultiParam",
            python_callable=airflow_wrapper,
            op_kwargs=CalculateTaLibWILLRMultiParam_params
        )

        CalculateTaLibPPOMultiParam = PythonOperator(
            task_id="CalculateTaLibPPOMultiParam",
            python_callable=airflow_wrapper,
            op_kwargs=CalculateTaLibPPOMultiParam_params
        )

        CalculateTaLibADXMultiParam = PythonOperator(
            task_id="CalculateTaLibADXMultiParam",
            python_callable=airflow_wrapper,
            op_kwargs=CalculateTaLibADXMultiParam_params
        )

        CalculateTaLibSTOCHRSIMultiParam >> CalculateVolatilityMultiParam >> CalculateTaLibWILLRMultiParam >> CalculateTaLibPPOMultiParam >> CalculateTaLibADXMultiParam

    with TaskGroup("DerivedSimplePriceFeatureProcessing",
                   tooltip="DerivedSimplePriceFeatureProcessing") as DerivedSimplePriceFeatureProcessing:
        ComputeBetaQuantamental = PythonOperator(
            task_id="ComputeBetaQuantamental",
            python_callable=airflow_wrapper,
            op_kwargs=ComputeBetaQuantamental_params
        )

        CalculateMACD = PythonOperator(
            task_id="CalculateMACD",
            python_callable=airflow_wrapper,
            op_kwargs=CalculateMACD_params
        )

        CalcualteCorrelation = PythonOperator(
            task_id="CalcualteCorrelation",
            python_callable=airflow_wrapper,
            op_kwargs=CalcualteCorrelation_params
        )

        CalculateDollarVolume = PythonOperator(
            task_id="CalculateDollarVolume",
            python_callable=airflow_wrapper,
            op_kwargs=CalculateDollarVolume_params
        )

        CalculateOvernightReturn = PythonOperator(
            task_id="CalculateOvernightReturn",
            python_callable=airflow_wrapper,
            op_kwargs=CalculateOvernightReturn_params
        )

        CalculatePastReturnEquity = PythonOperator(
            task_id="CalculatePastReturnEquity",
            python_callable=airflow_wrapper,
            op_kwargs=CalculatePastReturnEquity_params
        )

        CalculateTaLibSTOCH = PythonOperator(
            task_id="CalculateTaLibSTOCH",
            python_callable=airflow_wrapper,
            op_kwargs=CalculateTaLibSTOCH_params
        )

        CalculateTaLibSTOCHF = PythonOperator(
            task_id="CalculateTaLibSTOCHF",
            python_callable=airflow_wrapper,
            op_kwargs=CalculateTaLibSTOCHF_params
        )

        CalculateTaLibTRIX = PythonOperator(
            task_id="CalculateTaLibTRIX",
            python_callable=airflow_wrapper,
            op_kwargs=CalculateTaLibTRIX_params
        )

        CalculateTaLibULTOSC = PythonOperator(
            task_id="CalculateTaLibULTOSC",
            python_callable=airflow_wrapper,
            op_kwargs=CalculateTaLibULTOSC_params
        )

        ComputeBetaQuantamental >> CalculateMACD >> CalcualteCorrelation >> CalculateDollarVolume >> CalculateOvernightReturn >> CalculatePastReturnEquity >> CalculateTaLibSTOCH >> CalculateTaLibSTOCHF >> CalculateTaLibULTOSC



    DataPull >> EconData >> FundamentalCleanup >> Targets >> DerivedFundamentalDataProcessing >> DerivedTechnicalDataProcessing >> DerivedSimplePriceFeatureProcessing
