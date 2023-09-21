from dotenv import load_dotenv

from earnings_steps.derived_simple_price_step import ComputeBetaQuantamental_params,CalculateOvernightReturn_params
from earnings_steps.transformation_step import CreateYahooDailyPriceRolling_params, TransformEconomicDataWeekly_params

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
import datetime
import os
import sys

# Add the path to the "plugins" folder to sys.path
# Assuming the "calibration-dag" directory is the parent directory of your DAGs folder.
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
plugins_folder = os.path.join(parent_directory, "plugins")
data_processing_folder = os.path.join(plugins_folder, "data_processing")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(data_processing_folder, 'dcm-prod.json')
os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'

##### DATA PULL STEP INSTRUCTIONS #####
from earnings_steps.data_pull_step import CALIBRATIONDATEJUMP_PARAMS, S3_SECURITY_MASTER_READER_PARAMS, \
    S3_INDUSTRY_MAPPER_READER_PARAMS, \
    S3_ECON_TRANSFORMATION_PARAMS, YAHOO_DAILY_PRICE_READER_PARAMS, S3_RUSSELL_COMPONENT_READER_PARAMS, \
    S3_RAW_SQL_READER_PARAMS, \
    SQL_MINUTE_TO_DAILY_EQUITY_PRICES_PARAMS,current_gan_universe_params

### ECON DATA STEP INSTRUCTIONS ####
from earnings_steps.econ_data_step import DOWNLOAD_ECONOMIC_DATA_PARAMS

####### FundamentalCleanup INSTRUCTIONS #####
from earnings_steps.fundamental_cleanup_step import QuandlDataCleanup_PARAMS


####### DerivedFundamentalDataProcessing ######
from earnings_steps.derived_fundamental_data_process_step import CalculateDerivedQuandlFeatures_PARAMS



from earnings_steps.merge_step import QuantamentalMerge_params

from earnings_steps.filter_dates_single_names import FilterMonthlyDatesFullPopulationWeekly_params, \
    CreateMonthlyDataSingleNamesWeekly_params

from earnings_steps.merge_econ_step import QuantamentalMergeEconIndustryWeekly_params

from earnings_steps.pull_earnings_step import PullEarnings_params
from earnings_steps.merge_earnings import MergeEarnings_params

from earnings_steps.standarization_step import FactorStandardizationFullPopulationWeekly_params


current_date = datetime.datetime.now().date()
END_DATE = current_date.strftime('%Y-%m-%d')
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
    step_action_args = {k: pd.read_csv(v.format(os.environ['GCS_BUCKET']), index_col=0) for k, v in kwargs['required_data'].items()}


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


# specify connection as dcm_connection
with DAG(dag_id="earnings_calibration", start_date=days_ago(1)) as dag:

    with TaskGroup("DataPull", tooltip="DataPull") as DataPull:
        CalibrationDatesJump = PythonOperator(
            task_id="CalibrationDatesJump",
            python_callable=airflow_wrapper,
            provide_context=True,
            op_kwargs=CALIBRATIONDATEJUMP_PARAMS,
            execution_timeout=timedelta(minutes=25),
            conn_id='dcm_connection'
        )

        S3SecurityMasterReader = PythonOperator(
            task_id="S3SecurityMasterReader",
            python_callable=airflow_wrapper,
            op_kwargs=S3_SECURITY_MASTER_READER_PARAMS,
            execution_timeout=timedelta(minutes=25),
            conn_id='dcm_connection'
        )

        S3GANUniverseReader = PythonOperator(
            task_id="S3GANUniverseReader",
            python_callable=airflow_wrapper,
            op_kwargs=current_gan_universe_params,
            execution_timeout=timedelta(minutes=25),
            conn_id='dcm_connection'
        )

        S3IndustryMappingReader = PythonOperator(
            task_id="S3IndustryMappingReader",
            python_callable=airflow_wrapper,
            op_kwargs=S3_INDUSTRY_MAPPER_READER_PARAMS,
            execution_timeout=timedelta(minutes=25),
            conn_id='dcm_connection'
        )

        S3EconTransformationReader = PythonOperator(
            task_id="S3EconTransformationReader",
            python_callable=airflow_wrapper,
            op_kwargs=S3_ECON_TRANSFORMATION_PARAMS,
            execution_timeout=timedelta(minutes=25),
            conn_id='dcm_connection'
        )

        YahooDailyPriceReader = PythonOperator(
            task_id="YahooDailyPriceReader",
            python_callable=airflow_wrapper,
            op_kwargs=YAHOO_DAILY_PRICE_READER_PARAMS,
            execution_timeout=timedelta(minutes=25),
            conn_id='dcm_connection'
        )

        S3RussellComponentReader = PythonOperator(
            task_id="S3RussellComponentReader",
            python_callable=airflow_wrapper,
            op_kwargs=S3_RUSSELL_COMPONENT_READER_PARAMS,
            execution_timeout=timedelta(minutes=25),
            conn_id='dcm_connection'
        )

        S3RawQuandlDataReader = PythonOperator(
            task_id="S3RawQuandlDataReader",
            python_callable=airflow_wrapper,
            op_kwargs=S3_RAW_SQL_READER_PARAMS,
            execution_timeout=timedelta(minutes=25),
            conn_id='dcm_connection'
        )

        SQLMinuteToDailyEquityPrices = PythonOperator(
            task_id="SQLMinuteToDailyEquityPrices",
            python_callable=airflow_wrapper,
            op_kwargs=SQL_MINUTE_TO_DAILY_EQUITY_PRICES_PARAMS,
            execution_timeout=timedelta(minutes=50),
            conn_id='dcm_connection'
        )

        CalibrationDatesJump >> S3SecurityMasterReader >> S3GANUniverseReader >> S3IndustryMappingReader >> S3EconTransformationReader >> YahooDailyPriceReader >> S3RussellComponentReader >> S3RawQuandlDataReader >> SQLMinuteToDailyEquityPrices

    with TaskGroup("EconData", tooltip="EconData") as EconData:
        DownloadEconomicData = PythonOperator(
            task_id="DownloadEconomicData",
            python_callable=airflow_wrapper,
            op_kwargs=DOWNLOAD_ECONOMIC_DATA_PARAMS,
            conn_id='dcm_connection'
        )

    with TaskGroup("FundamentalCleanup", tooltip="FundamentalCleanup") as FundamentalCleanup:
        QuandlDataCleanup = PythonOperator(
            task_id="QuandlDataCleanup",
            python_callable=airflow_wrapper,
            op_kwargs=QuandlDataCleanup_PARAMS,
            conn_id='dcm_connection'
        )

    with TaskGroup("DerivedFundamentalDataProcessing",
                   tooltip="DerivedFundamentalDataProcessing") as DerivedFundamentalDataProcessing:
        CalculateDerivedQuandlFeatures = PythonOperator(
            task_id="CalculateDerivedQuandlFeatures",
            python_callable=airflow_wrapper,
            op_kwargs=CalculateDerivedQuandlFeatures_PARAMS,
            conn_id='dcm_connection'
        )

    with TaskGroup("DerivedSimplePriceFeatureProcessing",
                   tooltip="DerivedSimplePriceFeatureProcessing") as DerivedSimplePriceFeatureProcessing:
        ComputeBetaQuantamental = PythonOperator(
            task_id="ComputeBetaQuantamental",
            python_callable=airflow_wrapper,
            op_kwargs=ComputeBetaQuantamental_params,
            conn_id='dcm_connection'
        )

        CalculateOvernightReturn = PythonOperator(
            task_id="CalculateOvernightReturn",
            python_callable=airflow_wrapper,
            op_kwargs=CalculateOvernightReturn_params,
            conn_id='dcm_connection'
        )

        ComputeBetaQuantamental >> CalculateOvernightReturn

    with TaskGroup("MergeStep", tooltip="MergeStep") as MergeStep:
        QuantamentalMerge = PythonOperator(
            task_id="QuantamentalMerge",
            python_callable=airflow_wrapper,
            op_kwargs=QuantamentalMerge_params,
            conn_id='dcm_connection'
        )

    with TaskGroup("FilterDatesSingleNames", tooltip="FilterDatesSingleNames") as FilterDatesSingleNames:
        FilterMonthlyDatesFullPopulationWeekly = PythonOperator(
            task_id="FilterMonthlyDatesFullPopulationWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=FilterMonthlyDatesFullPopulationWeekly_params,
            conn_id='dcm_connection'
        )

        CreateMonthlyDataSingleNamesWeekly = PythonOperator(
            task_id="CreateMonthlyDataSingleNamesWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=CreateMonthlyDataSingleNamesWeekly_params,
            conn_id='dcm_connection'
        )

        FilterMonthlyDatesFullPopulationWeekly >> CreateMonthlyDataSingleNamesWeekly

    with TaskGroup("Transformation", tooltip="Transformation") as Transformation:
        CreateYahooDailyPriceRolling = PythonOperator(
            task_id="CreateYahooDailyPriceRolling",
            python_callable=airflow_wrapper,
            op_kwargs=CreateYahooDailyPriceRolling_params,
            conn_id='dcm_connection'
        )

        TransformEconomicDataWeekly = PythonOperator(
            task_id="TransformEconomicDataWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=TransformEconomicDataWeekly_params,
            conn_id='dcm_connection'
        )

        CreateYahooDailyPriceRolling >> TransformEconomicDataWeekly

    with TaskGroup("MergeEcon", tooltip="MergeEcon") as MergeEcon:
        QuantamentalMergeEconIndustryWeekly = PythonOperator(
            task_id="QuantamentalMergeEconIndustryWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=QuantamentalMergeEconIndustryWeekly_params,
            conn_id='dcm_connection'
        )

    with TaskGroup("Standarization", tooltip="Standarization") as Standarization:
        FactorStandardizationFullPopulationWeekly = PythonOperator(
            task_id="FactorStandardizationFullPopulationWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=FactorStandardizationFullPopulationWeekly_params,
            conn_id='dcm_connection'
        )

    DataPull >> EconData >> FundamentalCleanup >> DerivedFundamentalDataProcessing >> DerivedSimplePriceFeatureProcessing >> MergeStep >> FilterDatesSingleNames >> Transformation >> MergeEcon >> Standarization

