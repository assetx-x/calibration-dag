from dotenv import load_dotenv
load_dotenv()
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from datetime import timedelta

import os
import sys
# Add the path to the "plugins" folder to sys.path
# Assuming the "calibration-dag" directory is the parent directory of your DAGs folder.
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
plugins_folder = os.path.join(parent_directory, "plugins")
data_processing_folder = os.path.join(plugins_folder, "data_processing")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(data_processing_folder,'dcm-prod.json')
os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'

##### DATA PULL STEP INSTRUCTIONS #####
from data_pull_step import CALIBRATIONDATEJUMP_PARAMS,S3_SECURITY_MASTER_READER_PARAMS,S3_INDUSTRY_MAPPER_READER_PARAMS,\
S3_ECON_TRANSFORMATION_PARAMS,YAHOO_DAILY_PRICE_READER_PARAMS,S3_RUSSELL_COMPONENT_READER_PARAMS,S3_RAW_SQL_READER_PARAMS,\
SQL_MINUTE_TO_DAILY_EQUITY_PRICES_PARAMS

### ECON DATA STEP INSTRUCTIONS ####
from econ_data_step import DOWNLOAD_ECONOMIC_DATA


# PARAMS
END_DATE = '2023-06-28'
JUMP_DATES_CSV = os.path.join(data_processing_folder,'intervals_for_jump.csv')




def airflow_wrapper(**kwargs):
    params = kwargs['params']

    # Read all required data into step_action_args dictionary
    try:
        step_action_args = {k: pd.read_csv(v.format(os.environ['GCS_BUCKET'],kwargs['start_date']), index_col=0) for k, v in kwargs['required_data'].items()}
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
            gcs_path = kwargs['provided_data'][data_key].format(os.environ['GCS_BUCKET'], kwargs['start_date'], data_key)
            data_value.to_csv(gcs_path)


""" Calibration Process"""


with DAG(dag_id="test_calibration", start_date=days_ago(1)) as dag:



    CalibrationDatesJump = PythonOperator(
        task_id="calibration_jump_dates",
        python_callable=airflow_wrapper,
        provide_context=True,
        op_kwargs=CALIBRATIONDATEJUMP_PARAMS,
        execution_timeout=timedelta(minutes=25)
    )

    S3SecurityMasterReader = PythonOperator(
        task_id="s3_security_master_reader",
        python_callable=airflow_wrapper,
        op_kwargs=S3_SECURITY_MASTER_READER_PARAMS,
        execution_timeout=timedelta(minutes=25)
    )

    S3IndustryMappingReader = PythonOperator(
        task_id="s3_industry_mapping_reader",
        python_callable=airflow_wrapper,
        op_kwargs=S3_INDUSTRY_MAPPER_READER_PARAMS,
        execution_timeout=timedelta(minutes=25)
    )

    S3EconTransformationReader = PythonOperator(
        task_id="s3_econ_transformation_reader",
        python_callable=airflow_wrapper,
        op_kwargs=S3_ECON_TRANSFORMATION_PARAMS,
        execution_timeout=timedelta(minutes=25)
    )

    YahooDailyPriceReader = PythonOperator(
        task_id="yahoo_daily_price_reader",
        python_callable=airflow_wrapper,
        op_kwargs=YAHOO_DAILY_PRICE_READER_PARAMS,
        execution_timeout=timedelta(minutes=25)
    )

    S3RussellComponentReader = PythonOperator(
        task_id="s3_russell_component_reader",
        python_callable=airflow_wrapper,
        op_kwargs=S3_RUSSELL_COMPONENT_READER_PARAMS,
        execution_timeout=timedelta(minutes=25)
    )

    S3RawQuandlDataReader = PythonOperator(
        task_id="s3_raw_quandl_reader",
        python_callable=airflow_wrapper,
        op_kwargs=S3_RAW_SQL_READER_PARAMS,
        execution_timeout=timedelta(minutes=25)
    )



    DownloadEconomicData = PythonOperator(
        task_id="download_economic_data",
        python_callable=airflow_wrapper,
        op_kwargs=DOWNLOAD_ECONOMIC_DATA
    )


    CalibrationDatesJump >> S3SecurityMasterReader >> S3IndustryMappingReader >> S3EconTransformationReader >> YahooDailyPriceReader >> S3RussellComponentReader >> S3RawQuandlDataReader >> DownloadEconomicData
