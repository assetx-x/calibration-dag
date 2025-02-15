import logging

from dotenv import load_dotenv

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator

import pandas as pd
from datetime import datetime, timedelta
import gcsfs

import os
from plugins.parameters_file import PARAMS_DICTIONARY
import calendar


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}


def is_business_day(date):
    return date.weekday() < 5


def last_business_day_of_month():
    today = datetime.today()
    last_day = calendar.monthrange(today.year, today.month)[1]
    last_date = datetime(today.year, today.month, last_day)
    while not is_business_day(last_date):
        last_date -= timedelta(days=1)
    return last_date


load_dotenv()

# Add the path to the "plugins" folder to sys.path
# Assuming the "calibration-dag" directory is the parent directory of your DAGs folder.
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
plugins_folder = os.path.join(parent_directory, "plugins")
data_processing_folder = os.path.join(plugins_folder, "data_processing")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(
    data_processing_folder, 'dcm-prod.json'
)

os.environ['GCS_BUCKET'] = 'assetx-equity-data'

JUMP_DATES_CSV = os.path.join(data_processing_folder, 'intervals_for_jump.csv')


def read_csv_in_chunks(gcs_path, batch_size=10000, project_id='ax-prod-393101'):
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
            print('GCS BUCKET {}'.format(os.environ['GCS_BUCKET']))
            print(
                'WHERES THE CREDS {}'.format(
                    os.environ['GOOGLE_APPLICATION_CREDENTIALS']
                )
            )
            print('Data Dimensions {}'.format(data_value.shape))
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
with DAG(
    dag_id='calibration',
    default_args=default_args,
    description='Run on the last business day of the month',
    schedule_interval='0 0 28-31 * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    with TaskGroup("DataPull", tooltip="DataPull") as DataPull:
        CalibrationDatesJump = PythonOperator(
            task_id="CalibrationDatesJump",
            python_callable=airflow_wrapper,
            provide_context=True,
            op_kwargs=task_params_manager['CalibrationDatesJump'],
            execution_timeout=timedelta(minutes=25),
        )

        S3SecurityMasterReader = PythonOperator(
            task_id="S3SecurityMasterReader",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['S3SecurityMasterReader'],
            execution_timeout=timedelta(minutes=25),
        )

        S3GANUniverseReader = PythonOperator(
            task_id="S3GANUniverseReader",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['S3GANUniverseReader'],
            execution_timeout=timedelta(minutes=25),
        )

        S3IndustryMappingReader = PythonOperator(
            task_id="S3IndustryMappingReader",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['S3IndustryMappingReader'],
            execution_timeout=timedelta(minutes=25),
        )

        S3EconTransformationReader = PythonOperator(
            task_id="S3EconTransformationReader",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['S3EconTransformationReader'],
            execution_timeout=timedelta(minutes=25),
        )

        YahooDailyPriceReader = PythonOperator(
            task_id="YahooDailyPriceReader",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['YahooDailyPriceReader'],
            execution_timeout=timedelta(minutes=25),
        )

        S3RussellComponentReader = PythonOperator(
            task_id="S3RussellComponentReader",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['S3RussellComponentReader'],
            execution_timeout=timedelta(minutes=25),
        )

        S3RawQuandlDataReader = PythonOperator(
            task_id="S3RawQuandlDataReader",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['S3RawQuandlDataReader'],
            execution_timeout=timedelta(minutes=25),
        )

        SQLMinuteToDailyEquityPrices = PythonOperator(
            task_id="SQLMinuteToDailyEquityPrices",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['SQLMinuteToDailyEquityPrices'],
            execution_timeout=timedelta(minutes=150),
        )

        (
             CalibrationDatesJump
             >> S3SecurityMasterReader
             >> S3GANUniverseReader
             >> S3IndustryMappingReader
             >> S3EconTransformationReader
             >> YahooDailyPriceReader
             >> S3RussellComponentReader
             >> S3RawQuandlDataReader
             >> SQLMinuteToDailyEquityPrices
        )

    with TaskGroup("EconData", tooltip="EconData") as EconData:
        DownloadEconomicData = PythonOperator(
            task_id="DownloadEconomicData",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['DownloadEconomicData'],
        )

    with TaskGroup(
        "FundamentalCleanup", tooltip="FundamentalCleanup"
    ) as FundamentalCleanup:
        QuandlDataCleanup = PythonOperator(
            task_id="QuandlDataCleanup",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['QuandlDataCleanup'],
        )

    with TaskGroup("Targets", tooltip="Targets") as Targets:
        CalculateTargetReturns = PythonOperator(
            task_id="CalculateTargetReturns",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateTargetReturns'],
        )

    with TaskGroup(
        "DerivedFundamentalDataProcessing", tooltip="DerivedFundamentalDataProcessing"
    ) as DerivedFundamentalDataProcessing:
        CalculateDerivedQuandlFeatures = PythonOperator(
            task_id="CalculateDerivedQuandlFeatures",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateDerivedQuandlFeatures'],
        )

    with TaskGroup(
        "DerivedTechnicalDataProcessing", tooltip="DerivedTechnicalDataProcessing"
    ) as DerivedTechnicalDataProcessing:
        CalculateTaLibSTOCHRSIMultiParam = PythonOperator(
            task_id="CalculateTaLibSTOCHRSIMultiParam",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateTaLibSTOCHRSIMultiParam'],
        )

        CalculateVolatilityMultiParam = PythonOperator(
            task_id="CalculateVolatilityMultiParam",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateVolatilityMultiParam'],
        )

        CalculateTaLibWILLRMultiParam = PythonOperator(
            task_id="CalculateTaLibWILLRMultiParam",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateTaLibWILLRMultiParam'],
        )

        CalculateTaLibPPOMultiParam = PythonOperator(
            task_id="CalculateTaLibPPOMultiParam",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateTaLibPPOMultiParam'],
        )

        CalculateTaLibADXMultiParam = PythonOperator(
            task_id="CalculateTaLibADXMultiParam",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateTaLibADXMultiParam'],
        )

        (
            CalculateTaLibSTOCHRSIMultiParam
            >> CalculateVolatilityMultiParam
            >> CalculateTaLibWILLRMultiParam
            >> CalculateTaLibPPOMultiParam
            >> CalculateTaLibADXMultiParam
        )

    with TaskGroup(
        "DerivedSimplePriceFeatureProcessing",
        tooltip="DerivedSimplePriceFeatureProcessing",
    ) as DerivedSimplePriceFeatureProcessing:
        ComputeBetaQuantamental = PythonOperator(
            task_id="ComputeBetaQuantamental",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['ComputeBetaQuantamental'],
        )

        CalculateMACD = PythonOperator(
            task_id="CalculateMACD",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateMACD'],
        )

        CalcualteCorrelation = PythonOperator(
            task_id="CalcualteCorrelation",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalcualteCorrelation'],
        )

        CalculateDollarVolume = PythonOperator(
            task_id="CalculateDollarVolume",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateDollarVolume'],
        )

        CalculateOvernightReturn = PythonOperator(
            task_id="CalculateOvernightReturn",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateOvernightReturn'],
        )

        CalculatePastReturnEquity = PythonOperator(
            task_id="CalculatePastReturnEquity",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculatePastReturnEquity'],
        )

        CalculateTaLibSTOCH = PythonOperator(
            task_id="CalculateTaLibSTOCH",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateTaLibSTOCH'],
        )

        CalculateTaLibSTOCHF = PythonOperator(
            task_id="CalculateTaLibSTOCHF",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateTaLibSTOCHF'],
        )

        CalculateTaLibTRIX = PythonOperator(
            task_id="CalculateTaLibTRIX",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateTaLibTRIX'],
        )

        CalculateTaLibULTOSC = PythonOperator(
            task_id="CalculateTaLibULTOSC",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CalculateTaLibULTOSC'],
        )

        (
            ComputeBetaQuantamental
            >> CalculateMACD
            >> CalcualteCorrelation
            >> CalculateDollarVolume
            >> CalculateOvernightReturn
            >> CalculatePastReturnEquity
            >> CalculateTaLibSTOCH
            >> CalculateTaLibSTOCHF
            >> CalculateTaLibTRIX
            >> CalculateTaLibULTOSC
        )

    with TaskGroup("MergeStep", tooltip="MergeStep") as MergeStep:
        QuantamentalMerge = PythonOperator(
            task_id="QuantamentalMerge",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['QuantamentalMerge'],
            execution_timeout=timedelta(minutes=150),
        )

    with TaskGroup(
        "FilterDatesSingleNames", tooltip="FilterDatesSingleNames"
    ) as FilterDatesSingleNames:
        FilterMonthlyDatesFullPopulationWeekly = PythonOperator(
            task_id="FilterMonthlyDatesFullPopulationWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['FilterMonthlyDatesFullPopulationWeekly'],
        )

        CreateMonthlyDataSingleNamesWeekly = PythonOperator(
            task_id="CreateMonthlyDataSingleNamesWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CreateMonthlyDataSingleNamesWeekly'],
        )

        FilterMonthlyDatesFullPopulationWeekly >> CreateMonthlyDataSingleNamesWeekly

    with TaskGroup("Transformation", tooltip="Transformation") as Transformation:
        CreateYahooDailyPriceRolling = PythonOperator(
            task_id="CreateYahooDailyPriceRolling",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CreateYahooDailyPriceRolling'],
        )

        TransformEconomicDataWeekly = PythonOperator(
            task_id="TransformEconomicDataWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['TransformEconomicDataWeekly'],
        )

        CreateIndustryAverageWeekly = PythonOperator(
            task_id="CreateIndustryAverageWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['CreateIndustryAverageWeekly'],
        )

        (
            CreateYahooDailyPriceRolling
            >> TransformEconomicDataWeekly
            >> CreateIndustryAverageWeekly
        )

    with TaskGroup("MergeEcon", tooltip="MergeEcon") as MergeEcon:
        QuantamentalMergeEconIndustryWeekly = PythonOperator(
            task_id="QuantamentalMergeEconIndustryWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['QuantamentalMergeEconIndustryWeekly'],
        )

    with TaskGroup("Standarization", tooltip="Standarization") as Standarization:
        FactorStandardizationFullPopulationWeekly = PythonOperator(
            task_id="FactorStandardizationFullPopulationWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['FactorStandardizationFullPopulationWeekly'],
        )

    with TaskGroup("ActiveMatrix", tooltip="ActiveMatrix") as ActiveMatrix:
        GenerateActiveMatrixWeekly = PythonOperator(
            task_id="GenerateActiveMatrixWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['GenerateActiveMatrixWeekly'],
        )

    with TaskGroup(
        "AdditionalGanFeatures", tooltip="AdditionalGanFeatures"
    ) as AdditionalGanFeatures:
        GenerateBMEReturnsWeekly = PythonOperator(
            task_id="GenerateBMEReturnsWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['GenerateBMEReturnsWeekly'],
        )

    with TaskGroup("SaveGANInputs", tooltip="SaveGANInputs") as SaveGANInputs:
        GenerateDataGANWeekly = PythonOperator(
            task_id="GenerateDataGANWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['GenerateDataGANWeekly'],
        )

    with TaskGroup(
        "GenerateGANResults", tooltip="GenerateGANResults"
    ) as GenerateGANResults:
        ExtractGANFactors = DockerOperator(
            task_id="ExtractGANFactors",
            container_name='task__generate_gan',
            command="echo 'RUNNING GAN STEP'",
            # command=f"python generate_gan_results.py",
            api_version='auto',
            auto_remove='success',
            image='gan_image',
            network_mode='host',
        )

    with TaskGroup("MergeGANResults", tooltip="MergeGANResults") as MergeGANResults:
        ConsolidateGANResultsWeekly = PythonOperator(
            task_id="ConsolidateGANResultsWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['ConsolidateGANResultsWeekly'],
        )

        AddFoldIdToGANResultDataWeekly = PythonOperator(
            task_id="AddFoldIdToGANResultDataWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['AddFoldIdToGANResultDataWeekly'],
        )

        ConsolidateGANResultsWeekly >> AddFoldIdToGANResultDataWeekly

    with TaskGroup(
        "IntermediateModelTraining", tooltip="IntermediateModelTraining"
    ) as IntermediateModelTraining:
        TrainIntermediateModelsWeekly = PythonOperator(
            task_id="TrainIntermediateModelsWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['TrainIntermediateModelsWeekly'],
        )

    with TaskGroup("MergeSignal", tooltip="MergeSignal") as MergeSignal:
        QuantamentalMergeSignalsWeekly = PythonOperator(
            task_id="QuantamentalMergeSignalsWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['QuantamentalMergeSignalsWeekly'],
        )

    with TaskGroup(
        "GetAdjustmentFactors", tooltip="GetAdjustmentFactors"
    ) as GetAdjustmentFactors:
        SQLReaderAdjustmentFactors = PythonOperator(
            task_id="SQLReaderAdjustmentFactors",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['SQLReaderAdjustmentFactors'],
        )

    with TaskGroup("GetRawPrices", tooltip="GetRawPrices") as GetRawPrices:
        CalculateRawPrices = DockerOperator(
            task_id="CalculateRawPrices",
            container_name='task__prices',
            command="echo 'RUNNING GET RAW PRICES STEP'",
            # command=f"python generate_gan_results.py",
            api_version='auto',
            auto_remove='success',
            image='get_raw_prices_image',
            network_mode='host',
        )

    with TaskGroup("PopulationSplit", tooltip="PopulationSplit") as PopulationSplit:
        FilterRussell1000AugmentedWeekly = DockerOperator(
            task_id="FilterRussell1000AugmentedWeekly",
            container_name='task__filterr1k',
            command="echo 'RUNNING GET FILTER R1K STEP'",
            # command=f"python generate_gan_results.py",
            api_version='auto',
            auto_remove='success',
            image='filter_r1k_image',
            network_mode='host',
        )

    with TaskGroup("Residualization", tooltip="Residualization") as Residualization:
        FactorNeutralizationForStackingWeekly = PythonOperator(
            task_id="FactorNeutralizationForStackingWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['FactorNeutralizationForStackingWeekly'],
        )

    with TaskGroup(
        "ResidualizedStandardization", tooltip="ResidualizedStandardization"
    ) as ResidualizedStandardization:
        FactorStandardizationNeutralizedForStackingWeekly = PythonOperator(
            task_id="FactorStandardizationNeutralizedForStackingWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager[
                'FactorStandardizationNeutralizedForStackingWeekly'
            ],
        )

    with TaskGroup("AddFinalFoldId", tooltip="AddFinalFoldId") as AddFinalFoldId:
        AddFoldIdToNormalizedDataPortfolioWeekly = PythonOperator(
            task_id="AddFoldIdToNormalizedDataPortfolioWeekly",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['AddFoldIdToNormalizedDataPortfolioWeekly'],
        )

    with TaskGroup(
        "FinalModelTraining", tooltip="FinalModelTraining"
    ) as FinalModelTraining:
        RollingModelEstimationWeekly = DockerOperator(
            task_id="RollingModelEstimationWeekly",
            container_name='task__rolling_model_estimation',
            # command=f"python rolling_model_estimation.py",
            api_version='auto',
            auto_remove='success',
            image='rolling_image',
            network_mode='host',
        )

    with TaskGroup("EconFactorShap", tooltip="EconFactorShap") as EconFactorShap:
        EconInterpretation = PythonOperator(
            task_id="EconInterpretation",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['EconInterpretation'],
        )

    with TaskGroup(
        "FinalModelInterpretation", tooltip="FinalModelInterpretation"
    ) as FinalModelInterpretation:
        FinalUnravelingFactors = DockerOperator(
            task_id="FinalUnravelingFactors",
            container_name='task__unravel_r1k',
            command="echo 'RUNNING SHAP UNRAVELING'",
            # command=f"python generate_gan_results.py",
            api_version='auto',
            auto_remove='success',
            image='model_unravel_image',
            network_mode='host',
        )

    with TaskGroup("WritePredictions", tooltip="WritePredictions") as WritePredictions:
        WritePreds = PythonOperator(
            task_id="WritePreds",
            python_callable=airflow_wrapper,
            op_kwargs=task_params_manager['WritePrediction'],
        )

    (
        DataPull
        >> EconData
        >> FundamentalCleanup
        >> Targets
        >> DerivedFundamentalDataProcessing
        >> DerivedTechnicalDataProcessing
        >> DerivedSimplePriceFeatureProcessing
        >> MergeStep
        >> FilterDatesSingleNames
        >> Transformation
        >> MergeEcon
        >> Standarization
        >> ActiveMatrix
        >> AdditionalGanFeatures
        >> SaveGANInputs
        >> GenerateGANResults
        >> MergeGANResults
        >> IntermediateModelTraining
        >> MergeSignal
        >> GetAdjustmentFactors
        >> GetRawPrices
        >> PopulationSplit
        >> Residualization
        >> ResidualizedStandardization
        >> AddFinalFoldId
        >> FinalModelTraining
        >> EconFactorShap
        >> FinalModelInterpretation
        >> WritePredictions
    )


if __name__ == '__main__':
    dag.test()
