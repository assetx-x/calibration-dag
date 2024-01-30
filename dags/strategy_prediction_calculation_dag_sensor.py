from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from dags.strategy_prediction_calculation_dag import list_files_task, GS_BUCKET_NAME


STRATEGY_PATH_BLOB = 'api_v2_storage/strategies/*'
INACTIVITY_PERIOD = 30
MIN_OBJECT_AGE = 300


default_args = {
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries_delay': timedelta(minutes=1)
}

dag = DAG('your_dag', default_args=default_args, schedule_interval=timedelta(minutes=5))


def create_gcs_file_sensor(obj):
    """
    Creates a Google Cloud Storage (GCS) file sensor.

    :param obj: The name of the GCS object to monitor.
    :type obj: str

    :return: A GCSObjectExistenceSensor instance.
    :rtype: GCSObjectExistenceSensor
    """
    task_id = 'gcs_file_sensor_' + obj.split(".")[0]
    return GCSObjectExistenceSensor(
        task_id=task_id,
        bucket=GS_BUCKET_NAME,
        object=obj,
        inactivity_period=INACTIVITY_PERIOD,
        min_object_age=MIN_OBJECT_AGE,
        allow_delete=False,
        blob=STRATEGY_PATH_BLOB,
        dag=dag
    )

[
    create_gcs_file_sensor('growth_predictions.csv'),
    create_gcs_file_sensor('largecap_growth_predictions.csv')
] >> list_files_task
