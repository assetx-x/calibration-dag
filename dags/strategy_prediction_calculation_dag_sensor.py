from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor, GCSObjectUpdateSensor

from dags.strategy_prediction_calculation_dag import GS_BUCKET_NAME, list_files_in_bucket

STRATEGY_PATH_BLOB = Variable.get('STRATEGY_PATH_BLOB', 'api_v2_storage/strategies/*')
INACTIVITY_PERIOD = 30
MIN_OBJECT_AGE = 300


default_args = {
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries_delay': timedelta(minutes=1),
    'schedule_interval': '@daily',
}

dag = DAG(
    'gcp_sensor_dag',
    description='GCP Sensor Prediction Calculation. Triggered by a csv file inside the bucket, and send via API to DB',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5)
)


def create_gcs_file_sensor(obj):
    """
    Creates a Google Cloud Storage (GCS) file sensor.

    Parameters:
    - obj: A string representing the file name or path of the GCS object to be monitored.

    Returns:
    - An instance of the GCSObjectUpdateSensor class.

    Example Usage:
    sensor = create_gcs_file_sensor("my-bucket/my-file.txt")
    """
    task_id = 'gcs_file_sensor_' + obj.split(".")[0]

    return GCSObjectUpdateSensor(
        task_id=task_id,
        bucket=GS_BUCKET_NAME,
        object=obj,
        dag=dag,
        ts_func=list_files_in_bucket
    )


[
    create_gcs_file_sensor('growth_predictions_test.csv'),
    create_gcs_file_sensor('largecap_growth_predictions_test.csv')
]


if __name__ == '__main__':
    dag.test()
