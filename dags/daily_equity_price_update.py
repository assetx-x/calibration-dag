import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from insert_data import main


if sys.platform in ['darwin', 'linux']:
    """
    To log in into GCP locally use the following command:
    $ gcloud auth application-default login
    and follow the instructions, then the json will be created automatically
    """
    home_path = os.getenv('HOME')
    credentials_path = os.path.join(home_path, '.config/gcloud/application_default_credentials.json')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    print(f'Credentials set to {os.environ["GOOGLE_APPLICATION_CREDENTIALS"]}')
else:
    raise ValueError('Only Linux is supported')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'start_date': datetime(2023, 10, 10),
    'schedule_interval': '0 1 * * *',  # Schedule to everyday at 1AM
}


with DAG(dag_id="daily_equity_prices_auto_updater", default_args=default_args) as dag:

    tart_task = PythonOperator(
        task_id="update_daily_equity_prices_main",
        python_callable=main,
    )


if __name__ == '__main__':
    dag.test()
