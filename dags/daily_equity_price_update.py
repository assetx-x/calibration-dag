from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from insert_data import main


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'start_date': datetime(2023, 1, 1),
    'schedule_interval': '0 1 * * *',  # Schedule to everyday at 1AM
}


with DAG(dag_id="daily_equity_prices_auto_updater", default_args=default_args) as dag:

    tart_task = PythonOperator(
        task_id="update_daily_equity_prices_main",
        python_callable=main,
    )


if __name__ == '__main__':
    dag.test()
