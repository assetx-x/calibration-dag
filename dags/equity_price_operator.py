from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

from insert_data import main


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=60),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'ticker_check_dag',
    default_args=default_args,
    description='Your DAG description',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

ticker_operator = PythonOperator(
    task_id='ticker_check',
    python_callable=main,
    dag=dag,
)

start_operator >> ticker_operator


if __name__ == "__main__":
    dag.cli()
