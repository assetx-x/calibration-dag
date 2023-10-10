from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from insert_data import main

with DAG(dag_id="daily_equity_prices_auto_updater", start_date=days_ago(1)) as dag:

    tart_task = PythonOperator(
        task_id="update_daily_equity_prices_main",
        python_callable=main,
    )

