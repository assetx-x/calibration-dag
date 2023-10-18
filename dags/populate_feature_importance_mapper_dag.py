

from ax_gcp_functions import get_security_master_full,v3_feature_importance_edit

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
import gcsfs
import datetime
import os
import sys

# Add the path to the "plugins" folder to sys.path
# Assuming the "calibration-dag" directory is the parent directory of your DAGs folder.
parent_directory = os.path.abspath(os.getcwd())
json_auth_file = os.path.join('plugins', 'data_processing', 'dcm-prod.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = json_auth_file
os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'
print(f"GOOGLE_APPLICATION_CREDENTIALS: {os.environ['GOOGLE_APPLICATION_CREDENTIALS']}")
print(f"GCS_BUCKET: {os.environ['GCS_BUCKET']}")


def main():
    value_prediction_csv_path = (
        "gs://dcm-prod-ba2f-us-dcm-data-test/alex/value_predictions (1).csv"
    )
    growth_prediction_csv_path = (
        "gs://dcm-prod-ba2f-us-dcm-data-test/alex/growth_predictions (1).csv"
    )

    value_predictions = pd.read_csv(value_prediction_csv_path).set_index(
        ["date", "ticker"]
    )
    growth_predictions = pd.read_csv(growth_prediction_csv_path).set_index(
        ["date", "ticker"]
    )

    security_ids = sorted(
        set(
            list(growth_predictions.index.levels[1])
            + list(value_predictions.index.levels[1])
        )
    )

    holding_dictionary = []
    security_master = get_security_master_full()

    for sec_id in security_ids:
        try:
            ticker = security_master[(security_master.dcm_security_id == sec_id)]['ticker'].iloc[0]
            sector = security_master[(security_master.dcm_security_id == sec_id)]['Sector'].iloc[0]

            fi, model_type = v3_feature_importance_edit(sec_id)

            # prediction,model_type = grab_single_name_prediction('value', sec_id)

            pred_dict = {'ticker': ticker,
                         'sec_id': sec_id,
                         'model_type': model_type,
                         'sector': sector
                         }
            holding_dictionary.append(pred_dict)
            print(ticker)

        except Exception:
            pass

    holding_df = pd.DataFrame(holding_dictionary)
    path="gs://dcm-prod-ba2f-us-dcm-data-test/alex/feature_importance_mapper_df.csv"
    holding_df.to_csv(path)
    print('data written')



with DAG(dag_id="populate_feature_importance_mapper") as dag:

    tart_task = PythonOperator(
        task_id="mapper",
        python_callable=main,
    )



