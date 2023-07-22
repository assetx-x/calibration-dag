from dotenv import load_dotenv
load_dotenv()
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
import os
import sys
# Add the path to the "plugins" folder to sys.path
# Assuming the "calibration-dag" directory is the parent directory of your DAGs folder.
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
plugins_folder = os.path.join(parent_directory, "plugins")
data_processing_folder = os.path.join(plugins_folder, "data_processing")
#sys.path.append(plugins_folder)
#sys.path.append(data_processing_folder)

import data_pull_step as dps
# Get the absolute path of the project root (calibration-dag)


""" Calibration Process"""

with DAG(dag_id="taskgroup_example", start_date=days_ago(1)) as dag:
    start = DummyOperator(task_id="start")

    with TaskGroup("taskgroup_1", tooltip="task group #1") as section_1:
        task_1 = BashOperator(task_id="op-1", bash_command=":")
        task_2 = BashOperator(task_id="op-2", bash_command=":")

    with TaskGroup("taskgroup_2", tooltip="task group #2") as section_2:
        task_3 = BashOperator(task_id="op-3", bash_command=":")
        task_4 = BashOperator(task_id="op-4", bash_command=":")

    some_other_task = DummyOperator(task_id="some-other-task")

    end = DummyOperator(task_id="end")

    start >> section_1 >> some_other_task >> section_2 >> end



"""@dag(

    start_date= datetime(2023,7,22),
    catchup=False,
    tags=['calibration_test']

)
def data_pull():

    # DataPull step of dag
    @task()
    def S3SecurityMasterReader():
        data_pull_step.S3SecurityMasterReader().do_step_action()


    @task()
    def S
"""




#CalculateTargetReturns.do_step_action()

