import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timedelta
from scripts.extract_data import extract_and_load

from airflow import DAG
from airflow.decorators import dag
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "etl_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
    }

@dag (
    start_date=datetime(25, 9, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
)

def pipeline():

    task_start = EmptyOperator(task_id="task_start")

    task_extract_load = PythonOperator(
        task_id="task_extract_load",
        python_callable=extract_and_load,
    )

    task_end = EmptyOperator(task_id="task_end")

    task_start >> task_extract_load >> task_end

pipeline()