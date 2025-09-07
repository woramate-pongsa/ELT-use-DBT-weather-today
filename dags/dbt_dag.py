from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "etl_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
    }

@dag (
    start_date=datetime(2025, 9, 1),
    schedule="@daily",
    default_args=default_args,
    catchup=False
)

def dbt_dag():

    dbt_transform = BashOperator(
        task_id="dbt_transform",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir ."
    )

dbt_dag()