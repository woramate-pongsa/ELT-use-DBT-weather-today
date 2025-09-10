import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timedelta
from scripts.extract_data import extract_weather_data, load_to_gcs
from scripts.load_data import bigquery_client, table_exists, bigquery_schema, load_to_bq

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    "owner": "etl_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
    }

@dag (
    start_date=datetime(2025, 9, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
)

def pipeline():

    @task(task_id="extract_and_load")
    def task_extract_and_load():
        import logging
        import requests

        logging.basicConfig(level=logging.INFO)
        api_key = os.environ.get("API_KEY")

        logging.info(f"Extracting data from {api_key}")
        response = requests.get(api_key)
        df = extract_weather_data(response)

        load_to_gcs(df)
        logging.info("Load data to GCS complete")

    @task(task_id="load_gcs_to_bq")
    def task_load_gcs_to_bq():
        import logging
        from airflow.modules import Variable

        logging.basicConfig(level=logging.INFO)
        BUCKET_NAME = Variable.get("gcs_bucket")
        BUSINESS_DOMAIN = Variable.get("business_domain")
        DATASET = Variable.get("bq_dataset")
        RAW_TABLE = Variable.get("bq_table_raw")

        table_id = f"warm-helix-412914.{DATASET}.{RAW_TABLE}"
        source_gcs = f"gs://{BUCKET_NAME}/{BUSINESS_DOMAIN}/raw_data/{{ ds }}/*.parquet"

        logging.info("Start loading data to BigQuery")
        client = bigquery_client()
        write_disposition = table_exists(client, table_id)

        logging.info("Defining Schema")
        job_config = bigquery_schema(write_disposition)

        load_to_bq(job_config, source_gcs, table_id)
        logging.info("Loading data to Bigquery complete")



    task_dbt_data_model = BashOperator(
        task_id="dbt_data_model",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir ."
    )

    task_extract_and_load() >>  task_load_gcs_to_bq() >> task_dbt_data_model

pipeline()