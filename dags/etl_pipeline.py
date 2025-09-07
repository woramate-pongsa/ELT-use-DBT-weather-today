import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timedelta
from scripts.extract_data import extract_weather_data, load_to_gcs

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

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

    @task(task_id="task_extract_load")
    def extract_and_load():
        import logging
        import requests

        logging.basicConfig(level=logging.INFO)
        api_key = os.environ.get("API_KEY")

        logging.info(f"Extracting data from {api_key}")
        response = requests.get(api_key)
        df = extract_weather_data(response)

        load_to_gcs(df)
        logging.info("Load data to GCS complete")

    gcs_to_bq = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket="lake_project",
        source_objects=["weather_today_data/raw_data/{{ ds }}/*.parquet"],
        destination_project_dataset_table="warm-helix-412914.weather_today.raw",
        source_format="PARQUET",
        write_disposition="WRITE_APPEND"
    )

    


pipeline()