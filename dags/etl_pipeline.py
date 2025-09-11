import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timedelta
from scripts.extract_data import extract_weather_data, load_to_gcs
from scripts.load_data import bigquery_client, table_exists, bigquery_schema

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
        from datetime import datetime
        from airflow.models import Variable
        date = datetime.today().strftime("%Y-%m-%d")

        logging.basicConfig(level=logging.INFO)
        BUCKET_NAME = Variable.get("gcs_bucket")
        BUSINESS_DOMAIN = Variable.get("business_domain")
        GCP_PROJECT_ID = Variable.get("gcp_project_id")
        DATASET = Variable.get("bq_dataset")
        TABLE = Variable.get("bq_table")

        source_gcs = f"gs://{BUCKET_NAME}/{BUSINESS_DOMAIN}/raw_data/{date}/*.parquet"
        table_id = f"{GCP_PROJECT_ID}.{DATASET}.{TABLE}"

        logging.info("Start loading data to BigQuery")
        client = bigquery_client()
        write_disposition = table_exists(client, table_id)

        logging.info("Defining table schema")
        job_config = bigquery_schema(write_disposition)

        job = client.load_table_from_uri(
            source_gcs,
            table_id,
            job_config=job_config,
            location="asia-southeast1",
        )
        job.result()

        table = client.get_table(table_id)
        logging.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
        logging.info("Load data to BigQuery complete")

    task_dbt_data_model = BashOperator(
        task_id="dbt_data_model",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir ."
    )

    task_extract_and_load() >>  task_load_gcs_to_bq() >> task_dbt_data_model

pipeline()