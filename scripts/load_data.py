from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def bigquery_client():
    hook = BigQueryHook(gcp_conn_id="google_bq_default")
    bigquery_client = hook.get_client()

    return bigquery_client

def table_exists(bigquery_client, table_id):
    try:
        bigquery_client.get_table(table_id)
        table_exists = True
    except NotFound:
        table_exists = False

    write_disposition = (
        bigquery.WriteDisposition.WRITE_APPEND if table_exists
        else bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    return write_disposition

def bigquery_schema(write_disposition):
    job_config = bigquery.LoadJobConfig(
    write_disposition=write_disposition,
    source_format=bigquery.SourceFormat.PARQUET,
    schema=[
        bigquery.SchemaField("province", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("station_name", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("latitude", bigquery.SqlTypeNames.FLOAT),
        bigquery.SchemaField("longitude", bigquery.SqlTypeNames.FLOAT),
        bigquery.SchemaField("date_time", bigquery.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("temperature", bigquery.SqlTypeNames.FLOAT),
        bigquery.SchemaField("max_temperature", bigquery.SqlTypeNames.FLOAT),
        bigquery.SchemaField("min_temperature", bigquery.SqlTypeNames.FLOAT),
        bigquery.SchemaField("wind_speed", bigquery.SqlTypeNames.FLOAT)
    ],
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date_time",
        )
    )
    return job_config



