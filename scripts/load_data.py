    from google.cloud import bigquery

def load_parquet_to_bq():
    from datatime import datetime
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound

    hook = BigQueryHook(gcp_conn_id="google_bq_default")
    bigquery_client = hook.get_client()

    BUCKET_NAME = "lake_project"
    BUSINESS_DOMAIN = "weather_today_data"

    PROJECT_ID = "warm-helix-412914"
    DATASET = "weather_today"
    DATA_NAME = "raw"
    LOCATION = "asia-southeast1"
    date = datetime.today().strftime("%Y-%m-%d")

    source_data_in_gcs = f"gs://{BUCKET_NAME}/{BUSINESS_DOMAIN}/raw_data/{date}/*.parquet"
    table_id = f"{PROJECT_ID}.{DATASET}.{DATA_NAME}"

    try:
        bigquery_client.get_table(table_id)
        table_exists = True
    except NotFound:
        table_exists = False

    write_disposition = (
        bigquery.WriteDisposition.WRITE_APPEND if table_exists
        else bigquery.WriteDisposition.WRITE_TRUNCATE
    )

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

    job = bigquery_client.load_table_from_uri(
        source_data_in_gcs,
        table_id,
        job_config=job_config,
        location=LOCATION,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

def bigquery_client():
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    hook = BigQueryHook(gcp_conn_id="google_bq_default")
    bigquery_client = hook.get_client()

    return bigquery_client

def table_exists(bigquery_client, table_id):
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound

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
    from google.cloud import bigquery

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

def load_to_bq(job_config, source_data_in_gcs, table_id ):

    
    job = bigquery_client.load_table_from_uri(
        source_data_in_gcs,
        table_id,
        job_config=job_config,
        location="asia-southeast1",
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")






