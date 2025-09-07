# Base image from Airflow
FROM apache/airflow:3.0.3-python3.12

# Set working directiry
WORKDIR /opt/airflow

# Copy requirements.txt and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install dbt-core dbt-bigquery dbt-postgres

# Copy essentials and credentials file
COPY dags/ ./dags/
COPY scripts/ ./scripts/
COPY config/ ./config/