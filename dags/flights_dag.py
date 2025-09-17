"""
Airflow DAG for Flights
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.dataproc import DataprocBatchSensor
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'Juan Arias',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Flights DAG definition
flights_dag = DAG(
    'flights_data_pipeline',
    default_args=default_args,
    description='Flight data extraction pipeline using OpenSky Network API',
    schedule_interval='@daily',  # Run once daily at midnight UTC
    catchup=False,
    max_active_runs=1,
    tags=['flights', 'opensky', 'real-time', 'bigquery'],
)

# Get configuration from Airflow Variables
PROJECT_ID = Variable.get("gcp_project_id", default_var="pipeline-weather-flights")
REGION = Variable.get("gcp_region", default_var="us-central1")
BUCKET_NAME = Variable.get("storage_bucket", default_var="graduation-pipeline-code")

PIPELINE_SA = "svc-tfm-pipeline-executor@pipeline-weather-flights.iam.gserviceaccount.com"

BATCH_CONFIG = {
    'runtime_config': {
        'version': '1.1'
    },
    'environment_config': {
        'execution_config': {
            'service_account': PIPELINE_SA
        }
    }
}

def log_pipeline_start(**context):
    """
    Log the start of the flights pipeline execution
    Provides visibility into when the pipeline begins processing
    """
    execution_date = context['execution_date']
    logging.info(f"Starting Flights Pipeline execution for {execution_date}")

def log_pipeline_end(**context):
    """
    Log the completion of the flights pipeline execution
    Provides visibility into successful pipeline completion
    """
    execution_date = context['execution_date']
    logging.info(f"Completed Flights Pipeline execution for {execution_date}")

# Pipeline start task - logs the beginning of the workflow
start_task = PythonOperator(
    task_id='start_flights_pipeline',
    python_callable=log_pipeline_start,
    dag=flights_dag,
)

# Main data extraction task - runs the flights extraction script on Dataproc Serverless
# This task:
# 1. Calls OpenSky Network API to get current flight data
# 2. Processes and cleans the flight data using PySpark
# 3. Writes the processed data to BigQuery
flights_batch = DataprocCreateBatchOperator(
    task_id='extract_flights_data',
    region=REGION,
    project_id=PROJECT_ID,
    batch_id="flights-{{ ts_nodash | lower | replace('t','-') | replace('z','') }}",
    batch={
        'pyspark_batch': {
            'main_python_file_uri': f'gs://{BUCKET_NAME}/scripts/extract_flights.py',
            'jar_file_uris': [
                'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar'
            ]
        },
        'runtime_config': BATCH_CONFIG['runtime_config'],
        'environment_config': BATCH_CONFIG['environment_config'],
    },
    dag=flights_dag,
)

# Sensor task - waits for the Dataproc batch job to complete successfully
# This task:
# 1. Monitors the batch job status every 60 seconds (poke_interval)
# 2. Waits up to 30 minutes (timeout) for completion
# 3. Fails the pipeline if the batch job fails or times out
flights_sensor = DataprocBatchSensor(
    task_id='wait_for_flights_completion',
    batch_id="flights-{{ ts_nodash | lower | replace('t','-') | replace('z','') }}",  # Same batch ID as above
    project_id=PROJECT_ID,
    region=REGION,
    poke_interval=60,
    timeout=1800,
    dag=flights_dag,
)

def perform_flights_data_quality_check(**context):
    """
    Perform basic data quality check for flights data
    Validates that we have a minimum number of flight records for the current date
    """
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)

    # Simple count check for flights data
    flights_query = f"""
    SELECT COUNT(*) as record_count
    FROM `{PROJECT_ID}.tfm_bq_dataset.current_flights`
    WHERE extraction_date = CURRENT_DATE()
    """

    flights_result = client.query(flights_query).result()
    flights_count = list(flights_result)[0].record_count

    logging.info(f"Flights records extracted today: {flights_count}")
    
    # Basic quality check - ensure we have minimum flight records
    if flights_count < 10:
        raise ValueError(f"Flights data quality check failed: only {flights_count} records")
    
    logging.info("✅ Flights data quality check passed")

# Data quality validation task - ensures the extracted data meets basic requirements
# This task:
# 1. Queries BigQuery to count today's flight records
# 2. Validates we have at least 10 records
# 3. Fails the pipeline if data quality is insufficient
flights_quality_check = PythonOperator(
    task_id='flights_data_quality_check',
    python_callable=perform_flights_data_quality_check,
    dag=flights_dag,
)

# Pipeline end task - logs successful completion of the workflow
end_task = PythonOperator(
    task_id='end_pipeline',
    python_callable=log_pipeline_end,
    dag=flights_dag,
)

start_task >> flights_batch >> flights_sensor >> flights_quality_check >> end_task
