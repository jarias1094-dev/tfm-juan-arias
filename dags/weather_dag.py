"""
Airflow DAG for Weather Data
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

# Weather DAG definition
weather_dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Weather data extraction pipeline using OpenWeatherMap API for all airports',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'openweathermap', 'airports', 'bigquery'],
)

# Get configuration from Airflow Variables
PROJECT_ID = Variable.get("gcp_project_id", default_var="pipeline-weather-flights")
REGION = Variable.get("gcp_region", default_var="us-central1")
BUCKET_NAME = Variable.get("storage_bucket", default_var="graduation-pipeline-code")

# Service account for Dataproc batches
PIPELINE_SA = "svc-tfm-pipeline-executor@pipeline-weather-flights.iam.gserviceaccount.com"

# Dataproc Serverless configuration
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

def log_weather_pipeline_start(**context):
    """
    Log the start of the weather pipeline execution
    Provides visibility into when the weather pipeline begins processing
    """
    execution_date = context['execution_date']
    logging.info(f"Starting Weather Pipeline execution for {execution_date}")

def log_weather_pipeline_end(**context):
    """
    Log the completion of the weather pipeline execution
    Provides visibility into successful weather pipeline completion
    """
    execution_date = context['execution_date']
    logging.info(f"Weather Pipeline execution completed for {execution_date}")

# Pipeline start task - logs the beginning of the weather workflow
start_task = PythonOperator(
    task_id='start_weather_pipeline',
    python_callable=log_weather_pipeline_start,
    dag=weather_dag,
)

# Main weather data extraction task - runs the weather extraction script on Dataproc Serverless
# This task:
# 1. Reads airports from BigQuery airports table
# 2. Calls OpenWeatherMap API to get current weather for each airport
# 3. Processes and transforms the weather data using PySpark
# 4. Writes the processed weather data to BigQuery
weather_batch = DataprocCreateBatchOperator(
    task_id='extract_weather_data',
    region=REGION,
    project_id=PROJECT_ID,
    batch_id="weather-{{ ts_nodash | lower | replace('t','-') | replace('z','') }}",
    batch={
        'pyspark_batch': {
            'main_python_file_uri': f'gs://{BUCKET_NAME}/scripts/extract_weather.py',
            'jar_file_uris': [
                'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar'
            ]
        },
        'runtime_config': BATCH_CONFIG['runtime_config'],
        'environment_config': BATCH_CONFIG['environment_config'],
    },
    dag=weather_dag,
)

# Sensor task - waits for the weather Dataproc batch job to complete successfully
# This task:
# 1. Monitors the weather batch job status every 60 seconds (poke_interval)
# 2. Waits up to 30 minutes (timeout) for completion (longer than flights due to more API calls)
# 3. Fails the pipeline if the batch job fails or times out
weather_sensor = DataprocBatchSensor(
    task_id='wait_for_weather_completion',
    batch_id="weather-{{ ts_nodash | lower | replace('t','-') | replace('z','') }}",
    project_id=PROJECT_ID,
    region=REGION,
    poke_interval=60,
    timeout=1800,
    dag=weather_dag,
)

def perform_weather_data_quality_check(**context):
    """
    Perform basic data quality check for weather data
    Validates that we have weather records for a reasonable number of airports
    """
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)

    # Check weather data count and basic statistics
    weather_query = f"""
    SELECT 
        COUNT(*) as record_count,
        COUNT(DISTINCT airport_iata_code) as unique_airports,
        AVG(temperature) as avg_temperature
    FROM `{PROJECT_ID}.tfm_bq_dataset.current_weather`
    WHERE extraction_date = CURRENT_DATE()
    """

    weather_result = client.query(weather_query).result()
    row = list(weather_result)[0]
    
    weather_count = row.record_count
    unique_airports = row.unique_airports
    avg_temperature = row.avg_temperature or 0

    logging.info(f"Weather Quality Check Results:")
    logging.info(f"  Total records: {weather_count}")
    logging.info(f"  Unique airports: {unique_airports}")
    logging.info(f"  Average temperature: {avg_temperature:.2f}°C")
    
    # Basic quality checks
    if weather_count < 50:
        raise ValueError(f"Weather data quality check failed: only {weather_count} records (expected at least 50)")
    
    if unique_airports < 20:
        raise ValueError(f"Weather data quality check failed: only {unique_airports} unique airports (expected at least 20)")
    
    logging.info(" Weather data quality check passed")

# Data quality validation task - ensures the extracted weather data meets basic requirements
# This task:
# 1. Queries BigQuery to count today's weather records
# 2. Validates we have weather data for a reasonable number of airports
# 3. Fails the pipeline if data quality is insufficient
weather_quality_check = PythonOperator(
    task_id='weather_data_quality_check',
    python_callable=perform_weather_data_quality_check,
    dag=weather_dag,
)

# Pipeline end task - logs successful completion of the weather workflow
end_task = PythonOperator(
    task_id='end_pipeline',
    python_callable=log_weather_pipeline_end,
    dag=weather_dag,
)

start_task >> weather_batch >> weather_sensor >> weather_quality_check >> end_task
