"""
Cloud Composer DAG for Graduation Pipeline
Orchestrates the execution of weather, flights, and geography data extraction
Configured for your GCP infrastructure
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.dataproc import DataprocBatchSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'Juan Arias',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['your-email@example.com'],  # Update with your email
}

# DAG definition
dag = DAG(
    'graduation_pipeline_production',
    default_args=default_args,
    description='Production pipeline for weather, flights, and geography data extraction',
    schedule_interval='@daily',  # Run once daily at midnight UTC
    catchup=False,
    max_active_runs=1,
    tags=['data-pipeline', 'api-extraction', 'bigquery', 'production'],
)

# Configuration for your infrastructure
PROJECT_ID = "pipeline-weather-flights"
REGION = "us-central1"
BUCKET_NAME = "pipeline-weather-flights-code"  # Update with your actual bucket name
SERVICE_ACCOUNT = "svc-tfm-pipeline-executor@pipeline-weather-flights.iam.gserviceaccount.com"

# Common configuration for all batch jobs
BATCH_CONFIG = {
    'project_id': PROJECT_ID,
    'region': REGION,
    'pyspark_batch': {
        'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
    },
    'environment_config': {
        'execution_config': {
            'service_account': SERVICE_ACCOUNT,
        }
    }
}

def log_pipeline_start(**context):
    """Log the start of the pipeline execution"""
    execution_date = context['execution_date']
    logging.info(f"Starting Graduation Pipeline execution for {execution_date}")
    logging.info(f"Project: {PROJECT_ID}, Bucket: {BUCKET_NAME}")

def log_pipeline_end(**context):
    """Log the end of the pipeline execution"""
    execution_date = context['execution_date']
    logging.info(f"Completed Graduation Pipeline execution for {execution_date}")

def verify_secrets(**context):
    """Verify that required secrets are available"""
    try:
        opensky_client_id = Variable.get("OPENSKY_CLIENT_ID")
        opensky_client_secret = Variable.get("OPENSKY_CLIENT_SECRET")
        
        if not opensky_client_id or not opensky_client_secret:
            raise ValueError("OpenSky credentials not found in Cloud Composer secrets")
        
        logging.info("✅ OpenSky credentials verified")
        return True
        
    except Exception as e:
        logging.error(f"❌ Secret verification failed: {e}")
        raise

def perform_data_quality_check(**context):
    """Perform basic data quality checks"""
    from google.cloud import bigquery
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # Check weather data
    weather_query = f"""
    SELECT COUNT(*) as record_count
    FROM `{PROJECT_ID}.pipeline-weather-flights.current_weather`
    WHERE extraction_date = CURRENT_DATE()
    """
    
    try:
        weather_result = client.query(weather_query).result()
        weather_count = list(weather_result)[0].record_count
        logging.info(f"Weather records today: {weather_count}")
    except Exception as e:
        logging.warning(f"Could not check weather data: {e}")
        weather_count = 0
    
    # Check flights data
    flights_query = f"""
    SELECT COUNT(*) as record_count
    FROM `{PROJECT_ID}.pipeline-weather-flights.current_flights`
    WHERE extraction_date = CURRENT_DATE()
    """
    
    try:
        flights_result = client.query(flights_query).result()
        flights_count = list(flights_result)[0].record_count
        logging.info(f"Flights records today: {flights_count}")
    except Exception as e:
        logging.warning(f"Could not check flights data: {e}")
        flights_count = 0
    
    # Basic quality checks
    if weather_count < 5:
        logging.warning(f"Weather data quality check: only {weather_count} records")
    
    if flights_count < 100:
        logging.warning(f"Flights data quality check: only {flights_count} records")
    
    logging.info("Data quality checks completed")

# Start task
start_task = PythonOperator(
    task_id='start_pipeline',
    python_callable=log_pipeline_start,
    dag=dag,
)

# Verify secrets task
verify_secrets_task = PythonOperator(
    task_id='verify_secrets',
    python_callable=verify_secrets,
    dag=dag,
)

# Weather data extraction
weather_batch = DataprocCreateBatchOperator(
    task_id='extract_weather_data',
    batch_id='weather-extraction-{{ ds_nodash }}-{{ ts_nodash }}',
    pyspark_batch={
        **BATCH_CONFIG['pyspark_batch'],
        'main_python_file_uri': f'gs://{BUCKET_NAME}/scripts/extract_weather.py',
    },
    **{k: v for k, v in BATCH_CONFIG.items() if k != 'pyspark_batch'},
    dag=dag,
)

# Wait for weather batch to complete
weather_sensor = DataprocBatchSensor(
    task_id='wait_for_weather_completion',
    batch_id='weather-extraction-{{ ds_nodash }}-{{ ts_nodash }}',
    project_id=PROJECT_ID,
    region=REGION,
    poke_interval=60,  # Check every minute
    timeout=1800,  # 30 minutes timeout
    dag=dag,
)

# Flights data extraction
flights_batch = DataprocCreateBatchOperator(
    task_id='extract_flights_data',
    batch_id='flights-extraction-{{ ds_nodash }}-{{ ts_nodash }}',
    pyspark_batch={
        **BATCH_CONFIG['pyspark_batch'],
        'main_python_file_uri': f'gs://{BUCKET_NAME}/scripts/extract_flights.py',
    },
    **{k: v for k, v in BATCH_CONFIG.items() if k != 'pyspark_batch'},
    dag=dag,
)

# Wait for flights batch to complete
flights_sensor = DataprocBatchSensor(
    task_id='wait_for_flights_completion',
    batch_id='flights-extraction-{{ ds_nodash }}-{{ ts_nodash }}',
    project_id=PROJECT_ID,
    region=REGION,
    poke_interval=60,
    timeout=1800,
    dag=dag,
)

# Geography data extraction (runs less frequently)
geography_batch = DataprocCreateBatchOperator(
    task_id='extract_geography_data',
    batch_id='geography-extraction-{{ ds_nodash }}-{{ ts_nodash }}',
    pyspark_batch={
        **BATCH_CONFIG['pyspark_batch'],
        'main_python_file_uri': f'gs://{BUCKET_NAME}/scripts/extract_geography_simple.py',
    },
    **{k: v for k, v in BATCH_CONFIG.items() if k != 'pyspark_batch'},
    dag=dag,
)

# Wait for geography batch to complete
geography_sensor = DataprocBatchSensor(
    task_id='wait_for_geography_completion',
    batch_id='geography-extraction-{{ ds_nodash }}-{{ ts_nodash }}',
    project_id=PROJECT_ID,
    region=REGION,
    poke_interval=60,
    timeout=1800,
    dag=dag,
)

# Data quality check task
data_quality_check = PythonOperator(
    task_id='data_quality_check',
    python_callable=perform_data_quality_check,
    dag=dag,
)

# End task
end_task = PythonOperator(
    task_id='end_pipeline',
    python_callable=log_pipeline_end,
    dag=dag,
)

# Define task dependencies
start_task >> verify_secrets_task >> [weather_batch, flights_batch, geography_batch]

weather_batch >> weather_sensor
flights_batch >> flights_sensor
geography_batch >> geography_sensor

[weather_sensor, flights_sensor, geography_sensor] >> data_quality_check >> end_task

# Alternative DAG for daily geography extraction only
geography_daily_dag = DAG(
    'graduation_pipeline_geography_daily',
    default_args={
        **default_args,
        'schedule_interval': '@daily',  # Run daily
    },
    description='Daily geography data extraction',
    catchup=False,
    max_active_runs=1,
    tags=['data-pipeline', 'geography', 'daily'],
)

# Daily geography extraction
daily_geography_batch = DataprocCreateBatchOperator(
    task_id='extract_geography_data_daily',
    batch_id='geography-daily-{{ ds_nodash }}',
    pyspark_batch={
        **BATCH_CONFIG['pyspark_batch'],
        'main_python_file_uri': f'gs://{BUCKET_NAME}/scripts/extract_geography_simple.py',
    },
    **{k: v for k, v in BATCH_CONFIG.items() if k != 'pyspark_batch'},
    dag=geography_daily_dag,
)

# Wait for daily geography batch to complete
daily_geography_sensor = DataprocBatchSensor(
    task_id='wait_for_geography_daily_completion',
    batch_id='geography-daily-{{ ds_nodash }}',
    project_id=PROJECT_ID,
    region=REGION,
    poke_interval=60,
    timeout=1800,
    dag=geography_daily_dag,
)

daily_geography_batch >> daily_geography_sensor
