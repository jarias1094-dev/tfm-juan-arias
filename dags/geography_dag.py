#!/usr/bin/env python3
"""
Airflow DAG for Geography Data
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocGetBatchOperator
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocBatchSensor
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DAG Configuration
DAG_ID = 'geography_data_pipeline'
PROJECT_ID = 'pipeline-weather-flights'
REGION = 'us-central1'
BUCKET_NAME = 'tfm-pipeline-code'

# Default arguments for the DAG
default_args = {
    'owner': 'Juan Arias',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2025, 9, 13),
}

BATCH_CONFIG = {
    'environment_config': {
        'execution_config': {
            'service_account': 'svc-tfm-pipeline-executor@pipeline-weather-flights.iam.gserviceaccount.com',
            'subnetwork_uri': f'projects/{PROJECT_ID}/regions/{REGION}/subnetworks/default'
        }
    },
    'runtime_config': {
        'version': '1.1'
    }
}


def log_pipeline_start(**context):
    """
    Log the start of the geography pipeline execution
    Provides visibility into when the pipeline begins processing
    """
    execution_date = context['execution_date']
    logger.info(f" Starting Geography Data Pipeline execution for {execution_date}")
    logger.info(" Extracting places and regions data using GeoDB API")

def perform_geography_data_quality_check():
    """
    Perform data quality checks on extracted geography data
    Validates that the data extraction was successful by checking record counts
    
    Quality checks performed:
    - Minimum number of place records (100+)
    - Geographic coverage validation
    - Data completeness checks
    """
    from google.cloud import bigquery
    
    logger.info("🔍 Performing geography data quality checks...")
    
    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=PROJECT_ID)
        
        # Check geography_places table
        places_query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT country_code) as unique_countries,
            COUNT(DISTINCT place_type) as unique_place_types
        FROM `{PROJECT_ID}.tfm_bq_dataset.geography_places`
        WHERE DATE(extraction_timestamp) = CURRENT_DATE()
        """
        
        places_results = client.query(places_query).result()
        places_row = next(places_results)
        
        total_records = places_row.total_records
        unique_countries = places_row.unique_countries
        unique_place_types = places_row.unique_place_types
        
        logger.info(f"Geography Quality Check Results:")
        logger.info(f"   Total place records: {total_records}")
        logger.info(f"   Unique countries: {unique_countries}")
        logger.info(f"   Unique place types: {unique_place_types}")
        
        # Quality check thresholds
        if total_records < 100:
            raise ValueError(f"Insufficient geography records: {total_records} (minimum: 100)")
        
        if unique_countries < 10:
            raise ValueError(f"Insufficient country coverage: {unique_countries} (minimum: 10)")
        
        logger.info("✅ All geography data quality checks passed!")
        return "geography_quality_check_passed"
        
    except Exception as e:
        logger.error(f"Geography data quality check failed: {e}")
        raise

def log_pipeline_end(**context):
    """
    Log the completion of the geography pipeline execution
    Provides visibility into successful pipeline completion
    """
    execution_date = context['execution_date']
    logger.info(f"🎉 Geography Data Pipeline execution completed for {execution_date}")
    logger.info("📊 Geography data has been extracted and stored in BigQuery")

# Create the DAG
geography_dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Extract and process geography data using GeoDB Cities API',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['geography', 'geodb', 'api', 'bigquery', 'dataproc']
)

# Define tasks
start_task = PythonOperator(
    task_id='log_pipeline_start',
    python_callable=log_pipeline_start,
    dag=geography_dag
)

# Dataproc Batch for geography extraction
geography_batch = DataprocCreateBatchOperator(
    task_id='extract_geography_data',
    region=REGION,
    project_id=PROJECT_ID,
    batch_id="geography-{{ ts_nodash | lower | replace('t','-') | replace('z','') }}",
    batch={
        'pyspark_batch': {
            'main_python_file_uri': f'gs://{BUCKET_NAME}/scripts/extract_geography.py',
            'jar_file_uris': [
                'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar'
            ]
        },
        'environment_config': BATCH_CONFIG['environment_config'],
        'runtime_config': BATCH_CONFIG['runtime_config']
    },
    timeout=1800,
    impersonation_chain="svc-tfm-pipeline-executor@pipeline-weather-flights.iam.gserviceaccount.com",
    dag=geography_dag
)

# Sensor to wait for batch completion
geography_sensor = DataprocBatchSensor(
    task_id='wait_for_geography_batch',
    region=REGION,
    project_id=PROJECT_ID,
    batch_id="geography-{{ ts_nodash | lower | replace('t','-') | replace('z','') }}",
    timeout=1800,
    poke_interval=60,
    dag=geography_dag
)

# Data quality check task
data_quality_check = PythonOperator(
    task_id='geography_data_quality_check',
    python_callable=perform_geography_data_quality_check,
    dag=geography_dag
)

# End task
end_task = PythonOperator(
    task_id='log_pipeline_end',
    python_callable=log_pipeline_end,
    dag=geography_dag
)

start_task >> geography_batch >> geography_sensor >> data_quality_check >> end_task
