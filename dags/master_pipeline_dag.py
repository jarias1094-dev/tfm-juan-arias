#!/usr/bin/env python3
"""
Master DAG for TFM Pipeline - Orchestrates Flights, Geography, and Weather Data Pipelines
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DAG Configuration
DAG_ID = 'master_tfm_pipeline'
PROJECT_ID = 'pipeline-weather-flights'

# Default arguments for the master DAG
default_args = {
    'owner': 'Juan Arias',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'start_date': datetime(2024, 1, 1),
}

def log_master_pipeline_start(**context):
    """
    Log the start of the master pipeline execution
    Provides visibility into when the master orchestration begins
    """
    execution_date = context['execution_date']
    logger.info(f"🚀 Starting Master TFM Pipeline execution for {execution_date}")
    logger.info("📊 Orchestrating Flights, Geography, and Weather data pipelines")
    logger.info("🔄 Pipeline execution order: Geography → Weather → Flights")

def log_master_pipeline_end(**context):
    """
    Log the completion of the master pipeline execution
    Provides visibility into successful master pipeline completion
    """
    execution_date = context['execution_date']
    logger.info(f"✅ Master TFM Pipeline execution completed for {execution_date}")
    logger.info("🎉 All data pipelines (Geography, Weather, Flights) have been successfully executed")

def monitor_pipeline_health(**context):
    """
    Monitor the overall health of all data pipelines
    Performs basic checks to ensure all pipelines are functioning correctly
    """
    from google.cloud import bigquery
    
    logger.info("🔍 Performing master pipeline health check...")
    
    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=PROJECT_ID)
        
        # Check if all required tables have recent data
        health_checks = {
            'geography': f"""
                SELECT COUNT(*) as record_count
                FROM `{PROJECT_ID}.tfm_bq_dataset.geography_places`
                WHERE DATE(extraction_timestamp) = CURRENT_DATE()
            """,
            'weather': f"""
                SELECT COUNT(*) as record_count
                FROM `{PROJECT_ID}.tfm_bq_dataset.current_weather`
                WHERE extraction_date = CURRENT_DATE()
            """,
            'flights': f"""
                SELECT COUNT(*) as record_count
                FROM `{PROJECT_ID}.tfm_bq_dataset.current_flights`
                WHERE extraction_date = CURRENT_DATE()
            """
        }
        
        health_results = {}
        for pipeline, query in health_checks.items():
            result = client.query(query).result()
            count = list(result)[0].record_count
            health_results[pipeline] = count
            logger.info(f"  {pipeline.capitalize()} pipeline: {count} records")
        
        # Overall health assessment
        total_records = sum(health_results.values())
        logger.info(f"📊 Total records across all pipelines: {total_records}")
        
        if total_records < 100:
            logger.warning(f"⚠️  Low total record count: {total_records}")
        else:
            logger.info("✅ Master pipeline health check passed")
        
        return health_results
        
    except Exception as e:
        logger.error(f"❌ Master pipeline health check failed: {e}")
        raise

# Create the master DAG
master_dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Master orchestration DAG for TFM data pipelines (Geography, Weather, Flights)',
    schedule_interval='@daily',  # Run once daily
    catchup=False,
    max_active_runs=1,
    tags=['master', 'orchestration', 'tfm', 'geography', 'weather', 'flights']
)

# Master pipeline start task
master_start = PythonOperator(
    task_id='master_pipeline_start',
    python_callable=log_master_pipeline_start,
    dag=master_dag
)

# Trigger Geography Pipeline
trigger_geography = TriggerDagRunOperator(
    task_id='trigger_geography_pipeline',
    trigger_dag_id='geography_data_pipeline',
    wait_for_completion=True,
    poke_interval=60,
    dag=master_dag
)

# Trigger Weather Pipeline (depends on geography completion)
trigger_weather = TriggerDagRunOperator(
    task_id='trigger_weather_pipeline',
    trigger_dag_id='weather_data_pipeline',
    wait_for_completion=True,
    poke_interval=60,
    dag=master_dag
)

# Trigger Flights Pipeline (depends on weather completion)
trigger_flights = TriggerDagRunOperator(
    task_id='trigger_flights_pipeline',
    trigger_dag_id='flights_data_pipeline',
    wait_for_completion=True,
    poke_interval=60,
    dag=master_dag
)

# Pipeline health monitoring task
pipeline_health_check = PythonOperator(
    task_id='monitor_pipeline_health',
    python_callable=monitor_pipeline_health,
    dag=master_dag
)

# Master pipeline end task
master_end = PythonOperator(
    task_id='master_pipeline_end',
    python_callable=log_master_pipeline_end,
    dag=master_dag
)

# Define task dependencies - Sequential execution
# Geography → Weather → Flights → Health Check → End
master_start >> trigger_geography >> trigger_weather >> trigger_flights >> pipeline_health_check >> master_end
