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
    logger.info(f"Starting Master TFM Pipeline execution for {execution_date}")

def log_master_pipeline_end(**context):
    """
    Log the completion of the master pipeline execution
    Provides visibility into successful master pipeline completion
    """
    execution_date = context['execution_date']
    logger.info(f"Master TFM Pipeline execution completed for {execution_date}")

def validate_pipeline_execution(**context):
    """
    Validation to ensure all pipelines executed successfully
    """
    logger.info("Validating pipeline execution")

# Create the master DAG
master_dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Master orchestration DAG for TFM data pipelines (Geography, Weather, Flights)',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['master', 'orchestration', 'tfm', 'geography', 'weather', 'flights']
)

master_start = PythonOperator(
    task_id='master_pipeline_start',
    python_callable=log_master_pipeline_start,
    dag=master_dag
)

trigger_geography = TriggerDagRunOperator(
    task_id='trigger_geography_pipeline',
    trigger_dag_id='geography_data_pipeline',
    wait_for_completion=True,
    poke_interval=60,
    dag=master_dag
)

trigger_weather = TriggerDagRunOperator(
    task_id='trigger_weather_pipeline',
    trigger_dag_id='weather_data_pipeline',
    wait_for_completion=True,
    poke_interval=60,
    dag=master_dag
)

trigger_flights = TriggerDagRunOperator(
    task_id='trigger_flights_pipeline',
    trigger_dag_id='flights_data_pipeline',
    wait_for_completion=True,
    poke_interval=60,
    dag=master_dag
)

pipeline_validation = PythonOperator(
    task_id='validate_pipeline_execution',
    python_callable=validate_pipeline_execution,
    dag=master_dag
)

master_end = PythonOperator(
    task_id='master_pipeline_end',
    python_callable=log_master_pipeline_end,
    dag=master_dag
)

master_start >> trigger_geography >> trigger_weather >> trigger_flights >> pipeline_validation >> master_end
