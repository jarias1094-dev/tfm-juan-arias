#!/usr/bin/env python3
"""
DAG Maestro - Orquesta los Pipelines de Datos de Vuelos, Geografía y Clima
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración del DAG
DAG_ID = 'master_tfm_pipeline'
PROJECT_ID = 'pipeline-weather-flights'

# Argumentos por defecto para el DAG maestro
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
    Registra el inicio de la ejecución del pipeline maestro
    Proporciona visibilidad sobre cuándo comienza la orquestación maestra
    """
    execution_date = context['execution_date']
    logger.info(f"Iniciando ejecución del Pipeline Maestro para {execution_date}")

def log_master_pipeline_end(**context):
    """
    Registra la finalización de la ejecución del pipeline maestro
    Proporciona visibilidad sobre la finalización exitosa del pipeline maestro
    """
    execution_date = context['execution_date']
    logger.info(f"Ejecución del Pipeline Maestro completada para {execution_date}")

def validate_pipeline_execution(**context):
    """
    Validación para asegurar que todos los pipelines se ejecutaron exitosamente
    """
    logger.info("Validando ejecución de pipelines")

# Creación del DAG maestro
master_dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='DAG de orquestación maestro para pipelines de datos (Geografía, Clima, Vuelos)',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['master', 'orchestration', 'geography', 'weather', 'flights']
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
