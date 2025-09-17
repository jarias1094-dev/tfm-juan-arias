#!/usr/bin/env python3
"""
DAG de Airflow para Datos de Geografía
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

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración del DAG
DAG_ID = 'geography_data_pipeline'
PROJECT_ID = 'pipeline-weather-flights'
REGION = 'us-central1'
BUCKET_NAME = 'tfm-pipeline-code'

# Argumentos por defecto para el DAG
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
        'version': '1.1',
        'properties': {
            'spark.executor.instances': '1',
            'spark.executor.cores': '1',
            'spark.executor.memory': '2g',
            'spark.driver.memory': '1g',
            'spark.driver.cores': '1'
        }
    }
}


def log_pipeline_start(**context):
    """
    Registra el inicio de la ejecución del pipeline de geografía
    Proporciona visibilidad sobre cuándo el pipeline comienza a procesar
    """
    execution_date = context['execution_date']
    logger.info(f"Iniciando ejecución del Pipeline de Datos de Geografía para {execution_date}")
    logger.info("Extrayendo datos de lugares y regiones usando la API de GeoDB")

def perform_geography_data_quality_check():
    """
    Realiza verificaciones de calidad de datos en los datos de geografía extraídos
    Valida que la extracción de datos fue exitosa verificando conteos de registros
    
    Verificaciones de calidad realizadas:
    - Número mínimo de registros de lugares (100+)
    - Validación de cobertura geográfica
    - Verificaciones de completitud de datos
    """
    from google.cloud import bigquery
    
    logger.info("Realizando verificaciones de calidad de datos de geografía...")
    
    try:
        # Inicializar cliente de BigQuery
        client = bigquery.Client(project=PROJECT_ID)
        
        # Verificar tabla geography_places
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
        
        logger.info(f"Resultados de Verificación de Calidad de Geografía:")
        logger.info(f"   Total de registros de lugares: {total_records}")
        logger.info(f"   Países únicos: {unique_countries}")
        logger.info(f"   Tipos de lugares únicos: {unique_place_types}")
        
        # Umbrales de verificación de calidad
        if total_records < 100:
            raise ValueError(f"Registros de geografía insuficientes: {total_records} (mínimo: 100)")
        
        if unique_countries < 10:
            raise ValueError(f"Cobertura de países insuficiente: {unique_countries} (mínimo: 10)")
        
        logger.info("Todas las verificaciones de calidad de datos de geografía pasaron exitosamente!")
        return "geography_quality_check_passed"
        
    except Exception as e:
        logger.error(f"Verificación de calidad de datos de geografía falló: {e}")
        raise

def log_pipeline_end(**context):
    """
    Registra la finalización de la ejecución del pipeline de geografía
    Proporciona visibilidad sobre la finalización exitosa del pipeline
    """
    execution_date = context['execution_date']
    logger.info(f"Ejecución del Pipeline de Datos de Geografía completada para {execution_date}")
    logger.info("Los datos de geografía han sido extraídos y almacenados en BigQuery")

# Crear el DAG
geography_dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Extraer y procesar datos de geografía usando la API de GeoDB Cities',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['geography', 'geodb', 'api', 'bigquery', 'dataproc']
)

# Definir tareas
start_task = PythonOperator(
    task_id='log_pipeline_start',
    python_callable=log_pipeline_start,
    dag=geography_dag
)

# Lote de Dataproc para extracción de geografía
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

# Sensor para esperar la finalización del lote
geography_sensor = DataprocBatchSensor(
    task_id='wait_for_geography_batch',
    region=REGION,
    project_id=PROJECT_ID,
    batch_id="geography-{{ ts_nodash | lower | replace('t','-') | replace('z','') }}",
    timeout=900,
    poke_interval=30,
    dag=geography_dag
)

# Tarea de verificación de calidad de datos
data_quality_check = PythonOperator(
    task_id='geography_data_quality_check',
    python_callable=perform_geography_data_quality_check,
    dag=geography_dag
)

# Tarea de finalización
end_task = PythonOperator(
    task_id='log_pipeline_end',
    python_callable=log_pipeline_end,
    dag=geography_dag
)

start_task >> geography_batch >> geography_sensor >> data_quality_check >> end_task
