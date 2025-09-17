"""
DAG de Airflow para Datos de Vuelos
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.dataproc import DataprocBatchSensor
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'Juan Arias',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG de vuelos
flights_dag = DAG(
    'flights_data_pipeline',
    default_args=default_args,
    description='Pipeline de extracción de datos de vuelos usando la API de OpenSky Network',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['flights', 'opensky', 'real-time', 'bigquery'],
)

# Obtener configuración desde Variables de Airflow
PROJECT_ID = Variable.get("gcp_project_id", default_var="pipeline-weather-flights")
REGION = Variable.get("gcp_region", default_var="us-central1")
BUCKET_NAME = Variable.get("storage_bucket", default_var="tfm-pipeline-code")

# Usado para impersonación
PIPELINE_SA = "svc-tfm-pipeline-executor@pipeline-weather-flights.iam.gserviceaccount.com"

BATCH_CONFIG = {
    'runtime_config': {
        'version': '1.1',
        'properties': {
            'spark.executor.instances': '2',
            'spark.executor.cores': '4',
            'spark.executor.memory': '4g',
            'spark.driver.memory': '4g',
            'spark.driver.cores': '4'
        }
    },
    'environment_config': {
        'execution_config': {
            'service_account': PIPELINE_SA
        }
    }
}

def log_pipeline_start(**context):
    """
    Registra el inicio de la ejecución del pipeline de vuelos
    Proporciona visibilidad sobre cuándo el pipeline comienza a procesar
    """
    execution_date = context['execution_date']
    logging.info(f"Iniciando ejecución del Pipeline de Vuelos para {execution_date}")

def log_pipeline_end(**context):
    """
    Registra la finalización de la ejecución del pipeline de vuelos
    Proporciona visibilidad sobre la finalización exitosa del pipeline
    """
    execution_date = context['execution_date']
    logging.info(f"Ejecución del Pipeline de Vuelos completada para {execution_date}")

# Tarea de inicio del pipeline - registra el comienzo del flujo de trabajo
start_task = PythonOperator(
    task_id='start_flights_pipeline',
    python_callable=log_pipeline_start,
    dag=flights_dag,
)

# Tarea principal de extracción de datos - ejecuta el script de extracción de vuelos en Dataproc Serverless
# Esta tarea:
# 1. Llama a la API de OpenSky Network para obtener datos actuales de vuelos
# 2. Procesa y limpia los datos de vuelos usando PySpark
# 3. Escribe los datos procesados a BigQuery
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

# Tarea sensor - espera a que el trabajo batch de Dataproc se complete exitosamente
# Esta tarea:
# 1. Monitorea el estado del trabajo batch cada 60 segundos (poke_interval)
# 2. Espera hasta 30 minutos (timeout) para la finalización
# 3. Falla el pipeline si el trabajo batch falla o se agota el tiempo
flights_sensor = DataprocBatchSensor(
    task_id='wait_for_flights_completion',
    batch_id="flights-{{ ts_nodash | lower | replace('t','-') | replace('z','') }}",
    project_id=PROJECT_ID,
    region=REGION,
    poke_interval=30,
    timeout=900,
    dag=flights_dag,
)

def perform_flights_data_quality_check(**context):
    """
    Realiza verificación de calidad de datos para datos de vuelos
    Valida que tenemos un número mínimo de registros de vuelos para la fecha actual
    """
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)

    flights_query = f"""
    SELECT COUNT(*) as record_count
    FROM `{PROJECT_ID}.tfm_bq_dataset.current_flights`
    WHERE extraction_date = CURRENT_DATE()
    """

    flights_result = client.query(flights_query).result()
    flights_count = list(flights_result)[0].record_count

    logging.info(f"Registros de vuelos extraídos hoy: {flights_count}")
    
    # Verificación de calidad - asegurar que tenemos registros mínimos de vuelos
    if flights_count < 10:
        raise ValueError(f"Verificación de calidad de datos de vuelos falló: solo {flights_count} registros")
    
    logging.info("Verificación de calidad de datos de vuelos pasó exitosamente")

# Tarea de validación de calidad de datos - asegura que los datos extraídos cumplan los requisitos
# Esta tarea:
# 1. Consulta BigQuery para contar los registros de vuelos de hoy
# 2. Valida que tenemos al menos 10 registros
# 3. Falla el pipeline si la calidad de datos es insuficiente
flights_quality_check = PythonOperator(
    task_id='flights_data_quality_check',
    python_callable=perform_flights_data_quality_check,
    dag=flights_dag,
)

# Tarea de finalización del pipeline - registra la finalización exitosa del flujo de trabajo
end_task = PythonOperator(
    task_id='end_pipeline',
    python_callable=log_pipeline_end,
    dag=flights_dag,
)

start_task >> flights_batch >> flights_sensor >> flights_quality_check >> end_task
