"""
DAG de Airflow para Datos del Clima
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

# Definición del DAG del clima
weather_dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Pipeline de extracción de datos del clima usando la API de OpenWeatherMap para todos los aeropuertos',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'openweathermap', 'airports', 'bigquery'],
)

# Obtener configuración desde Variables de Airflow
PROJECT_ID = Variable.get("gcp_project_id", default_var="pipeline-weather-flights")
REGION = Variable.get("gcp_region", default_var="us-central1")
BUCKET_NAME = Variable.get("storage_bucket", default_var="tfm-pipeline-code")

# Cuenta de servicio para lotes de Dataproc
PIPELINE_SA = "svc-tfm-pipeline-executor@pipeline-weather-flights.iam.gserviceaccount.com"

# Configuración de Dataproc Serverless
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

def log_weather_pipeline_start(**context):
    """
    Registra el inicio de la ejecución del pipeline del clima
    Proporciona visibilidad sobre cuándo el pipeline del clima comienza a procesar
    """
    execution_date = context['execution_date']
    logging.info(f"Iniciando ejecución del Pipeline del Clima para {execution_date}")

def log_weather_pipeline_end(**context):
    """
    Registra la finalización de la ejecución del pipeline del clima
    Proporciona visibilidad sobre la finalización exitosa del pipeline del clima
    """
    execution_date = context['execution_date']
    logging.info(f"Ejecución del Pipeline del Clima completada para {execution_date}")

# Tarea de inicio del pipeline - registra el comienzo del flujo de trabajo del clima
start_task = PythonOperator(
    task_id='start_weather_pipeline',
    python_callable=log_weather_pipeline_start,
    dag=weather_dag,
)

# Tarea principal de extracción de datos del clima - ejecuta el script de extracción del clima en Dataproc Serverless
# Esta tarea:
# 1. Lee aeropuertos desde la tabla de aeropuertos de BigQuery
# 2. Llama a la API de OpenWeatherMap para obtener el clima actual para cada aeropuerto
# 3. Procesa y transforma los datos del clima usando PySpark
# 4. Escribe los datos del clima procesados a BigQuery
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

# Tarea sensor - espera a que el trabajo batch del clima de Dataproc se complete exitosamente
# Esta tarea:
# 1. Monitorea el estado del trabajo batch del clima cada 60 segundos (poke_interval)
# 2. Espera hasta 30 minutos (timeout) para la finalización (más tiempo que vuelos debido a más llamadas API)
# 3. Falla el pipeline si el trabajo batch falla o se agota el tiempo
weather_sensor = DataprocBatchSensor(
    task_id='wait_for_weather_completion',
    batch_id="weather-{{ ts_nodash | lower | replace('t','-') | replace('z','') }}",
    project_id=PROJECT_ID,
    region=REGION,
    poke_interval=30,
    timeout=900,
    dag=weather_dag,
)

def perform_weather_data_quality_check(**context):
    """
    Realiza verificación básica de calidad de datos para datos del clima
    Valida que tenemos registros del clima para un número razonable de aeropuertos
    """
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)

    # Verificar conteo de datos del clima y estadísticas básicas
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

    logging.info(f"Resultados de Verificación de Calidad del Clima:")
    logging.info(f"  Total de registros: {weather_count}")
    logging.info(f"  Aeropuertos únicos: {unique_airports}")
    logging.info(f"  Temperatura promedio: {avg_temperature:.2f}°C")
    
    # Verificaciones básicas de calidad
    if weather_count < 50:
        raise ValueError(f"Verificación de calidad de datos del clima falló: solo {weather_count} registros (esperado al menos 50)")
    
    if unique_airports < 20:
        raise ValueError(f"Verificación de calidad de datos del clima falló: solo {unique_airports} aeropuertos únicos (esperado al menos 20)")
    
    logging.info("Verificación de calidad de datos del clima pasó exitosamente")

# Tarea de validación de calidad de datos - asegura que los datos del clima extraídos cumplan los requisitos básicos
# Esta tarea:
# 1. Consulta BigQuery para contar los registros del clima de hoy
# 2. Valida que tenemos datos del clima para un número razonable de aeropuertos
# 3. Falla el pipeline si la calidad de datos es insuficiente
weather_quality_check = PythonOperator(
    task_id='weather_data_quality_check',
    python_callable=perform_weather_data_quality_check,
    dag=weather_dag,
)

# Tarea de finalización del pipeline - registra la finalización exitosa del flujo de trabajo del clima
end_task = PythonOperator(
    task_id='end_pipeline',
    python_callable=log_weather_pipeline_end,
    dag=weather_dag,
)

start_task >> weather_batch >> weather_sensor >> weather_quality_check >> end_task
