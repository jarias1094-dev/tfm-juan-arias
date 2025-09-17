# Trabajo Final de Master - Pipeline ETL - Datos de Clima, Vuelos y Geografía

**Autor**: Juan Arias  
**Proyecto**: Trabajo Final de Master  
**Creado**: Enero 2025

## Descripción General

Este pipeline de datos extrae y procesa información de múltiples fuentes:
- **Datos de Vuelos**: API de OpenSky Network
- **Datos del Clima**: API de OpenWeatherMap  
- **Datos de Geografía**: API de GeoDB Cities

## Arquitectura del Pipeline

El sistema consta de cuatro DAGs principales:

### Pipeline Maestro (`master_pipeline_dag.py`)
- **Programación**: Diario a medianoche UTC
- **Propósito**: Orquesta todos los pipelines de datos en secuencia
- **Orden de Ejecución**: Geografía → Clima → Vuelos
- **Características**: Validación simple y logging comprensivo

### Pipelines Individuales de Datos

#### Pipeline de Geografía (`geography_dag.py`)
- **Programación**: Diario
- **Fuente de Datos**: API de GeoDB Cities
- **Salida**: Datos de lugares y países
- **Verificaciones de Calidad**: Validación de conteo de registros, cobertura geográfica

#### Pipeline del Clima (`weather_dag.py`)
- **Programación**: Diario
- **Fuente de Datos**: API de OpenWeatherMap
- **Salida**: Clima actual para aeropuertos
- **Verificaciones de Calidad**: Validación de cobertura de aeropuertos, rangos de temperatura

#### Pipeline de Vuelos (`flights_dag.py`)
- **Programación**: Diario
- **Fuente de Datos**: API de OpenSky Network
- **Salida**: Posiciones y datos actuales de vuelos
- **Verificaciones de Calidad**: Validación de conteo mínimo de registros

## Despliegue en Producción

### Archivos Requeridos para Cloud Composer:

**DAGs** (subir a la carpeta `dags/`):
- `master_pipeline_dag.py` - DAG de orquestación maestro
- `flights_dag.py` - Extracción de datos de vuelos
- `weather_dag.py` - Extracción de datos del clima  
- `geography_dag.py` - Extracción de datos de geografía

**Scripts** (subir al bucket de GCS):
- `scripts/extract_flights.py`
- `scripts/extract_weather.py`
- `scripts/extract_geography.py`

### Tablas de BigQuery Creadas:
- `tfm_bq_dataset.current_flights`
- `tfm_bq_dataset.current_weather` 
- `tfm_bq_dataset.geography_places`
- `tfm_bq_dataset.geography_countries`

### Cuenta de Servicio:
- `svc-tfm-pipeline-executor@pipeline-weather-flights.iam.gserviceaccount.com`

## Stack Técnico

- **Orquestación**: Google Cloud Composer (Airflow)
- **Procesamiento**: Google Dataproc Serverless (PySpark)
- **Almacenamiento**: Google BigQuery
- **Secretos**: Google Secret Manager
- **Monitoreo**: Cloud Logging
- **Control de Versiones**: GitHub
---
*© 2025 Juan Arias - Trabajo Final de Master*