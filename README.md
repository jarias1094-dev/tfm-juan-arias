# Trabajo Final de Master - ETL Pipeline - Weather, Flights & Geography Data

**Author**: Juan Arias  
**Project**: Trabajo Final de Master
**Created**: January 2025

## Overview

This data pipeline extracts and processes data from multiple sources:
- **Flight Data**: OpenSky Network API
- **Weather Data**: OpenWeatherMap API  
- **Geography Data**: GeoDB Cities API

## Pipeline Architecture

The system consists of four main DAGs:

### Master Pipeline (`master_pipeline_dag.py`)
- **Schedule**: Daily at midnight UTC
- **Purpose**: Orchestrates all data pipelines in sequence
- **Execution Order**: Geography → Weather → Flights
- **Features**: Simple validation and comprehensive logging

### Individual Data Pipelines

#### Geography Pipeline (`geography_dag.py`)
- **Schedule**: Daily
- **Data Source**: GeoDB Cities API
- **Output**: Places and countries data
- **Quality Checks**: Record count validation, geographic coverage

#### Weather Pipeline (`weather_dag.py`)
- **Schedule**: Daily
- **Data Source**: OpenWeatherMap API
- **Output**: Current weather for airports
- **Quality Checks**: Airport coverage validation, temperature ranges

#### Flights Pipeline (`flights_dag.py`)
- **Schedule**: Daily
- **Data Source**: OpenSky Network API
- **Output**: Current flight positions and data
- **Quality Checks**: Minimum record count validation

## Production Deployment

### Required Files for Cloud Composer:

**DAGs** (upload to `dags/` folder):
- `master_pipeline_dag.py` - Master orchestration DAG
- `flights_dag.py` - Flight data extraction
- `weather_dag.py` - Weather data extraction  
- `geography_dag.py` - Geography data extraction

**Scripts** (upload to GCS bucket):
- `scripts/extract_flights.py`
- `scripts/extract_weather.py`
- `scripts/extract_geography.py`

**Configuration**:
- `config/config.yaml` - Main configuration file
- `config/production_config.yaml` - Production settings

### BigQuery Tables Created:
- `tfm_bq_dataset.current_flights`
- `tfm_bq_dataset.current_weather` 
- `tfm_bq_dataset.geography_places`
- `tfm_bq_dataset.geography_countries`

### Service Account:
- `svc-tfm-pipeline-executor@pipeline-weather-flights.iam.gserviceaccount.com`

## Technical Stack

- **Orchestration**: Google Cloud Composer (Airflow)
- **Processing**: Google Dataproc Serverless (PySpark)
- **Storage**: Google BigQuery
- **Secrets**: Google Secret Manager
- **Monitoring**: Cloud Logging
- **Version Control**: GitHub

*© 2025 Juan Arias - Trabajo Final de Master*