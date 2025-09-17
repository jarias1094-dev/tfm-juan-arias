# TFM Pipeline - Weather & Flights Data

**Author**: Juan Arias  
**Project**: Master's Thesis Final Project  
**Created**: September 2025

## Overview

This pipeline extracts and processes data from multiple sources:
- ✈️ **Flight Data**: OpenSky Network API
- 🌤️ **Weather Data**: OpenWeatherMap API  
- 🌍 **Geography Data**: GeoDB Cities API

## Production Deployment

### Required Files for Cloud Composer:

**DAGs** (upload to `dags/` folder):
- `flights_dag.py` - Flight data extraction (daily at 2 AM)
- `weather_dag.py` - Weather data extraction (every 6 hours)  
- `geography_dag.py` - Geography data extraction (weekly on Sundays)

**Scripts** (upload to GCS bucket):
- `scripts/extract_flights.py`
- `scripts/extract_weather.py`
- `scripts/extract_geography.py`

**Configuration**:
- `config/config.yaml` - Main configuration file

### Required Secrets in Google Secret Manager:
- `OPENWEATHERMAP_API_KEY` - OpenWeatherMap API key
- `GEODB_API_KEY` - GeoDB Cities API key

### BigQuery Tables Created:
- `tfm_bq_dataset.current_flights`
- `tfm_bq_dataset.current_weather` 
- `tfm_bq_dataset.geography_places`
- `tfm_bq_dataset.geography_countries`

### Service Account:
- `svc-tfm-pipeline-executor@pipeline-weather-flights.iam.gserviceaccount.com`

## Architecture

- **Orchestration**: Google Cloud Composer (Airflow)
- **Processing**: Google Dataproc Serverless (PySpark)
- **Storage**: Google BigQuery
- **Secrets**: Google Secret Manager
- **Monitoring**: Cloud Logging

---
*© 2025 Juan Arias - Master's Thesis Project*
