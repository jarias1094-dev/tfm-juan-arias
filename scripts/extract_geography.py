#!/usr/bin/env python3
"""
Script de Extracción de Datos de Geografía para la API de GeoDB Cities vía RapidAPI
"""

import os
import sys
import json
import requests
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, isnan, isnull
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType
import google.cloud.bigquery as bigquery
from google.cloud import storage, secretmanager

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración del proyecto
PROJECT_ID = "pipeline-weather-flights"
DATASET_ID = "tfm_bq_dataset"

def load_credentials_from_secret_manager(project_id: str) -> Dict[str, str]:
    """Cargar credenciales de la API de GeoDB desde Google Secret Manager"""
    try:
        # Inicializar cliente de Secret Manager
        client = secretmanager.SecretManagerServiceClient()
        
        # Nombre del secreto para la clave API de GeoDB - usando nombre del proyecto
        api_key_secret_name = f"projects/pipeline-weather-flights/secrets/GEODB_API_KEY/versions/latest"
        
        
        # Obtener clave API
        response = client.access_secret_version(request={"name": api_key_secret_name})
        geodb_api_key = response.payload.data.decode("UTF-8").strip()
        
        
        return {
            'api_key': geodb_api_key
        }
        
    except Exception as e:
        logger.error(f"Falló al cargar credenciales desde Secret Manager: {e}")
        logger.error(f"   Asegúrate de que el secreto existe: gcloud secrets create GEODB_API_KEY --data-file=-")
        raise ValueError(f"No se pudieron cargar las credenciales de GeoDB desde Secret Manager: {e}")

def load_credentials():
    """Cargar credenciales desde Google Secret Manager o usar variables de entorno como respaldo"""
    try:
        # Obtener ID del proyecto
        project_id = PROJECT_ID
        
        # Intentar cargar desde Secret Manager primero
        try:
            credentials = load_credentials_from_secret_manager(project_id)
            return {
                'geodb': credentials
            }
        except Exception as e:
            logger.error(f"Falló al cargar credenciales desde Secret Manager: {e}")
            raise ValueError(f"No se pudieron cargar las credenciales de GeoDB desde Secret Manager: {e}")
        
        # Respaldo a variables de entorno
        geodb_api_key = os.getenv('GEODB_API_KEY')
        
        if geodb_api_key:
            return {
                'geodb': {
                    'api_key': geodb_api_key
                }
            }
        
        # Respaldo final a archivo de credenciales local
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_dir = os.path.dirname(script_dir)
        credentials_path = os.path.join(project_dir, 'config', 'credentials.json')
        
        if os.path.exists(credentials_path):
            with open(credentials_path, 'r') as f:
                credentials = json.load(f)
            return credentials
        
        raise ValueError("No se encontraron credenciales de GeoDB en Secret Manager, variables de entorno o archivos locales")
        
    except Exception as e:
        logger.error(f"Falló al cargar credenciales: {e}")
        raise

class GeographyExtractor:
    """Extraer datos geográficos desde la API de GeoDB Cities vía RapidAPI"""
    
    def __init__(self):
        credentials = load_credentials()
        
        # Obtener clave API de GeoDB
        geodb_creds = credentials.get('geodb', {})
        self.api_key = geodb_creds.get('api_key')
        
        self.base_url = "https://wft-geo-db.p.rapidapi.com/v1"
        self.spark = SparkSession.builder \
            .appName("GeographyDataExtraction") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.executor.instances", "2") \
            .config("spark.executor.cores", "4") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.driver.cores", "4") \
            .getOrCreate()
        
        self.project_id = PROJECT_ID
        self.dataset_id = DATASET_ID
        self.table_id = 'geography_places'
        
        if not self.api_key:
            raise ValueError("Se requiere clave API de GeoDB (configurar en Secret Manager, variables de entorno o credentials.json)")
    
    def get_places(self, limit: int = 500) -> Optional[List[Dict]]:
        """Obtener lugares usando la API de Lugares de GeoDB con limitación de velocidad"""
        import time
        
        try:
            batch_size = min(10, limit)  # Límite del plan BÁSICO: máximo 10 resultados
            all_places = []
            offset = 0
            
            while len(all_places) < limit and offset < 1000:  # Límite superior razonable
                url = f"{self.base_url}/geo/places"
                params = {
                    'limit': batch_size,
                    'offset': offset,
                    'sort': 'population',
                    'minPopulation': 10000,
                    'types': 'CITY'  # Enfocarse en ciudades
                }
                
                headers = {
                    'X-RapidAPI-Key': self.api_key,
                    'X-RapidAPI-Host': 'wft-geo-db.p.rapidapi.com'
                }
                
                
                response = requests.get(url, params=params, headers=headers, timeout=30)
                
                if response.status_code == 429:
                    time.sleep(5)
                    continue
                elif response.status_code == 403:
                    logger.error(f"403 Prohibido - problema con clave API o suscripción")
                    logger.error(f"   Respuesta: {response.text}")
                    logger.error(f"   Longitud de clave API: {len(headers['X-RapidAPI-Key'])}")
                    return None
                elif response.status_code != 200:
                    logger.error(f"API Error {response.status_code}: {response.text}")
                    return None
                
                response.raise_for_status()
                data = response.json()
                
                if not data.get('data'):
                    break
                
                batch_places = []
                for place in data['data']:
                    place_data = {
                        'place_id': place.get('id'),
                        'place_name': place.get('name'),
                        'place_type': place.get('type'),
                        'country_code': place.get('countryCode'),
                        'country_name': place.get('country'),
                        'region': place.get('region'),
                        'region_code': place.get('regionCode'),
                        'latitude': float(place.get('latitude')) if place.get('latitude') is not None else None,
                        'longitude': float(place.get('longitude')) if place.get('longitude') is not None else None,
                        'population': place.get('population'),
                        'elevation_meters': place.get('elevationMeters'),
                        'timezone': place.get('timezone'),
                        'extraction_timestamp': datetime.now(timezone.utc)
                    }
                    batch_places.append(place_data)
                
                all_places.extend(batch_places)
                
                if len(data['data']) < batch_size:
                    break
                
                offset += batch_size
                
                if len(all_places) < limit:
                    time.sleep(2)
            
            return all_places[:limit]
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None

    def get_countries(self, limit: int = 100) -> Optional[List[Dict]]:
        """Obtener lista de países con paginación para plan BÁSICO (máximo 10 resultados por solicitud)"""
        import time
        
        try:
            all_countries = []
            offset = 0
            batch_size = 10
            
            while len(all_countries) < limit and offset < 200:
                url = f"{self.base_url}/geo/countries"
                params = {
                    'limit': batch_size,
                    'offset': offset
                }
                
                headers = {
                    'X-RapidAPI-Key': self.api_key,
                    'X-RapidAPI-Host': 'wft-geo-db.p.rapidapi.com'
                }
                
                
                max_retries = 3
                for attempt in range(max_retries):
                    response = requests.get(url, params=params, headers=headers, timeout=30)
                    
                    if response.status_code == 429:
                        wait_time = 5 * (attempt + 1)
                        time.sleep(wait_time)
                        continue
                    elif response.status_code == 403:
                        logger.error(f"403 Prohibido - problema con clave API")
                        return None
                    elif response.status_code != 200:
                        logger.error(f"API Error {response.status_code}: {response.text}")
                        return None
                    
                    response.raise_for_status()
                    break
                else:
                    logger.error("Máximo de reintentos excedido debido a limitación de velocidad")
                    return None
                
                data = response.json()
                
                if not data.get('data'):
                    break
                
                batch_countries = []
                for country in data['data']:
                    country_data = {
                        'country_code': country.get('code'),
                        'country_name': country.get('name'),
                        'currency_code': country.get('currencyCodes', [None])[0] if country.get('currencyCodes') else None,
                        'continent': country.get('continent'),
                        'extraction_timestamp': datetime.now(timezone.utc)
                    }
                    batch_countries.append(country_data)
                
                all_countries.extend(batch_countries)
                
                if len(data['data']) < batch_size:
                    break
                
                offset += batch_size
                
                if len(all_countries) < limit:
                    time.sleep(2)
            
            return all_countries[:limit]
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
    
    def get_cities_by_country(self, country_code: str, limit: int = 100) -> Optional[List[Dict]]:
        """Obtener ciudades para un país específico"""
        try:
            url = f"{self.base_url}/geo/cities"
            params = {
                'limit': limit,
                'offset': 0,
                'countryIds': country_code,
                'sort': 'population',
                'order': 'desc'
            }
            
            headers = {}
            if self.api_key:
                headers['X-RapidAPI-Key'] = self.api_key
                headers['X-RapidAPI-Host'] = 'geodb-free-service.wirefreethought.com'
            
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data.get('data'):
                return []
            
            cities = []
            for city in data['data']:
                city_data = {
                    'city_id': city.get('id'),
                    'city_name': city.get('name'),
                    'country_code': city.get('countryCode', country_code),
                    'latitude': city.get('latitude'),
                    'longitude': city.get('longitude'),
                    'population': city.get('population'),
                    'elevation_meters': city.get('elevationMeters'),
                    'timezone': city.get('timezone'),
                    'extraction_timestamp': datetime.now(timezone.utc)
                }
                cities.append(city_data)
            
            return cities
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Solicitud API falló para el país {country_code}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error inesperado para el país {country_code}: {e}")
            return None
    
    def get_cities_near_coordinates(self, latitude: float, longitude: float, radius_km: int = 50, limit: int = 50) -> Optional[List[Dict]]:
        """Obtener ciudades cerca de coordenadas específicas"""
        try:
            lat_str = f"{latitude:+.4f}"
            lon_str = f"{longitude:+.4f}"
            location = f"{lat_str}{lon_str}"
            
            url = f"{self.base_url}/geo/locations/{location}/nearbyCities"
            params = {
                'radius': radius_km,
                'limit': limit,
                'offset': 0,
                'sort': 'distance',
                'order': 'asc'
            }
            
            headers = {}
            if self.api_key:
                headers['X-RapidAPI-Key'] = self.api_key
                headers['X-RapidAPI-Host'] = 'geodb-free-service.wirefreethought.com'
            
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data.get('data'):
                return []
            
            cities = []
            for city in data['data']:
                city_data = {
                    'city_id': city.get('id'),
                    'city_name': city.get('name'),
                    'country_code': city.get('countryCode'),
                    'latitude': city.get('latitude'),
                    'longitude': city.get('longitude'),
                    'population': city.get('population'),
                    'elevation_meters': city.get('elevationMeters'),
                    'timezone': city.get('timezone'),
                    'distance_km': city.get('distance'),
                    'extraction_timestamp': datetime.now(timezone.utc)
                }
                cities.append(city_data)
            
            return cities
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for coordinates {latitude}, {longitude}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for coordinates {latitude}, {longitude}: {e}")
            return None
    
    def get_major_cities_worldwide(self) -> List[Dict]:
        """Obtener ciudades principales basadas en países obtenidos dinámicamente"""
        
        # Get countries from API instead of hardcoded list
        countries_data = self.get_countries(limit=15)  # Cap at 15 countries for demo
        
        if not countries_data:
            # Fallback to hardcoded list only if API fails
            logger.warning("No se pudieron obtener países desde la API, usando lista de respaldo")
            major_countries = ['US', 'GB', 'FR', 'DE', 'IT', 'ES', 'NL', 'JP', 'CN', 'IN']
        else:
            # Use country codes from API response
            major_countries = [country['country_code'] for country in countries_data]
            logger.info(f"Obteniendo ciudades para {len(major_countries)} países desde la API")
        
        all_cities = []
        
        for country_code in major_countries:
            cities = self.get_cities_by_country(country_code, limit=10)  # Cap at 10 cities per country
            
            if cities:
                all_cities.extend(cities)
                logger.info(f"Agregadas {len(cities)} ciudades para {country_code}")
            
            import time
            time.sleep(0.1)  # Rate limiting
        
        logger.info(f"Total de ciudades obtenidas: {len(all_cities)}")
        return all_cities
    
    def create_places_schema(self) -> StructType:
        """Definir el esquema para los datos de lugares"""
        return StructType([
            StructField("place_id", IntegerType(), True),
            StructField("place_name", StringType(), True),
            StructField("place_type", StringType(), True),
            StructField("country_code", StringType(), True),
            StructField("country_name", StringType(), True),
            StructField("region", StringType(), True),
            StructField("region_code", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("population", IntegerType(), True),
            StructField("elevation_meters", IntegerType(), True),
            StructField("timezone", StringType(), True),
            StructField("extraction_timestamp", TimestampType(), False)
        ])
    
    def create_countries_schema(self) -> StructType:
        """Definir el esquema para los datos de países"""
        return StructType([
            StructField("country_code", StringType(), True),
            StructField("country_name", StringType(), True),
            StructField("currency_code", StringType(), True),
            StructField("continent", StringType(), True),
            StructField("extraction_timestamp", TimestampType(), False)
        ])
    
    def enrich_geography_data(self, df):
        """Agregar campos derivados a los datos de geografía."""
        df = df.withColumn(
            "population_category",
            when(col("population").isNull(), "Unknown")
            .when(col("population") < 100000, "Small")
            .when(col("population") < 500000, "Medium")
            .when(col("population") < 2000000, "Large")
            .otherwise("Metropolitan")
        )
        
        df = df.withColumn(
            "elevation_category",
            when(col("elevation_meters").isNull(), "Unknown")
            .when(col("elevation_meters") < 0, "Below Sea Level")
            .when(col("elevation_meters") < 100, "Low")
            .when(col("elevation_meters") < 500, "Medium")
            .when(col("elevation_meters") < 1000, "High")
            .otherwise("Very High")
        )
        
        df = df.withColumn(
            "hemisphere",
            when(col("latitude") > 0, "Northern")
            .when(col("latitude") < 0, "Southern")
            .otherwise("Equatorial")
        )
        
        return df
    
    def extract_and_process_geography_data(self) -> None:
        """Método principal para extraer y procesar datos de geografía."""
        
        countries_limit = 15  # Reduced for demo
        
        # Use dynamic country-based approach instead of general places
        places_data = self.get_major_cities_worldwide()
        
        if not places_data:
            logger.error("No se extrajeron datos de geografía")
            return
        
        
        places_schema = self.create_places_schema()
        places_df = self.spark.createDataFrame(places_data, places_schema)
        
        places_df = self.enrich_geography_data(places_df)
        
        places_df = places_df.withColumn("extraction_date", col("extraction_timestamp").cast("date"))
        
        record_count = places_df.count()
        
        self.write_places_to_bigquery(places_df)
        
        import time
        time.sleep(3)
        
        countries_data = self.get_countries(limit=countries_limit)
        
        if countries_data:
            countries_schema = self.create_countries_schema()
            countries_df = self.spark.createDataFrame(countries_data, countries_schema)
            countries_df = countries_df.withColumn("extraction_date", col("extraction_timestamp").cast("date"))
            
            countries_count = countries_df.count()
            self.write_countries_to_bigquery(countries_df)
        
    
    def write_places_to_bigquery(self, df) -> None:
        """Escribir DataFrame de lugares a BigQuery."""
        try:
            df.write \
                .format("bigquery") \
                .option("table", f"{self.project_id}.{self.dataset_id}.{self.table_id}") \
                .option("writeMethod", "direct") \
                .option("partitionField", "extraction_date") \
                .option("partitionType", "DAY") \
                .mode("append") \
                .save()
            
            
        except Exception as e:
            logger.error(f"Falló al escribir lugares a BigQuery: {e}")
            raise
    
    def write_countries_to_bigquery(self, df) -> None:
        """Escribir DataFrame de países a BigQuery."""
        try:
            df.write \
                .format("bigquery") \
                .option("table", f"{self.project_id}.{self.dataset_id}.geography_countries") \
                .option("writeMethod", "direct") \
                .option("partitionField", "extraction_date") \
                .option("partitionType", "DAY") \
                .mode("append") \
                .save()
            
            
        except Exception as e:
            logger.error(f"Falló al escribir los países a BigQuery: {e}")
            raise
    
    def cleanup(self):
        """Limpiar sesión de Spark."""
        if self.spark:
            self.spark.stop()

def main():
    """Función principal de ejecución."""
    try:
        extractor = GeographyExtractor()
        extractor.extract_and_process_geography_data()
    except Exception as e:
        logger.error(f"Extracción de geografía falló: {e}")
        sys.exit(1)
    finally:
        if 'extractor' in locals():
            extractor.cleanup()

if __name__ == "__main__":
    main()
