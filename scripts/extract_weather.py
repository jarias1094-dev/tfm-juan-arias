#!/usr/bin/env python3
"""
Weather Data Extraction Script for OpenWeatherMap API
Extracts current weather data for airports using Current Weather API 2.5 (Free)
Stores processed data in BigQuery with proper authentication via Secret Manager
"""

import os
import sys
import json
import requests
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import google.cloud.bigquery as bigquery
from google.cloud import storage, secretmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project configuration
PROJECT_ID = "pipeline-weather-flights"
DATASET_ID = "tfm_bq_dataset"

def load_credentials_from_secret_manager(project_id: str) -> Dict[str, str]:
    """Load OpenWeatherMap credentials from Google Secret Manager"""
    try:
        # Initialize Secret Manager client
        client = secretmanager.SecretManagerServiceClient()
        
        # Secret name for OpenWeatherMap API key
        api_key_secret_name = f"projects/{project_id}/secrets/OPENWEATHER_API_KEY/versions/latest"
        
        # Get API key
        response = client.access_secret_version(request={"name": api_key_secret_name})
        openweather_api_key = response.payload.data.decode("UTF-8").strip()
        
        
        return {
            'api_key': openweather_api_key
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to load credentials from Secret Manager: {e}")
        raise ValueError(f"Could not load OpenWeatherMap credentials from Secret Manager: {e}")

def load_credentials():
    """Load credentials from Google Secret Manager or fallback to environment variables"""
    try:
        # Get project ID
        project_id = PROJECT_ID
        
        # Try to load from Secret Manager first
        try:
            credentials = load_credentials_from_secret_manager(project_id)
            return {
                'openweather': credentials
            }
        except Exception as e:
        
        # Fallback to environment variables
        openweather_api_key = os.getenv('OPENWEATHER_API_KEY')
        
        if openweather_api_key:
            return {
                'openweather': {
                    'api_key': openweather_api_key
                }
            }
        
        # Final fallback to local credentials file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_dir = os.path.dirname(script_dir)
        credentials_path = os.path.join(project_dir, 'config', 'credentials.json')
        
        if os.path.exists(credentials_path):
            with open(credentials_path, 'r') as f:
                credentials = json.load(f)
            return credentials
        
        raise ValueError("No OpenWeatherMap credentials found in Secret Manager, environment variables, or local files")
        
    except Exception as e:
        logger.error(f"Failed to load credentials: {e}")
        raise

class WeatherExtractor:
    """Extract weather data from OpenWeatherMap API"""
    
    def __init__(self):
        # Load credentials from Secret Manager or fallback options
        credentials = load_credentials()
        
        # Get OpenWeatherMap API key
        openweather_creds = credentials.get('openweather', {})
        self.api_key = openweather_creds.get('api_key')
        
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"
        self.spark = SparkSession.builder \
            .appName("WeatherDataExtraction") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        # BigQuery configuration
        self.project_id = PROJECT_ID
        self.dataset_id = DATASET_ID
        self.table_id = 'current_weather'
        
        if not self.api_key:
            raise ValueError("OpenWeatherMap API key is required (set in Secret Manager, environment variables, or credentials.json)")
    
    def get_weather_data(self, airport_info: Dict) -> Optional[Dict]:
        """Fetch weather data for specific airport using Current Weather API 2.5 (Free)"""
        lat = airport_info['lat']
        lon = airport_info['lon']
        airport_code = airport_info['iata_code']
        
        try:
            params = {
                'lat': lat,
                'lon': lon,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Transform the data to our schema using Current Weather API 2.5 structure
            main = data.get('main', {})
            weather_info = data.get('weather', [{}])[0] if data.get('weather') else {}
            wind = data.get('wind', {})
            clouds = data.get('clouds', {})
            sys = data.get('sys', {})
            
            weather_data = {
                'airport_iata_code': airport_info['iata_code'],
                'airport_icao_code': airport_info['icao_code'],
                'airport_name': airport_info['name'],
                'airport_municipality': airport_info['municipality'],
                'airport_country': airport_info['country'],
                'latitude': float(lat),
                'longitude': float(lon),
                'elevation_ft': airport_info.get('elevation_ft', None),
                'timezone': None,  # Not available in free API
                'timezone_offset': data.get('timezone', None),  # Timezone offset in seconds
                'temperature': float(main.get('temp')) if main.get('temp') is not None else None,
                'feels_like': float(main.get('feels_like')) if main.get('feels_like') is not None else None,
                'humidity': main.get('humidity', None),
                'pressure': main.get('pressure', None),
                'dew_point': None,  # Not available in free API
                'uvi': None,  # Not available in free API
                'cloudiness': clouds.get('all', None),
                'visibility': data.get('visibility', None),
                'wind_speed': float(wind.get('speed')) if wind.get('speed') is not None else None,
                'wind_direction': wind.get('deg', None),
                'wind_gust': float(wind.get('gust')) if wind.get('gust') is not None else None,
                'weather_main': weather_info.get('main', None),
                'weather_description': weather_info.get('description', None),
                'weather_icon': weather_info.get('icon', None),
                'sunrise': datetime.fromtimestamp(sys['sunrise'], tz=timezone.utc) if sys.get('sunrise') else None,
                'sunset': datetime.fromtimestamp(sys['sunset'], tz=timezone.utc) if sys.get('sunset') else None,
                'extraction_timestamp': datetime.now(timezone.utc)
            }
            
            return weather_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {airport_code}: {e}")
            return None
        except KeyError as e:
            logger.error(f"Missing key in API response for {airport_code}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for {airport_code}: {e}")
            return None
    
    def get_airports_from_bigquery(self) -> List[Dict]:
        """Get list of airports from BigQuery airports table"""
        try:
            
            # Read airports table from BigQuery
            airports_df = self.spark.read \
                .format("bigquery") \
                .option("table", f"{self.project_id}.{self.dataset_id}.airports") \
                .load()
            
            # Filter airports with coordinates and scheduled service
            filtered_airports_df = airports_df.filter(
                (airports_df.latitude_deg.isNotNull()) & 
                (airports_df.longitude_deg.isNotNull()) &
                (airports_df.scheduled_service == True) &
                (airports_df.iata_code.isNotNull()) &
                (airports_df.iata_code != '')
            ).select(
                "iata_code",
                "icao_code", 
                "name",
                "latitude_deg",
                "longitude_deg",
                "elevation_ft",
                "municipality",
                "iso_country"
            ).orderBy("name")
            
            # Check the count first to avoid memory issues
            airport_count = filtered_airports_df.count()
            
            # Limit to reasonable number for weather API calls (avoid rate limits and memory issues)
            if airport_count > 500:
                filtered_airports_df = filtered_airports_df.limit(500)
            
            # Convert to list of dictionaries with better error handling
            try:
                airports_data = filtered_airports_df.collect()
            except Exception as collect_error:
                logger.error(f"❌ Error collecting airport data: {collect_error}")
                # Try with smaller batch first
                airports_data = filtered_airports_df.limit(100).collect()
            
            airports = []
            for i, row in enumerate(airports_data):
                try:
                    # Handle potential None values and data type conversions
                    airport_info = {
                        'iata_code': str(row['iata_code']) if row['iata_code'] else None,
                        'icao_code': str(row['icao_code']) if row['icao_code'] else str(row['iata_code']),
                        'name': str(row['name']) if row['name'] else 'Unknown Airport',
                        'lat': float(row['latitude_deg']) if row['latitude_deg'] is not None else 0.0,
                        'lon': float(row['longitude_deg']) if row['longitude_deg'] is not None else 0.0,
                        'elevation_ft': int(row['elevation_ft']) if row['elevation_ft'] is not None else None,
                        'municipality': str(row['municipality']) if row['municipality'] else 'Unknown',
                        'country': str(row['iso_country']) if row['iso_country'] else 'Unknown'
                    }
                    
                    # Skip airports with invalid coordinates
                    if airport_info['lat'] == 0.0 and airport_info['lon'] == 0.0:
                        continue
                        
                    airports.append(airport_info)
                    
                except Exception as row_error:
                    logger.error(f"❌ Error processing airport row {i}: {row_error}")
                    logger.error(f"   Row data: {dict(row.asDict()) if hasattr(row, 'asDict') else row}")
                    continue
            
            
            # Log sample airports
            if airports:
                for airport in airports[:5]:
            
            return airports
            
        except Exception as e:
            logger.error(f"❌ Failed to load airports from BigQuery: {e}")
            # Fallback to a small set of major airports for testing
            return self.get_fallback_airports()
    
    def get_fallback_airports(self) -> List[Dict]:
        """Fallback list of major airports for testing when BigQuery is not available"""
        airports = [
            {'iata_code': 'JFK', 'icao_code': 'KJFK', 'name': 'John F. Kennedy International Airport', 'lat': 40.6413, 'lon': -73.7781, 'municipality': 'New York', 'country': 'US'},
            {'iata_code': 'LAX', 'icao_code': 'KLAX', 'name': 'Los Angeles International Airport', 'lat': 33.9425, 'lon': -118.4081, 'municipality': 'Los Angeles', 'country': 'US'},
            {'iata_code': 'LHR', 'icao_code': 'EGLL', 'name': 'London Heathrow Airport', 'lat': 51.4700, 'lon': -0.4543, 'municipality': 'London', 'country': 'GB'},
            {'iata_code': 'CDG', 'icao_code': 'LFPG', 'name': 'Charles de Gaulle Airport', 'lat': 49.0097, 'lon': 2.5479, 'municipality': 'Paris', 'country': 'FR'},
            {'iata_code': 'NRT', 'icao_code': 'RJAA', 'name': 'Narita International Airport', 'lat': 35.7720, 'lon': 140.3928, 'municipality': 'Tokyo', 'country': 'JP'}
        ]
        return airports
    
    def create_weather_schema(self) -> StructType:
        """Define the schema for weather data using Current Weather API 2.5 structure"""
        return StructType([
            StructField("airport_iata_code", StringType(), False),
            StructField("airport_icao_code", StringType(), True),
            StructField("airport_name", StringType(), True),
            StructField("airport_municipality", StringType(), True),
            StructField("airport_country", StringType(), True),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("elevation_ft", IntegerType(), True),
            StructField("timezone", StringType(), True),
            StructField("timezone_offset", IntegerType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("feels_like", DoubleType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("pressure", IntegerType(), True),
            StructField("dew_point", DoubleType(), True),
            StructField("uvi", DoubleType(), True),
            StructField("cloudiness", IntegerType(), True),
            StructField("visibility", IntegerType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("wind_direction", IntegerType(), True),
            StructField("wind_gust", DoubleType(), True),
            StructField("weather_main", StringType(), True),
            StructField("weather_description", StringType(), True),
            StructField("weather_icon", StringType(), True),
            StructField("sunrise", TimestampType(), True),
            StructField("sunset", TimestampType(), True),
            StructField("extraction_timestamp", TimestampType(), False)
        ])
    
    def extract_and_process_weather_data(self) -> None:
        """Main method to extract and process weather data"""
        
        # Get airports from BigQuery
        airports = self.get_airports_from_bigquery()
        
        if not airports:
            logger.error("❌ No airports loaded - cannot proceed")
            return
        
        # Extract weather data for each airport
        weather_data_list = []
        successful_extractions = 0
        failed_extractions = 0
        
        for i, airport in enumerate(airports, 1):
            airport_code = airport['iata_code']
            
            try:
                weather_data = self.get_weather_data(airport)
                
                if weather_data:
                    weather_data_list.append(weather_data)
                    successful_extractions += 1
                else:
                    failed_extractions += 1
                    
            except Exception as e:
                failed_extractions += 1
                logger.error(f"❌ Error extracting weather for {airport_code}: {e}")
        
        # Summary
        
        if not weather_data_list:
            logger.error("❌ No weather data extracted - cannot proceed")
            return
        
        # Create DataFrame
        schema = self.create_weather_schema()
        df = self.spark.createDataFrame(weather_data_list, schema)
        
        # Add partition column for better BigQuery performance
        df = df.withColumn("extraction_date", col("extraction_timestamp").cast("date"))
        
        record_count = df.count()
        
        # Write to BigQuery
        self.write_to_bigquery(df)
        
    
    def write_to_bigquery(self, df) -> None:
        """Write DataFrame to BigQuery"""
        try:
            # Configure BigQuery write
            df.write \
                .format("bigquery") \
                .option("table", f"{self.project_id}.{self.dataset_id}.{self.table_id}") \
                .option("writeMethod", "direct") \
                .option("partitionField", "extraction_date") \
                .option("partitionType", "DAY") \
                .mode("append") \
                .save()
            
            logger.info(f"Successfully wrote {df.count()} records to BigQuery")
            
        except Exception as e:
            logger.error(f"Failed to write to BigQuery: {e}")
            raise
    
    def cleanup(self):
        """Clean up Spark session"""
        if self.spark:
            self.spark.stop()

def main():
    """Main execution function"""
    try:
        extractor = WeatherExtractor()
        extractor.extract_and_process_weather_data()
    except Exception as e:
        logger.error(f"Weather extraction failed: {e}")
        sys.exit(1)
    finally:
        if 'extractor' in locals():
            extractor.cleanup()

if __name__ == "__main__":
    main()
