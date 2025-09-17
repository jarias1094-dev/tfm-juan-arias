#!/usr/bin/env python3
"""
Flight Data Extraction Script for OpenSky Network API
Extracts real-time flight data and stores in BigQuery
"""

import os
import sys
import json
import requests
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, isnan, isnull
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType
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
    """Load OpenSky credentials from Google Secret Manager"""
    try:
        # Initialize Secret Manager client
        client = secretmanager.SecretManagerServiceClient()
        
        # Secret names
        client_id_secret_name = f"projects/{project_id}/secrets/OPENSKY_CLIENT_ID/versions/latest"
        client_secret_secret_name = f"projects/{project_id}/secrets/OPENSKY_CLIENT_SECRET/versions/latest"
        
        # Get client ID
        response = client.access_secret_version(request={"name": client_id_secret_name})
        opensky_client_id = response.payload.data.decode("UTF-8").strip()
        
        # Get client secret
        response = client.access_secret_version(request={"name": client_secret_secret_name})
        opensky_client_secret = response.payload.data.decode("UTF-8").strip()
            
        return {
            'client_id': opensky_client_id,
            'client_secret': opensky_client_secret
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to load credentials from Secret Manager: {e}")
        raise ValueError(f"Could not load OpenSky credentials from Secret Manager: {e}")

def load_credentials():
    """Load credentials from Google Secret Manager or fallback to environment variables"""
    try:
        # Get project ID
        project_id = os.getenv('GCP_PROJECT_ID', 'pipeline-weather-flights')
        
        # Try to load from Secret Manager first
        try:
            credentials = load_credentials_from_secret_manager(project_id)
            return {
                'opensky': credentials
            }
        except Exception as e:
        
        # Fallback to environment variables
        opensky_client_id = os.getenv('OPENSKY_CLIENT_ID')
        opensky_client_secret = os.getenv('OPENSKY_CLIENT_SECRET')
        
        if opensky_client_id and opensky_client_secret:
            return {
                'opensky': {
                    'client_id': opensky_client_id,
                    'client_secret': opensky_client_secret
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
        
        raise ValueError("No OpenSky credentials found in Secret Manager, environment variables, or local files")
        
    except Exception as e:
        logger.error(f"Failed to load credentials: {e}")
        raise

class FlightExtractor:
    """Extract flight data from OpenSky Network API"""
    
    def __init__(self):
        # Load credentials from JSON file or environment variables
        credentials = load_credentials()
        
        # Get OpenSky credentials (prefer JSON file over environment variables)
        opensky_creds = credentials.get('opensky', {})
        self.client_id = opensky_creds.get('client_id') or os.getenv('OPENSKY_CLIENT_ID')
        self.client_secret = opensky_creds.get('client_secret') or os.getenv('OPENSKY_CLIENT_SECRET')
        
        self.base_url = "https://opensky-network.org/api"
        self.spark = SparkSession.builder \
            .appName("FlightDataExtraction") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        # BigQuery configuration
        self.project_id = PROJECT_ID
        self.dataset_id = DATASET_ID
        self.table_id = 'current_flights'
        
        # OAuth2 token for OpenSky API
        self.access_token = None
        if self.client_id and self.client_secret:
            self.access_token = self._get_oauth2_token()
    
    def _get_oauth2_token(self) -> Optional[str]:
        """Get OAuth2 access token for OpenSky API"""
        try:
            token_url = "https://opensky-network.org/api/oauth/token"
            data = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret
            }
            
            response = requests.post(token_url, data=data, timeout=30)
            response.raise_for_status()
            
            token_data = response.json()
            return token_data.get('access_token')
            
        except Exception as e:
            return None
    
    def get_all_flights(self) -> Optional[List[Dict]]:
        """Fetch all current flights from OpenSky Network"""
        try:
            url = f"{self.base_url}/states/all"
            
            # Add OAuth2 authentication if available
            headers = {}
            if self.access_token:
                headers['Authorization'] = f'Bearer {self.access_token}'
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data.get('states'):
                return []
            
            flights = []
            for state in data['states']:
                # OpenSky API returns states as arrays, we need to map them to fields
                flight_data = self._parse_flight_state(state)
                if flight_data:
                    flights.append(flight_data)
            
            return flights
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
    
    def _parse_flight_state(self, state: List) -> Optional[Dict]:
        """Parse OpenSky flight state array into structured data"""
        try:
            # OpenSky API state array structure:
            # [0] icao24, [1] callsign, [2] origin_country, [3] time_position, 
            # [4] last_contact, [5] longitude, [6] latitude, [7] baro_altitude,
            # [8] on_ground, [9] velocity, [10] true_track, [11] vertical_rate,
            # [12] sensors, [13] geo_altitude, [14] squawk, [15] spi, [16] position_source
            
            if len(state) < 17:
                return None
            
            # Handle None values and convert to appropriate types
            def safe_float(value):
                return float(value) if value is not None else None
            
            def safe_int(value):
                return int(value) if value is not None else None
            
            def safe_str(value):
                return str(value).strip() if value is not None else None
            
            flight_data = {
                'icao24': safe_str(state[0]),
                'callsign': safe_str(state[1]),
                'origin_country': safe_str(state[2]),
                'time_position': safe_int(state[3]),
                'last_contact': safe_int(state[4]),
                'longitude': safe_float(state[5]),
                'latitude': safe_float(state[6]),
                'baro_altitude': safe_float(state[7]),
                'on_ground': bool(state[8]) if state[8] is not None else None,
                'velocity': safe_float(state[9]),
                'true_track': safe_float(state[10]),
                'vertical_rate': safe_float(state[11]),
                'sensors': safe_str(state[12]),
                'geo_altitude': safe_float(state[13]),
                'squawk': safe_str(state[14]),
                'spi': bool(state[15]) if state[15] is not None else None,
                'position_source': safe_int(state[16]),
                'extraction_timestamp': datetime.now(timezone.utc)
            }
            
            # Only include flights with valid position data
            if flight_data['longitude'] is not None and flight_data['latitude'] is not None:
                return flight_data
            
            return None
            
        except (IndexError, ValueError, TypeError) as e:
            return None
    
    def get_flights_by_airport(self, airport_icao: str, radius_km: int = 200) -> Optional[List[Dict]]:
        """Get flights within radius of a specific airport"""
        try:
            # This would require airport coordinates - for now using a simplified approach
            # In a real implementation, you'd need to get airport coordinates first
            url = f"{self.base_url}/states/all"
            
            # Add OAuth2 authentication if available
            headers = {}
            if self.access_token:
                headers['Authorization'] = f'Bearer {self.access_token}'
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data.get('states'):
                return []
            
            # Filter flights (this is a simplified approach)
            # In practice, you'd calculate distance from airport coordinates
            flights = []
            for state in data['states']:
                flight_data = self._parse_flight_state(state)
                if flight_data and flight_data['on_ground'] is False:  # Only airborne flights
                    flights.append(flight_data)
            
            return flights
            
        except Exception as e:
            logger.error(f"Failed to get flights for airport {airport_icao}: {e}")
            return None
    
    def create_flight_schema(self) -> StructType:
        """Define the schema for flight data"""
        return StructType([
            StructField("icao24", StringType(), True),
            StructField("callsign", StringType(), True),
            StructField("origin_country", StringType(), True),
            StructField("time_position", IntegerType(), True),
            StructField("last_contact", IntegerType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("baro_altitude", DoubleType(), True),
            StructField("on_ground", BooleanType(), True),
            StructField("velocity", DoubleType(), True),
            StructField("true_track", DoubleType(), True),
            StructField("vertical_rate", DoubleType(), True),
            StructField("sensors", StringType(), True),
            StructField("geo_altitude", DoubleType(), True),
            StructField("squawk", StringType(), True),
            StructField("spi", BooleanType(), True),
            StructField("position_source", IntegerType(), True),
            StructField("extraction_timestamp", TimestampType(), False)
        ])
    
    def enrich_flight_data(self, df):
        """Add derived fields to flight data"""
        # Add calculated fields
        df = df.withColumn(
            "altitude_ft", 
            when(col("baro_altitude").isNotNull(), col("baro_altitude") * 3.28084).otherwise(None)
        )
        
        df = df.withColumn(
            "velocity_knots",
            when(col("velocity").isNotNull(), col("velocity") * 1.94384).otherwise(None)
        )
        
        df = df.withColumn(
            "vertical_rate_fpm",
            when(col("vertical_rate").isNotNull(), col("vertical_rate") * 196.85).otherwise(None)
        )
        
        # Add flight status
        df = df.withColumn(
            "flight_status",
            when(col("on_ground") == True, "On Ground")
            .when(col("on_ground") == False, "Airborne")
            .otherwise("Unknown")
        )
        
        # Add data quality indicators
        df = df.withColumn(
            "has_position", 
            when((col("longitude").isNotNull()) & (col("latitude").isNotNull()), True).otherwise(False)
        )
        
        df = df.withColumn(
            "has_altitude",
            when(col("baro_altitude").isNotNull(), True).otherwise(False)
        )
        
        return df
    
    def extract_and_process_flight_data(self) -> None:
        """Main method to extract and process flight data"""
        
        # Extract all flights
        flights_data = self.get_all_flights()
        
        if not flights_data:
            logger.error("No flight data extracted")
            return
        
        # Create DataFrame
        schema = self.create_flight_schema()
        df = self.spark.createDataFrame(flights_data, schema)
        
        # Enrich the data
        df = self.enrich_flight_data(df)
        
        # Add partition column
        df = df.withColumn("extraction_date", col("extraction_timestamp").cast("date"))
        
        
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
        extractor = FlightExtractor()
        extractor.extract_and_process_flight_data()
    except Exception as e:
        logger.error(f"Flight extraction failed: {e}")
        sys.exit(1)
    finally:
        if 'extractor' in locals():
            extractor.cleanup()

if __name__ == "__main__":
    main()
