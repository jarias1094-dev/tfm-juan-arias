#!/usr/bin/env python3
"""
Geography Data Extraction Script for GeoDB Cities API via RapidAPI
Extracts geographical information for cities, countries, and regions using GeoDB API
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
    """Load GeoDB API credentials from Google Secret Manager"""
    try:
        # Initialize Secret Manager client
        client = secretmanager.SecretManagerServiceClient()
        
        # Secret name for GeoDB API key - using project name
        api_key_secret_name = f"projects/pipeline-weather-flights/secrets/GEODB_API_KEY/versions/latest"
        
        
        # Get API key
        response = client.access_secret_version(request={"name": api_key_secret_name})
        geodb_api_key = response.payload.data.decode("UTF-8").strip()
        
        
        return {
            'api_key': geodb_api_key
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to load credentials from Secret Manager: {e}")
        logger.error(f"   Make sure the secret exists: gcloud secrets create GEODB_API_KEY --data-file=-")
        raise ValueError(f"Could not load GeoDB credentials from Secret Manager: {e}")

def load_credentials():
    """Load credentials from Google Secret Manager or fallback to environment variables"""
    try:
        # Get project ID
        project_id = PROJECT_ID
        
        # Try to load from Secret Manager first
        try:
            credentials = load_credentials_from_secret_manager(project_id)
            return {
                'geodb': credentials
            }
        except Exception as e:
        
        # Fallback to environment variables
        geodb_api_key = os.getenv('GEODB_API_KEY')
        
        if geodb_api_key:
            return {
                'geodb': {
                    'api_key': geodb_api_key
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
        
        raise ValueError("No GeoDB credentials found in Secret Manager, environment variables, or local files")
        
    except Exception as e:
        logger.error(f"Failed to load credentials: {e}")
        raise

class GeographyExtractor:
    """Extract geographical data from GeoDB Cities API via RapidAPI"""
    
    def __init__(self):
        # Load credentials from Secret Manager or fallback options
        credentials = load_credentials()
        
        # Get GeoDB API key
        geodb_creds = credentials.get('geodb', {})
        self.api_key = geodb_creds.get('api_key')
        
        self.base_url = "https://wft-geo-db.p.rapidapi.com/v1"
        self.spark = SparkSession.builder \
            .appName("GeographyDataExtraction") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        # BigQuery configuration
        self.project_id = PROJECT_ID
        self.dataset_id = DATASET_ID
        self.table_id = 'geography_places'
        
        if not self.api_key:
            raise ValueError("GeoDB API key is required (set in Secret Manager, environment variables, or credentials.json)")
    
    def get_places(self, limit: int = 500) -> Optional[List[Dict]]:
        """Fetch places using the GeoDB Places API with rate limiting"""
        import time
        
        try:
            # For BASIC plan, maximum 10 results per request
            batch_size = min(10, limit)  # BASIC plan limit: 10 results max
            all_places = []
            offset = 0
            
            while len(all_places) < limit and offset < 1000:  # Reasonable upper limit
                url = f"{self.base_url}/geo/places"
                params = {
                    'limit': batch_size,
                    'offset': offset,
                    'sort': 'population',
                    'minPopulation': 10000,  # Focus on places with reasonable population
                    'types': 'CITY'  # Focus on cities
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
                    logger.error(f"❌ 403 Forbidden - API key issue or subscription problem")
                    logger.error(f"   Response: {response.text}")
                    logger.error(f"   API Key length: {len(headers['X-RapidAPI-Key'])}")
                    return None
                elif response.status_code != 200:
                    logger.error(f"❌ API Error {response.status_code}: {response.text}")
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
                
                # If we got fewer results than requested, we've reached the end
                if len(data['data']) < batch_size:
                    break
                
                offset += batch_size
                
                # Rate limiting: wait between requests
                if len(all_places) < limit:
                    time.sleep(2)
            
            return all_places[:limit]  # Return only requested amount
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ API request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            return None

    def get_countries(self, limit: int = 100) -> Optional[List[Dict]]:
        """Fetch list of countries with pagination for BASIC plan (10 results max per request)"""
        import time
        
        try:
            all_countries = []
            offset = 0
            batch_size = 10  # BASIC plan maximum
            
            while len(all_countries) < limit and offset < 200:  # Reasonable upper limit for countries
                url = f"{self.base_url}/geo/countries"
                params = {
                    'limit': batch_size,
                    'offset': offset
                }
                
                headers = {
                    'X-RapidAPI-Key': self.api_key,
                    'X-RapidAPI-Host': 'wft-geo-db.p.rapidapi.com'
                }
                
                
                # Retry logic for rate limits
                max_retries = 3
                for attempt in range(max_retries):
                    response = requests.get(url, params=params, headers=headers, timeout=30)
                    
                    if response.status_code == 429:
                        wait_time = 5 * (attempt + 1)  # Exponential backoff
                        time.sleep(wait_time)
                        continue
                    elif response.status_code == 403:
                        logger.error(f"❌ 403 Forbidden - API key issue")
                        return None
                    elif response.status_code != 200:
                        logger.error(f"❌ API Error {response.status_code}: {response.text}")
                        return None
                    
                    response.raise_for_status()
                    break
                else:
                    logger.error("❌ Max retries exceeded due to rate limiting")
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
                
                # If we got fewer results than requested, we've reached the end
                if len(data['data']) < batch_size:
                    break
                
                offset += batch_size
                
                # Rate limiting: wait between requests
                if len(all_countries) < limit:
                    time.sleep(2)
            
            return all_countries[:limit]  # Return only requested amount
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ API request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            return None
    
    def get_cities_by_country(self, country_code: str, limit: int = 100) -> Optional[List[Dict]]:
        """Fetch cities for a specific country"""
        try:
            # Use the general cities endpoint with country filter
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
            logger.error(f"API request failed for country {country_code}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for country {country_code}: {e}")
            return None
    
    def get_cities_near_coordinates(self, latitude: float, longitude: float, radius_km: int = 50, limit: int = 50) -> Optional[List[Dict]]:
        """Fetch cities near specific coordinates"""
        try:
            # Format coordinates in ISO 6709 format (±DD.DDDD±DDD.DDDD)
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
        """Get major cities from key countries worldwide"""
        # List of major countries to get cities from
        major_countries = [
            'US', 'GB', 'FR', 'DE', 'IT', 'ES', 'NL', 'JP', 'CN', 'IN',
            'BR', 'CA', 'AU', 'MX', 'AR', 'RU', 'KR', 'SG', 'TH', 'MY'
        ]
        
        all_cities = []
        
        for country_code in major_countries:
            cities = self.get_cities_by_country(country_code, limit=20)  # Top 20 cities per country
            
            if cities:
                all_cities.extend(cities)
            
            # Add small delay to respect rate limits
            import time
            time.sleep(0.1)
        
        return all_cities
    
    def create_places_schema(self) -> StructType:
        """Define the schema for places data"""
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
        """Define the schema for countries data"""
        return StructType([
            StructField("country_code", StringType(), True),
            StructField("country_name", StringType(), True),
            StructField("currency_code", StringType(), True),
            StructField("continent", StringType(), True),
            StructField("extraction_timestamp", TimestampType(), False)
        ])
    
    def enrich_geography_data(self, df):
        """Add derived fields to geography data"""
        # Add population category
        df = df.withColumn(
            "population_category",
            when(col("population").isNull(), "Unknown")
            .when(col("population") < 100000, "Small")
            .when(col("population") < 500000, "Medium")
            .when(col("population") < 2000000, "Large")
            .otherwise("Metropolitan")
        )
        
        # Add elevation category
        df = df.withColumn(
            "elevation_category",
            when(col("elevation_meters").isNull(), "Unknown")
            .when(col("elevation_meters") < 0, "Below Sea Level")
            .when(col("elevation_meters") < 100, "Low")
            .when(col("elevation_meters") < 500, "Medium")
            .when(col("elevation_meters") < 1000, "High")
            .otherwise("Very High")
        )
        
        # Add hemisphere information
        df = df.withColumn(
            "hemisphere",
            when(col("latitude") > 0, "Northern")
            .when(col("latitude") < 0, "Southern")
            .otherwise("Equatorial")
        )
        
        return df
    
    def extract_and_process_geography_data(self) -> None:
        """Main method to extract and process geography data"""
        
        # For BASIC plan, use conservative limits (10 results per request max)
        places_limit = 100  # 10 batches of 10 = 100 places total
        countries_limit = 50   # 5 batches of 10 = 50 countries total
        
        # Extract places data using GeoDB API
        places_data = self.get_places(limit=places_limit)
        
        if not places_data:
            logger.error("❌ No geography data extracted")
            return
        
        
        # Create DataFrames
        places_schema = self.create_places_schema()
        places_df = self.spark.createDataFrame(places_data, places_schema)
        
        # Enrich the data
        places_df = self.enrich_geography_data(places_df)
        
        # Add partition column
        places_df = places_df.withColumn("extraction_date", col("extraction_timestamp").cast("date"))
        
        record_count = places_df.count()
        
        # Write places to BigQuery
        self.write_places_to_bigquery(places_df)
        
        # Wait before next API call
        import time
        time.sleep(3)
        
        # Extract countries data
        countries_data = self.get_countries(limit=countries_limit)
        
        # Process countries data if available
        if countries_data:
            countries_schema = self.create_countries_schema()
            countries_df = self.spark.createDataFrame(countries_data, countries_schema)
            countries_df = countries_df.withColumn("extraction_date", col("extraction_timestamp").cast("date"))
            
            countries_count = countries_df.count()
            self.write_countries_to_bigquery(countries_df)
        
    
    def write_places_to_bigquery(self, df) -> None:
        """Write places DataFrame to BigQuery"""
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
            logger.error(f"❌ Failed to write places to BigQuery: {e}")
            raise
    
    def write_countries_to_bigquery(self, df) -> None:
        """Write countries DataFrame to BigQuery"""
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
            logger.error(f"❌ Failed to write countries to BigQuery: {e}")
            raise
    
    def cleanup(self):
        """Clean up Spark session"""
        if self.spark:
            self.spark.stop()

def main():
    """Main execution function"""
    try:
        extractor = GeographyExtractor()
        extractor.extract_and_process_geography_data()
    except Exception as e:
        logger.error(f"Geography extraction failed: {e}")
        sys.exit(1)
    finally:
        if 'extractor' in locals():
            extractor.cleanup()

if __name__ == "__main__":
    main()
