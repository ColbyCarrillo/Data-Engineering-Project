"""
Main script for running the NOAA weather data pipeline.

This script initializes the database connection, downloads station metadata
and weather data, and inserts the parsed results into a PostgreSQL database.

Requires environment variables for database configuration and NOAA data URLs.

Author: Colby Carrillo
"""
import os
from dotenv import load_dotenv
from pipeline.weather_pipeline import WeatherPipeline

load_dotenv()

db_config = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

pipeline = WeatherPipeline(db_config)

# Setup Schema
pipeline.setup_schema("db/NOAAschema.sql")

# Set up DB + load stations
pipeline.run_stations_pipeline()

# Download + insert weather data
pipeline.run_weather_pipeline(2022)

pipeline.close()