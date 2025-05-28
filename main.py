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

# Set up DB + load stations
pipeline.run_stations_pipeline("db/NOAAschema.sql")

# Download + insert weather data
pipeline.run_weather_pipeline(2022)

pipeline.close()



# import os

# from dotenv import load_dotenv
# import pandas as pd

# from pipeline.weather_pipeline import WeatherPipeline
# from ingestion.noaa_downloader import NOAADownloader
# from ingestion.noaa_parser import NOAAParser
# from db.postgres_client import PostgresClient

# load_dotenv()

# # DB dict configuration
# db_config = {
#     "host": os.getenv("DB_HOST"),
#     "port": os.getenv("DB_PORT"),
#     "dbname": os.getenv("DB_NAME"),
#     "user": os.getenv("DB_USER"),
#     "password": os.getenv("DB_PASSWORD"),
# }

# db = PostgresClient(db_config)

# # Create tables if not exist
# db.create_tables_from_file("db/schema.sql")

# # Download stations
# downloader = NOAADownloader()
# stations_path = downloader.download_stations_file()

# # Parse and insert station data
# stations_df = pd.read_csv(stations_path)

# for _, row in stations_df.iterrows():
#     db.insert_station(row)
# db.conn.commit()

# # Download weather data for a given year
# YEAR = 2022
# extract_path = downloader.download_year_archive(YEAR)

# # Parse and insert weather data
# parser = NOAAParser(db)
# parser.parse_folder_and_insert(extract_path)

# db.close()
