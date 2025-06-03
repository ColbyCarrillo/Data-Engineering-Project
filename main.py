"""
Main script for running the NOAA weather data pipeline.

This script initializes the database connection, downloads station metadata
and weather data, and inserts the parsed results into a PostgreSQL database.

Requires environment variables for database configuration and NOAA data URLs.

Author: Colby Carrillo
"""
import os
import argparse
from dotenv import load_dotenv
from pipeline.weather_pipeline import WeatherPipeline
from db.postgres_client import PostgresClient

def main():
    load_dotenv()

    db_config = {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }

    db = PostgresClient(db_config)
    pipeline = WeatherPipeline(db)

    parser = argparse.ArgumentParser(description="NOAA Weather Data Pipeline")
    parser.add_argument("--init-schema", action="store_true", help="Initialize DB schema")
    parser.add_argument("--stations", action="store_true", help="Run stations ingestion")
    parser.add_argument("--weather", type=int, help="Download and insert weather data for a given year")
    parser.add_argument("--all", action="store_true", help="Run all steps")

    args = parser.parse_args()

    try:
        # If the user asked for schema setup (or wants to run everything), create the database tables
        if args.init_schema or args.all:
            pipeline.setup_schema("/opt/airflow/db/NOAAschema.sql")
        
        # If the user wants to ingest station metadata (or run everything), do that step
        if args.stations or args.all:
            pipeline.run_stations_pipeline()

        # If the user passed --weather with a year OR chose --all, parse the weather data
        if args.weather or args.all:
            year = args.weather if args.weather else int(os.getenv("DOWNLOAD_YEAR", "2023"))
            pipeline.run_weather_pipeline(year)

    finally:
        pipeline.close()

if __name__ == "__main__":
    print("Starting NOAA pipeline main controller...")
    main()

