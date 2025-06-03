# pylint: disable=missing-module-docstring, broad-exception-caught, missing-class-docstring, missing-function-docstring

from ingestion.noaa_downloader import NOAADownloader
from ingestion.noaa_parser import NOAAParser

class WeatherPipeline:
    def __init__(self, db_client):
        self.db = db_client
        self.downloader = NOAADownloader()
        self.parser = NOAAParser(self.db)

    def setup_schema(self, schema_path):
        """Create tables based on schema.sql file."""
        self.db.create_tables_from_file(schema_path)
        print("Database schema created.")

    def run_stations_pipeline(self):
        """Download and load station data into the database."""
        stations_path = self.downloader.download_stations_file()
        self.parser.parse_stations_and_insert(stations_path)

    def run_weather_pipeline(self, year):
        """Download and load U.S.-only weather data for a given year."""
        extract_path = self.downloader.download_and_extract_us_stations(year)
        self.parser.parse_folder_and_insert(extract_path)

    def close(self):
        self.db.close()

if __name__ == "__main__":
    print("Running full weather pipeline...")

    import os
    from dotenv import load_dotenv
    from pipeline.weather_pipeline import WeatherPipeline
    from db.postgres_client import PostgresClient

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
    pipeline.setup_schema("/opt/airflow/db/NOAAschema.sql")  # Or update path for local use
    pipeline.run_stations_pipeline()
    pipeline.run_weather_pipeline(year=os.getenv("DOWNLOAD_YEAR", "2023"))
    pipeline.close()
