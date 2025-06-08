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
