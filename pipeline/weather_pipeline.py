# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import pandas as pd
from tqdm import tqdm
from ingestion.noaa_downloader import NOAADownloader
from ingestion.noaa_parser import NOAAParser
from db.postgres_client import PostgresClient

class WeatherPipeline:
    def __init__(self, db_config):
        self.db = PostgresClient(db_config)
        self.downloader = NOAADownloader()
        self.parser = NOAAParser(self.db)

    def setup_schema(self, schema_path):
        """Create tables based on schema.sql file."""
        self.db.create_tables_from_file(schema_path)
        print("Database schema created.")

    def run_stations_pipeline(self):
        """Download and load station data into the database."""
        stations_path = self.downloader.download_stations_file()

        if self.db.is_file_already_ingested("NOAA_Weather", stations_path):
            print("Stations data already ingested. Skipping.")
            return
        try:
            stations_df = pd.read_csv(stations_path)
        except Exception as e:
            self.db.log_ingestion("NOAA_Weather", stations_path, 0, False, str(e))
            return

        # Log and get the ingestion ID
        ingestion_id = self.db.log_ingestion("NOAA_Weather", stations_path, 0)
        rows_inserted = 0

        for _, row in tqdm(stations_df.iterrows(), total=len(stations_df), desc="Insert stations"):
            self.db.insert_station(row, ingestion_id)
            rows_inserted += 1

        self.db.conn.commit()

        # Update ingestion record with number of rows inserted
        self.db.update_ingestion_record(ingestion_id, rows_inserted, success=True)

        print(f"Inserted {len(stations_df)} stations.")
        print("Stations pipeline completed.")

    def run_weather_pipeline(self, year):
        """Download and load U.S.-only weather data for a given year."""
        extract_path = self.downloader.download_and_extract_us_stations(year)
        self.parser.parse_folder_and_insert(extract_path)

    def close(self):
        self.db.close()
