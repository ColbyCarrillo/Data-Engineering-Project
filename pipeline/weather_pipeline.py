# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import pandas as pd
from ingestion.noaa_downloader import NOAADownloader
from ingestion.noaa_parser import NOAAParser
from db.postgres_client import PostgresClient

class WeatherPipeline:
    def __init__(self, db_config):
        self.db = PostgresClient(db_config)
        self.downloader = NOAADownloader()
        self.parser = NOAAParser(self.db)

    def run_stations_pipeline(self, schema_path):
        self.db.create_tables_from_file(schema_path)
        stations_path = self.downloader.download_stations_file()
        stations_df = pd.read_csv(stations_path)
    
        for _, row in stations_df.iterrows():
            self.db.insert_station(row)
    
        self.db.conn.commit()
        print(f"Inserted {len(stations_df)} stations.")
        print("Stations pipeline completed.")

    def run_weather_pipeline(self, year):
        extract_path = self.downloader.download_year_archive(year)
        self.parser.parse_folder_and_insert(extract_path)

    def close(self):
        self.db.close()
