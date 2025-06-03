# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import os
import tarfile
import requests
import pandas as pd
from tqdm import tqdm


class NOAADownloader:
    def __init__(self, data_dir='data'):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.station_url = os.getenv('STATION_URL')
        self.archive_base_url = os.getenv('ARCHIVE_BASE_URL')
        self.us_stations_ids = set()

    def download_stations_file(self):
        path = os.path.join(self.data_dir, "stations.csv")
        if not os.path.exists(path):
            r = requests.get(self.station_url, timeout=15)
            with open(path, 'wb') as f:
                f.write(r.content)
        return path

    def filter_us_stations_ids(self, stations_csv_path):
        df = pd.read_csv(stations_csv_path)
        df = df[df['CTRY'] == 'US']
        df['STATION'] = df['USAF'].astype(str).str.zfill(6) + df['WBAN'].astype(str).str.zfill(5)
        self.us_stations_ids = set(df['STATION'].dropna())

    def download_year_archive(self, year):
        archive_name = f"{year}.tar.gz"
        archive_path = os.path.join(self.data_dir, archive_name)
        extract_path = os.path.join(self.data_dir, f"gsod_{year}")

        if not os.path.exists(archive_path):
            r = requests.get(self.archive_base_url + archive_name, stream=True, timeout=15)
            tot_ln = int(r.headers.get('content-length', 0))
            with open(archive_path, 'wb') as f:
                with tqdm(total=tot_ln, unit='B', unit_scale=True, desc=f"DL: {archive_name}") as p:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        p.update(len(chunk))

        if not os.path.exists(extract_path):
            with tarfile.open(archive_path, "r:gz") as tar:
                members = tar.getmembers()
                for member in tqdm(members, desc="Extracting", unit="file"):
                    tar.extract(member, path=extract_path)

        return extract_path

    def download_and_extract_us_stations(self, year):
        stations_csv_path = self.download_stations_file()
        self.filter_us_stations_ids(stations_csv_path)

        archive_name = f"{year}.tar.gz"
        archive_path = os.path.join(self.data_dir, archive_name)
        extract_path = os.path.join(self.data_dir, f"gsod_{year}_us")

        if not os.path.exists(archive_path):
            r = requests.get(self.archive_base_url + archive_name, stream=True, timeout=15)
            with open(archive_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        if not os.path.exists(extract_path):
            os.makedirs(extract_path, exist_ok=True)
            with tarfile.open(archive_path, "r:gz") as tar:
                for member in tar.getmembers():
                    if member.name.endswith(".csv"):
                        station_id = os.path.splitext(os.path.basename(member.name))[0]
                        if station_id in self.us_stations_ids:
                            tar.extract(member, path=extract_path)

        return extract_path

if __name__ == "__main__":
    print("Starting NOAA downloader script...")

    import sys
    import os
    from dotenv import load_dotenv
    from pipeline.weather_pipeline import WeatherPipeline
    from db.postgres_client import PostgresClient

    if os.getenv("AIRFLOW_HOME"):
        sys.path.append("/opt/airflow")

    load_dotenv()

    db_config = {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }

    year = os.getenv("DOWNLOAD_YEAR", "2023")
    downloader = NOAADownloader(data_dir="/opt/airflow/data")
    stations_path = downloader.download_stations_file()

    db = PostgresClient(db_config)
    pipeline = WeatherPipeline(db)
    pipeline.parser.parse_stations_and_insert(stations_path)

    output_path = downloader.download_and_extract_us_stations(year)

    print(f"Data downloaded and extracted to: {output_path}")
