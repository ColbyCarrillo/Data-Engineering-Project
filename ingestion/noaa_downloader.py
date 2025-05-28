# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import os
import tarfile
import requests
import pandas as pd

class NOAADownloader:
    def __init__(self, data_dir='data'):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        # Static website variables - NOAA GSOD data
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
        print(f"Filtered {len(self.us_stations_ids)} US station IDs.")

    def download_year_archive(self, year):
        """Downloads and extracts all stations (no filtering)."""
        archive_name = f"{year}.tar.gz"
        archive_path = os.path.join(self.data_dir, archive_name)
        extract_path = os.path.join(self.data_dir, f"gsod_{year}")

        if not os.path.exists(archive_path):
            r = requests.get(self.archive_base_url + archive_name, stream=True, timeout=15)
            with open(archive_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        if not os.path.exists(extract_path):
            with tarfile.open(archive_path, "r:gz") as tar:
                tar.extractall(path=extract_path)

        return extract_path

    def download_and_extract_us_stations(self, year):
        """Downloads and extracts ONLY U.S. station data based on filtered list."""
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
