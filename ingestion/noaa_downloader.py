import os
import requests
import tarfile
from ingestion.noaa_parser import NOAAParser

# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
class NOAADownloader:
    def __init__(self, data_dir='data'):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.station_url = os.getenv('STATION_URL')
        self.archive_base_url = os.getenv('ARCHIVE_BASE_URL')

    def download_stations_file(self):
        path = os.path.join(self.data_dir, "stations.csv")
        if not os.path.exists(path):
            r = requests.get(self.station_url)
            with open(path, 'wb') as f:
                f.write(r.content)
        return path

    def download_year_archive(self, year):
        archive_name = f"{year}.tar.gz"
        archive_path = os.path.join(self.data_dir, archive_name)
        extract_path = os.path.join(self.data_dir, f"gsod_{year}")

        if not os.path.exists(archive_path):
            r = requests.get(self.archive_base_url + archive_name, stream=True)
            with open(archive_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        if not os.path.exists(extract_path):
            with tarfile.open(archive_path, "r:gz") as tar:
                tar.extractall(path=extract_path)

        return extract_path

    def download_and_parse_year(self, year, db_client):
        extract_path = self.download_year_archive(year)
        parser = NOAAParser(db_client)
        parser.parse_folder_and_insert(extract_path)
