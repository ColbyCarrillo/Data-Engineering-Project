# pylint: disable=line-too-long, too-many-locals, bare-except, too-few-public-methods, missing-module-docstring, missing-class-docstring, missing-function-docstring

import os
from datetime import datetime
import pandas as pd
from tqdm import tqdm

class NOAAParser:
    def __init__(self, db_client, project_name="NOAA_Weather"):
        self.db = db_client
        self.project_name = project_name

        self.weather_elements = [
            "TEMP", "DEWP", "SLP", "VISIB", "WDSP",
            "PRCP", "SNDP", "MAX", "MIN"
        ]

        self.missing_values = {
            "TEMP": 9999.9,
            "DEWP": 9999.9,
            "SLP": 9999.9,
            "VISIB": 999.9,
            "WDSP": 999.9,
            "PRCP": 99.99,
            "SNDP": 999.9,
            "MAX": 9999.9,
            "MIN": 9999.9,
        }

    def parse_folder_and_insert(self, folder_path):
        file_list = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

        for file_name in tqdm(file_list, desc="Processing weather files"):
            file_path = os.path.join(folder_path, file_name)

            if self.db.is_file_already_ingested(self.project_name, file_path):
                continue
            ingestion_id = self.db.log_ingestion(self.project_name, file_path, 0, success=True)
            # tqdm.write(f"Processing: {file_path}")

            try:
                df = pd.read_csv(file_path, dtype={"STATION": str})
            except (pd.errors.ParserError, FileNotFoundError) as e:
                self.db.update_ingestion_record(ingestion_id, 0, success=False, error_message=str(e))
                continue

            rows_inserted = 0

            for _, row in df.iterrows():
                station = str(row.get("STATION", "")).strip()
                if len(station) != 11:
                    continue

                usaf = station[:6]
                wban = station[6:]

                try:
                    date = datetime.strptime(row.get("DATE", ""), "%Y-%m-%d").date()
                except Exception:
                    continue

                for element in self.weather_elements:
                    value = row.get(element, None)
                    if pd.isna(value):
                        continue

                    try:
                        float_value = float(value)
                    except ValueError:
                        continue

                    sentinel = self.missing_values[element]
                    if float_value == sentinel:
                        continue

                    self.db.cursor.execute(
                        """
                        INSERT INTO daily_weather
                        (usaf, wban, date, element, value, mflag, qflag, sflag, obs_time, ingestion_log_id, inserted_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT DO NOTHING;
                        """,
                        (
                            usaf, wban, date, element, value,
                            None, None, None, None,
                            ingestion_id
                        )
                    )
                    rows_inserted += 1

            self.db.conn.commit()
            # tqdm.write(f"Inserted {rows_inserted} rows into the daily_weather from {file_path}")
            self.db.update_ingestion_record(ingestion_id, rows_inserted, success=True)
