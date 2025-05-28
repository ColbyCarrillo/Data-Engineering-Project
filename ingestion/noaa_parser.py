# pylint: disable=line-too-long, too-many-locals, too-few-public-methods, missing-module-docstring, missing-class-docstring, missing-function-docstring

import os
from datetime import datetime
import pandas as pd
from tqdm import tqdm

class NOAAParser:
    def __init__(self, db_client):
        self.db = db_client

    def parse_folder_and_insert(self, folder_path):
        weather_elements = [
            "TEMP", "DEWP", "SLP", "VISIB", "WDSP",
            "PRCP", "SNDP", "MAX", "MIN"
        ]

        inserted = 0  # Track rows inserted

        file_list = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

        # Progress Bar for file processing
        for file_name in tqdm(file_list, desc="Processing weather files"):
            file_path = os.path.join(folder_path, file_name)
            tqdm.write(f"Processing: {file_path}")

            try:
                # Specify dtype for 'STATION' to ensure don't lose leading zeros
                df = pd.read_csv(file_path, dtype={"STATION": str})
            except (pd.errors.ParserError, FileNotFoundError) as e:
                print(f"Error reading {file_path}: {e}")
                continue

            for _, row in df.iterrows():
                station = str(row.get("STATION", "")).strip()
                if len(station) != 11:
                    continue  # Skip malformed station IDs

                usaf = station[:6]
                wban = station[6:]
                date_str = row.get("DATE", "")
                try:
                    date = datetime.strptime(date_str, "%Y-%m-%d").date()
                except:
                    continue # pylint disable=bare-except # Skip rows with invalid dates

                for element in weather_elements:
                    value = row.get(element, None)
                    if pd.isna(value) or str(value).strip() in ["", "9999.9", "999.9", "99.99", "99.99"]:
                        continue

                    try:
                        value = round(float(value))
                    except:
                        continue # pylint disable=bare-except

                    self.db.cursor.execute(
                        """
                        INSERT INTO daily_weather
                        (usaf, wban, date, element, value, mflag, qflag, sflag, obs_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                        """,
                        (
                            usaf, wban, date, element, value,
                            None, None, None, None
                        )
                    )
                    inserted += 1

            tqdm.write(f"Inserted {inserted} rows into the daily_weather from {folder_path}")
            self.db.conn.commit()
