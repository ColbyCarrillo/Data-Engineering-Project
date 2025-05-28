# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import os
from datetime import datetime

class NOAAParser:
    def __init__(self, db_client):
        self.db = db_client

    def parse_line(self, line):
        # GSOD .op format: station, wban, year, mo, da, temp, ...
        tokens = line.strip().split()
        if len(tokens) < 7:
            return None  # skip broken lines

        usaf = tokens[0]
        wban = tokens[1]
        date = datetime.strptime(f"{tokens[2]}-{tokens[3]}-{tokens[4]}", "%Y-%m-%d").date()
        temp = float(tokens[5]) if tokens[5] != "9999.9" else None  # missing value

        return {
            "usaf": usaf,
            "wban": wban,
            "date": date,
            "element": "TEMP",
            "value": temp,
            "mflag": None,
            "qflag": None,
            "sflag": None,
            "obs_time": None,
        }

    def parse_folder_and_insert(self, folder_path):
        for file_name in os.listdir(folder_path):
            if not file_name.endswith(".op"):
                continue

            file_path = os.path.join(folder_path, file_name)
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    row = self.parse_line(line)
                    if row and row["value"] is not None:
                        self.db.cursor.execute(
                            """
                            INSERT INTO daily_weather
                            (usaf, wban, date, element, value, mflag, qflag, sflag, obs_time)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING;
                            """,
                            (
                                row["usaf"], row["wban"], row["date"], row["element"],
                                row["value"], row["mflag"], row["qflag"],
                                row["sflag"], row["obs_time"]
                            )
                        )
        self.db.conn.commit()
