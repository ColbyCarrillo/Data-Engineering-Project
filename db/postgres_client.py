#pylint: disable=line-too-long, too-many-arguments, too-many-positional-arguments, missing-module-docstring, missing-class-docstring, missing-function-docstring

import datetime
import psycopg2

class PostgresClient:
    def __init__(self, db_config):
        self.conn = psycopg2.connect(**db_config)
        self.cursor = self.conn.cursor()

    def create_tables_from_file(self, schema_path):
        with open(schema_path, 'r', encoding='utf-8') as f:
            self.cursor.execute(f.read())
        self.conn.commit()

    def insert_station(self, station, ingestion_id):
        self.cursor.execute(
            """
            INSERT INTO stations (
                usaf, wban, name, country, state, icao, lat, lon, elev,
                ingestion_log_id, inserted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (usaf, wban) DO NOTHING;
            """,
            (
                station['USAF'],
                station['WBAN'],
                station['STATION NAME'],
                station['CTRY'],
                station.get('STATE'),
                station.get('ICAO'),
                station.get('LAT'),
                station.get('LON'),
                station.get('ELEV(M)'),
                ingestion_id,
                datetime.datetime.now(datetime.timezone.utc)
            )
        )

    def insert_weather_record(self, record, ingestion_id):
        self.cursor.execute(
            """
            INSERT INTO daily_weather (
                usaf, wban, date, element, value, mflag, qflag, sflag, obs_time,
                ingestion_log_id, inserted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (usaf, wban, date, element) DO NOTHING;
            """,
            (
                record["usaf"], record["wban"], record["date"],
                record["element"], record["value"],
                record.get("mflag"), record.get("qflag"),
                record.get("sflag"), record.get("obs_time"),
                ingestion_id,
                datetime.datetime.now(datetime.timezone.utc)
            )
        )

    def log_ingestion(self, project_name, file_path, num_records, success=True, error_message=None):
        self.cursor.execute(
            """
            INSERT INTO file_ingestion_log (project_name, file_path, num_records, success, error_message)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (project_name, file_path, num_records, success, error_message)
        )
        ingestion_id = self.cursor.fetchone()[0]
        self.conn.commit()
        return ingestion_id

    def update_ingestion_record(self, ingestion_id, num_records, success=True, error_message=None):
        self.cursor.execute(
            """
            UPDATE file_ingestion_log
            SET num_records = %s,
                success = %s,
                error_message = %s,
                inserted_at = CURRENT_TIMESTAMP
            WHERE id = %s;
            """,
            (num_records, success, error_message, ingestion_id)
        )
        self.conn.commit()

    def is_file_already_ingested(self, project_name, file_path):
        self.cursor.execute(
            """
            SELECT 1 FROM file_ingestion_log
            WHERE project_name = %s AND file_path = %s AND success = TRUE
            LIMIT 1;
            """,
            (project_name, file_path)
        )
        return self.cursor.fetchone() is not None

    def close(self):
        self.cursor.close()
        self.conn.close()
