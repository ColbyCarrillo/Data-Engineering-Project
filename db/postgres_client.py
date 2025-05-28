import psycopg2

#pylint: disable=line-too-long, missing-module-docstring, missing-class-docstring, missing-function-docstring
class PostgresClient:
    def __init__(self, db_config):
        self.conn = psycopg2.connect(**db_config)
        self.cursor = self.conn.cursor()

    def create_tables_from_file(self, schema_path):
        with open(schema_path, 'r', encoding='utf-8') as f:
            self.cursor.execute(f.read())
        self.conn.commit()

    def insert_station(self, station):
        self.cursor.execute(
                            """
                            INSERT INTO stations (usaf, wban, name, country, state, icao, lat, lon, elev)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                            )
                        )

    def close(self):
        self.cursor.close()
        self.conn.close()
