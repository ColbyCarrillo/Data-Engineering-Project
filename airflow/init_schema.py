import os
from dotenv import load_dotenv
from pipeline.weather_pipeline import WeatherPipeline
from db.postgres_client import PostgresClient

if __name__ == "__main__":
    load_dotenv()
        
    db_config = {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }
    db = PostgresClient(db_config)
    pipeline = WeatherPipeline(db)
    pipeline.setup_schema("/opt/airflow/db/NOAAschema.sql") 
    pipeline.close()