# NOAA DBT Models

This folder contains all **dbt transformations** for the NOAA Weather Data Pipeline project. It defines how raw weather data ingested into PostgreSQL is cleaned, transformed, and prepared for analysis using [dbt](https://www.getdbt.com/).

## Project Structure

noaa_dbt/
├── models/
│ ├── staging/ # Raw data cleaned and standardized
│ │ ├── stg_stations.sql
│ │ └── stg_daily_weather.sql
│ ├── gold/ # Aggregated, analysis-ready tables
│ │ └── gold_daily_weather_summary.sql
│ │ └── gold_station_gaps.sql
├── dbt_project.yml # Main dbt project config

## How to Run

1. **Install dbt (Postgres adapter)**  
   ```bash
   pip install dbt-postgres
   ```
2. **Set up your profiles.yml**

3. **Run/Test dbt models**
    ```bash
    dbt run
    dbt test
    ```

##  Notes
- This is part of a larger data pipeline project using Python, Docker, and PostgreSQL

- For more, see the top-level project README
