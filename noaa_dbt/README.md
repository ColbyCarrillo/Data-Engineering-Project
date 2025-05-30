# NOAA DBT Models

This folder contains all **dbt transformations** for the NOAA Weather Data Pipeline project. It defines how raw weather data ingested into PostgreSQL is cleaned, transformed, and prepared for analysis using [dbt](https://www.getdbt.com/).

## Project Structure

noaa_dbt/
├── models/
│ ├── staging/
│ │ ├── stg_stations.sql
│ │ └── stg_daily_weather.sql
│ ├── gold/
│ │ ├── gold_daily_weather_summary.sql
│ │ └── gold_station_gaps.sql
│ ├── staging/
│ │ ├── schema.yml
│ └── gold/
│ └── schema.yml
│ └── README.md
└── dbt_project.yml # Main dbt project config

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

## Highlights
- Modular, maintainable transformations using staging and gold layers

- Reproducible builds and tests

- Detects data gaps using generate_series() and window functions

- Tests for nulls, uniqueness, and relationships

##  Notes
- This is part of a larger data pipeline project using Python, Docker, and PostgreSQL

- For more, see the top-level project README
