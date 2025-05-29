# NOAA Weather Data Pipeline

A complete, end-to-end data engineering pipeline built to demonstrate proficiency in ingestion, transformation, and loading of real-world weather data using Python, Docker, PostgreSQL, and industry-standard tools.

## Project Overview

This pipeline ingests historical weather data from the [NOAA GSOD](https://www.ncei.noaa.gov/data/global-summary-of-the-day/) dataset, processes it, and loads it into a PostgreSQL database for analysis and dashboarding.


## Getting Started

### [1] Clone the Repository
```bash
git clone https://github.com/ColbyCarrillo/Data-Engineering-Project.git
cd Data-Engineering-Project
```

### [2] Set Up Environment Variables

Create a .env file in the root directory with the following content:

```bash
STATION_URL=https://www.ncei.noaa.gov/pub/data/gsod/isd-history.csv
ARCHIVE_BASE_URL=https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/
```

### [3] Install Python Dependencies

Set up a virtual environment and install the required packages:

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### [4] Start PostgreSQL Using Docker
Make sure Docker is running. Then launch the database container:

```bash
docker-compose up -d
```

### [5] Run the Data Pipeline

```bash
python main.py
```

### This will:
- Create the necessary database schema
- Download and insert station metadata
- Download and insert daily weather data for U.S. stations only
- Log all processed files to avoid duplicates on rerun

### Rerunning the Pipeline
The pipeline is safe to rerun. Files that have already been processed will be skipped automatically based on the log table.

To fully reset the environment:

```bash
docker-compose down -v
```

#### Note: If you've previously downloaded files or used a persistent Docker volume, you may need to manually remove the following directories before rerunning:
```bash
rm -rf ./postgres-data ./data
```

Then rerun: 

```bash
docker-compose up -d
python main.py
```

## Project Structure

```text
├── db/
│   └── postgres_client.py
│   └── NOAAschema.sql
├── ingestion/
│   ├── noaa_downloader.py
│   └── noaa_parser.py
├── pipeline/
│   └── weather_pipeline.py
├── data/
│   └── (downloaded data here)
├── .env
├── .gitignore
├── main.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```


## This pipeline demonstrates how to

- Design and run scalable ingestion processes
- Work with public datasets and transform structured data
- Build rerunnable, modular pipelines with logging
- Interface with PostgreSQL in Python using best practices
