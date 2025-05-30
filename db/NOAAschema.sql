CREATE TABLE IF NOT EXISTS file_ingestion_log (
    id SERIAL PRIMARY KEY,
    project_name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    num_records INTEGER,
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stations (
    usaf VARCHAR(10),
    wban VARCHAR(10),
    name TEXT,
    country VARCHAR(10),
    state VARCHAR(10),
    icao VARCHAR(10),
    lat FLOAT,
    lon FLOAT,
    elev FLOAT,
    ingestion_log_id INT REFERENCES file_ingestion_log(id),
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (usaf, wban)
);

CREATE TABLE IF NOT EXISTS daily_weather (
    usaf TEXT,
    wban TEXT,
    date DATE,
    element TEXT,
    value INTEGER,
    mflag TEXT,
    qflag TEXT,
    sflag TEXT,
    obs_time TEXT,
    ingestion_log_id INT REFERENCES file_ingestion_log(id),
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (usaf, wban, date, element)
);
