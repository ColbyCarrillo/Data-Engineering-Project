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
    PRIMARY KEY (usaf, wban, date, element)
);
