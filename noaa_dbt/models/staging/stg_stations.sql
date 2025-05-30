{{ config(materialized='view') }}

with raw_stations as (

    select
        usaf,
        wban,
        name,
        country,
        state,
        icao,
        lat,
        lon,
        elev
    from {{ source('public', 'stations') }}

)

select
    usaf,
    wban,
    name as station_name,
    country,
    state,
    icao,
    cast(lat as float) as latitude,
    cast(lon as float) as longitude,
    cast(elev as float) as elevation
from raw_stations
where country = 'US'