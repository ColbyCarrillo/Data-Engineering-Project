{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('public', 'daily_weather') }}

),

renamed as (

    select
        usaf,
        wban,
        date,
        element,
        value,
        mflag,
        qflag,
        sflag,
        obs_time,
        ingestion_log_id,
        inserted_at

    from source

)

select * from renamed
