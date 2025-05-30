{{ config(materialized='table') }}

with base as (

    select
        usaf,
        wban,
        date,
        max(case when element = 'TEMP' then value / 10.0 end) as avg_temp_c,
        max(case when element = 'DEWP' then value / 10.0 end) as dew_point_c,
        max(case when element = 'PRCP' then value / 10.0 end) as precipitation_mm,
        max(case when element = 'MAX' then value / 10.0 end) as max_temp_c,
        max(case when element = 'MIN' then value / 10.0 end) as min_temp_c
    from {{ ref('stg_daily_weather') }}
    group by usaf, wban, date

)

select * from base
