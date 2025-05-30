{{ config(materialized='view') }}

with weather as (
    select *
    from {{ ref('stg_daily_weather') }}
),

stations as (
    select *
    from {{ ref('stg_stations') }}
),

joined as (
    select
        w.usaf,
        w.wban,
        s.station_name,
        s.state,
        s.country,
        w.date,
        w.element,
        w.value
    from weather w
    left join stations s
        on w.usaf = s.usaf
        and w.wban = s.wban
),

summary as (
    select
        station_name,
        state,
        country,
        element,
        avg(value) as avg_value,
        min(value) as min_value,
        max(value) as max_value,
        count(*) as num_observations
    from joined
    group by 1, 2, 3, 4
)

select * from summary
