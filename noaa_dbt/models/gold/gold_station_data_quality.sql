{{ config(materialized='view') }}

with date_range as (
    select
        min(date) as min_date,
        max(date) as max_date
    from {{ ref('stg_daily_weather') }}
),

expected_days as (
    select
        s.usaf,
        s.wban,
        dr.min_date,
        dr.max_date,
        -- Calculate the number of days expected for each station
        (dr.max_date - dr.min_date + 1) as total_days_expected
    from {{ ref('stg_stations') }} s
    cross join date_range dr
),

actual_days as (
    select
        usaf,
        wban,
        count(distinct date) as actual_days_reported,
        max(date) as last_report_date
    from {{ ref('stg_daily_weather') }}
    group by usaf, wban
)

select
    e.usaf,
    e.wban,
    e.total_days_expected,
    coalesce(a.actual_days_reported, 0) as actual_days_reported,
    round(coalesce(a.actual_days_reported, 0) * 1.0 / nullif(e.total_days_expected, 0), 4) as data_completeness_ratio,
    a.last_report_date
from expected_days e
left join actual_days a
  on e.usaf = a.usaf and e.wban = a.wban
