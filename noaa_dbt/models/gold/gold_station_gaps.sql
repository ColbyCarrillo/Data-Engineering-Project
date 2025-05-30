{{ config(materialized='view') }}

WITH all_dates AS (
    SELECT 
        usaf,
        wban,
        generate_series(
            MIN(date)::date,
            MAX(date)::date,
            INTERVAL '1 day'
        ) AS date
    FROM {{ ref('stg_daily_weather') }}
    GROUP BY usaf, wban
),

missing_dates AS (
    SELECT 
        d.usaf,
        d.wban,
        d.date
    FROM all_dates d
    LEFT JOIN {{ ref('stg_daily_weather') }} w
        ON d.usaf = w.usaf AND d.wban = w.wban AND d.date = w.date
    WHERE w.date IS NULL
),

with_gaps AS (
    SELECT *,
           date - (ROW_NUMBER() OVER (PARTITION BY usaf, wban ORDER BY date) * INTERVAL '1 day') AS grp
    FROM missing_dates
),

gap_ranges AS (
    SELECT 
        usaf,
        wban,
        MIN(date) AS gap_start,
        MAX(date) AS gap_end,
        COUNT(*) AS gap_length
    FROM with_gaps
    GROUP BY usaf, wban, grp
)

SELECT *
FROM gap_ranges
ORDER BY usaf, wban, gap_start
