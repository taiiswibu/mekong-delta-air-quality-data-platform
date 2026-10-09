{{ config(alias='AQI_HOURLY_PATTERN') }}

select
    c.city_slug as queried_city,
    c.city_name,
    c.region,
    t.hour,
    t.time_of_day,
    avg(f.aqi) as avg_aqi,
    avg(f.pm25) as avg_pm25,
    avg(f.pm10) as avg_pm10,
    count(*) as record_count,
    current_timestamp() as aggregated_at
from {{ ref('fact_aqi') }} f
join {{ ref('dim_city') }} c using (city_key)
join {{ ref('dim_time') }} t using (time_key)
group by 1,2,3,4,5
