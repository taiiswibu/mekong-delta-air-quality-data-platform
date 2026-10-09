{{ config(alias='STATION_SUMMARY') }}

with station_month as (
    select
        s.waqi_idx,
        s.station_name,
        c.city_slug as queried_city,
        c.city_name,
        c.region,
        s.lat,
        s.lon,
        d.year,
        d.month,
        avg(f.aqi) as avg_aqi,
        max(f.aqi) as max_aqi,
        min(f.aqi) as min_aqi,
        avg(f.pm25) as avg_pm25,
        avg(f.pm10) as avg_pm10,
        avg(f.humidity) as avg_humidity,
        avg(f.temperature) as avg_temperature,
        count(*) as record_count,
        mode(f.dominant_pollutant) as dominant_pollutant
    from {{ ref('fact_aqi') }} f
    join {{ ref('dim_station') }} s using (station_key)
    join {{ ref('dim_city') }} c on c.city_key = f.city_key
    join {{ ref('dim_date') }} d using (date_key)
    group by 1,2,3,4,5,6,7,8,9
)
select
    *,
    {{ aqi_category('avg_aqi') }} as pollution_level,
    dense_rank() over (partition by queried_city, year, month order by avg_aqi desc) as rank_in_city,
    current_timestamp() as aggregated_at
from station_month
