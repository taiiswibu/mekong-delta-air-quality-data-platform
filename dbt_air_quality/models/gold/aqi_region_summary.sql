{{ config(alias='AQI_REGION_SUMMARY') }}

select
    c.region,
    d.year,
    d.month,
    avg(f.aqi) as avg_aqi,
    max(f.aqi) as max_aqi,
    min(f.aqi) as min_aqi,
    avg(f.pm25) as avg_pm25,
    avg(f.pm10) as avg_pm10,
    count(distinct c.city_key) as city_count,
    count(*) as record_count,
    mode(f.dominant_pollutant) as dominant_pollutant,
    {{ aqi_category('avg(f.aqi)') }} as pollution_level,
    current_timestamp() as aggregated_at
from {{ ref('fact_aqi') }} f
join {{ ref('dim_city') }} c using (city_key)
join {{ ref('dim_date') }} d using (date_key)
group by 1,2,3
