{{ config(alias='AQI_POLLUTANT_SUMMARY') }}

select
    c.city_slug as queried_city,
    c.city_name,
    d.year,
    d.month,
    avg(f.pm25) as avg_pm25,
    avg(f.pm10) as avg_pm10,
    avg(f.no2) as avg_no2,
    avg(f.o3) as avg_o3,
    avg(f.so2) as avg_so2,
    avg(f.co) as avg_co,
    count(*) as record_count,
    current_timestamp() as aggregated_at
from {{ ref('fact_aqi') }} f
join {{ ref('dim_city') }} c using (city_key)
join {{ ref('dim_date') }} d using (date_key)
group by 1,2,3,4
