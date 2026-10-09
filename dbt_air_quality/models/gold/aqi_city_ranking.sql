{{ config(alias='AQI_CITY_RANKING') }}

with monthly as (
    select
        c.city_slug as queried_city,
        c.city_name,
        c.region,
        d.year,
        d.month,
        avg(f.aqi) as avg_aqi,
        max(f.aqi) as max_aqi,
        min(f.aqi) as min_aqi,
        avg(f.pm25) as avg_pm25,
        avg(f.pm10) as avg_pm10,
        count(*) as record_count,
        count_if(f.pollution_level = 'Good') as records_good,
        count_if(f.pollution_level = 'Moderate') as records_moderate,
        count_if(f.pollution_level = 'Unhealthy for Sensitive Groups') as records_sensitive,
        count_if(f.pollution_level = 'Unhealthy') as records_unhealthy,
        count_if(f.pollution_level = 'Very Unhealthy') as records_very_unhealthy,
        count_if(f.pollution_level = 'Hazardous') as records_hazardous,
        mode(f.dominant_pollutant) as dominant_pollutant
    from {{ ref('fact_aqi') }} f
    join {{ ref('dim_city') }} c using (city_key)
    join {{ ref('dim_date') }} d using (date_key)
    group by 1,2,3,4,5
)
select
    *,
    {{ aqi_category('avg_aqi') }} as pollution_level,
    dense_rank() over (partition by year, month order by avg_aqi desc) as aqi_rank,
    dense_rank() over (partition by year, month order by avg_pm25 desc nulls last) as pm25_rank,
    current_timestamp() as aggregated_at
from monthly
