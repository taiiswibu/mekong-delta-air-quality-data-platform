{{ config(alias='AQI_LATEST_STATUS') }}

select
    c.city_slug as queried_city,
    c.city_name,
    c.region,
    c.lat as city_lat,
    c.lon as city_lon,
    s.station_name,
    f.measured_at,
    f.aqi,
    f.pollution_level,
    f.dominant_pollutant,
    f.pm25,
    f.pm10,
    f.humidity,
    f.temperature,
    f.source,
    f.ingested_at
from {{ ref('fact_aqi') }} f
join {{ ref('dim_city') }} c using (city_key)
join {{ ref('dim_station') }} s using (station_key)
qualify row_number() over (partition by c.city_key order by f.measured_at desc, f.ingested_at desc) = 1
