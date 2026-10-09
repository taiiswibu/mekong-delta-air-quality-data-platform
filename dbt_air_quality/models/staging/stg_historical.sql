{{ config(alias='STG_HISTORICAL') }}

select
    md5(concat_ws('|', c.station_id, c.location, c.data_time_s)) as event_id,
    coalesce(m.conformed_city, lower(replace(trim(c.location), ' ', '-'))) as city_slug,
    try_to_number(c.station_id) as waqi_idx,
    c.station_name,
    null::double as lat,
    null::double as lon,
    c.url,
    try_to_timestamp_tz(c.data_time_s) as measured_at,
    try_to_double(c.aqi_index) as aqi,
    nullif(c.dominant_pollutant, 'aqi') as dominant_pollutant,
    try_to_double(c.pm25) as pm25,
    try_to_double(c.pm10) as pm10,
    try_to_double(c.co) as co,
    try_to_double(c.no2) as no2,
    try_to_double(c.o3) as o3,
    try_to_double(c.so2) as so2,
    try_to_double(c.humidity) as humidity,
    try_to_double(c.temperature) as temperature,
    try_to_double(c.pressure) as pressure,
    try_to_double(c.wind) as wind,
    'kaggle' as source,
    c.ingested_at
from {{ source('bronze', 'historical_csv') }} c
left join {{ source('control', 'city_map') }} m
  on m.source_city = c.location
where try_to_number(c.station_id) is not null
