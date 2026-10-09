{{ config(alias='STG_API_RAW') }}

select
    event_id,
    queried_city as city_slug,
    raw_payload:data:idx::number as waqi_idx,
    raw_payload:data:city:name::string as station_name,
    raw_payload:data:city:geo[0]::double as lat,
    raw_payload:data:city:geo[1]::double as lon,
    raw_payload:data:city:url::string as url,
    try_to_timestamp_tz(raw_payload:data:time:iso::string) as measured_at,
    try_to_double(raw_payload:data:aqi::string) as aqi,
    nullif(raw_payload:data:dominentpol::string, 'aqi') as dominant_pollutant,
    try_to_double(raw_payload:data:iaqi:pm25:v::string) as pm25,
    try_to_double(raw_payload:data:iaqi:pm10:v::string) as pm10,
    try_to_double(raw_payload:data:iaqi:co:v::string) as co,
    try_to_double(raw_payload:data:iaqi:no2:v::string) as no2,
    try_to_double(raw_payload:data:iaqi:o3:v::string) as o3,
    try_to_double(raw_payload:data:iaqi:so2:v::string) as so2,
    try_to_double(raw_payload:data:iaqi:h:v::string) as humidity,
    try_to_double(raw_payload:data:iaqi:t:v::string) as temperature,
    try_to_double(raw_payload:data:iaqi:p:v::string) as pressure,
    try_to_double(raw_payload:data:iaqi:w:v::string) as wind,
    'api' as source,
    ingested_at
from {{ source('bronze', 'api_raw') }}
where raw_payload:data:aqi is not null
qualify row_number() over (partition by event_id order by ingested_at desc) = 1
