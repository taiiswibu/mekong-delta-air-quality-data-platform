{{ config(alias='DIM_CITY') }}

select
    abs(hash(city_slug)) as city_key,
    city_slug,
    city_name,
    region,
    lat,
    lon
from {{ source('control', 'city_reference') }}
