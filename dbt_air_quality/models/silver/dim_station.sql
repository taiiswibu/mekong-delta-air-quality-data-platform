{{ config(alias='DIM_STATION') }}

with src as (
    select city_slug, waqi_idx, station_name, lat, lon, url, source, measured_at
    from {{ ref('stg_api_raw') }}
    union all
    select city_slug, waqi_idx, station_name, lat, lon, url, source, measured_at
    from {{ ref('stg_historical') }}
), ranked as (
    select
        s.*,
        min(measured_at) over (partition by city_slug, waqi_idx) as first_seen,
        max(measured_at) over (partition by city_slug, waqi_idx) as last_seen,
        row_number() over (
            partition by city_slug, waqi_idx
            order by case source when 'api' then 1 else 2 end, measured_at desc
        ) as rn
    from src s
    where waqi_idx is not null
)
select
    abs(hash(r.city_slug, r.waqi_idx)) as station_key,
    r.waqi_idx,
    c.city_key,
    r.station_name,
    r.lat,
    r.lon,
    r.url,
    r.source,
    r.first_seen,
    r.last_seen,
    true as is_active
from ranked r
join {{ ref('dim_city') }} c on c.city_slug = r.city_slug
where r.rn = 1
