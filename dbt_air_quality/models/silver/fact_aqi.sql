{{
  config(
    alias='FACT_AQI',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='measurement_key',
    on_schema_change='sync_all_columns'
  )
}}

with combined as (
    select * from {{ ref('stg_api_raw') }}
    union all
    select * from {{ ref('stg_historical') }}
), candidate as (
    select *
    from combined
    where measured_at is not null and waqi_idx is not null
    {% if is_incremental() %}
      and ingested_at >= (
        select dateadd(hour, -24, coalesce(max(ingested_at), '1900-01-01'::timestamp_tz)) from {{ this }}
      )
    {% endif %}
), deduped as (
    select *
    from candidate
    qualify row_number() over (
      partition by city_slug, waqi_idx, measured_at
      order by case source when 'api' then 1 else 2 end, ingested_at desc
    ) = 1
)
select
    md5(concat_ws('|', d.city_slug, d.waqi_idx::string, d.measured_at::string)) as measurement_key,
    dt.date_key,
    tm.time_key,
    c.city_key,
    s.station_key,
    d.measured_at,
    d.aqi,
    {{ aqi_category('d.aqi') }} as pollution_level,
    d.dominant_pollutant,
    d.pm25,
    d.pm10,
    d.co,
    d.no2,
    d.o3,
    d.so2,
    d.humidity,
    d.temperature,
    d.pressure,
    d.wind,
    d.source,
    d.ingested_at
from deduped d
join {{ ref('dim_city') }} c
  on c.city_slug = d.city_slug
join {{ ref('dim_station') }} s
  on s.city_key = c.city_key and s.waqi_idx = d.waqi_idx
join {{ ref('dim_date') }} dt
  on dt.full_date = to_date(d.measured_at)
join {{ ref('dim_time') }} tm
  on tm.time_key = hour(d.measured_at) * 100 + minute(d.measured_at)
