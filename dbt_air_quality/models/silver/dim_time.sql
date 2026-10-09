{{ config(alias='DIM_TIME') }}

with minutes as (
    select seq4() as minute_of_day
    from table(generator(rowcount => 1440))
)
select
    floor(minute_of_day / 60) * 100 + mod(minute_of_day, 60) as time_key,
    floor(minute_of_day / 60) as hour,
    mod(minute_of_day, 60) as minute,
    case
      when floor(minute_of_day / 60) between 5 and 11 then 'Morning'
      when floor(minute_of_day / 60) between 12 and 16 then 'Afternoon'
      when floor(minute_of_day / 60) between 17 and 21 then 'Evening'
      else 'Night'
    end as time_of_day
from minutes
