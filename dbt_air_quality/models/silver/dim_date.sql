{{ config(alias='DIM_DATE') }}

with dates as (
    select dateadd(day, seq4(), '2019-01-01'::date) as full_date
    from table(generator(rowcount => 6209))
)
select
    to_number(to_char(full_date, 'YYYYMMDD')) as date_key,
    full_date,
    year(full_date) as year,
    month(full_date) as month,
    monthname(full_date) as month_name,
    day(full_date) as day_of_month,
    dayofweekiso(full_date) as day_of_week,
    dayname(full_date) as day_name,
    quarter(full_date) as quarter,
    weekofyear(full_date) as week_of_year,
    case quarter(full_date)
      when 1 then 'Jan-Mar'
      when 2 then 'Apr-Jun'
      when 3 then 'Jul-Sep'
      else 'Oct-Dec'
    end as season
from dates
