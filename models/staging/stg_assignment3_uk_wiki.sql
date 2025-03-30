{{ config(materialized='view')}}

with desktop as (
  select
    'desktop' as src,
    title,
    datehour,
    views,
    date(datehour) as date,
    extract(dayofweek from datehour) as day_of_week_raw,
    case 
      when extract(dayofweek from datehour) = 1 then 7  
      else extract(dayofweek from datehour) - 1          
    end as day_of_week,
    extract(hour from datehour) as hour_of_day
  from {{ source('test_dataset', 'assignment3_input_uk') }}
),

mobile as (
  select
    'mobile' as src,
    title,
    datehour,
    views,
    date(datehour) as date,
    extract(dayofweek from datehour) as day_of_week_raw,
    case 
      when extract(dayofweek from datehour) = 1 then 7  
      else extract(dayofweek from datehour) - 1          
    end as day_of_week,
    extract(hour from datehour) as hour_of_day
  from {{ source('test_dataset', 'assignment3_input_uk_m') }}
)

select * from desktop
union all
select * from mobile