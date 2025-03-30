{{ config(materialized='view')}}

select
  hour_of_day,
  sum(views) as total_views,
  sum(case when src = 'mobile' then views else 0 end) as total_mobile_views,
  round((sum(case when src = 'mobile' then views else 0 end) / sum(views)) * 100, 2) as mobile_percentage
from {{ ref('int_assignment3_uk_wiki') }}
where title = 'AWStats'  
group by hour_of_day
order by hour_of_day