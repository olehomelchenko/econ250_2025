{{config(materialized='table')}}

select
publish_month,
round(avg(days_until_updated)) avg_update_period_total,
round(avg(if(is_updated = 1, days_until_updated, null))) avg_update_period,
from {{ref('int_arxiv_duration_llm')}}
where is_updated = 0
group by 1