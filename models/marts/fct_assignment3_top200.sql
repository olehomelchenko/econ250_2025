{{ config(materialized='table')}}

with article_views as (
  select
    title,
    sum(views) as total_views,
    sum(case when src = 'mobile' then views else 0 end) as total_mobile_views
  from {{ ref('int_assignment3_uk_wiki') }}
  where is_meta_page = false
  group by title
),

top_articles as (
  select
    title,
    total_views,
    total_mobile_views,
    (total_mobile_views / total_views) * 100 as mobile_percentage
  from article_views
  order by total_views desc
  limit 200
)

select
  title,
  total_views,
  total_mobile_views,
  round(mobile_percentage, 2) as mobile_percentage
from top_articles
order by mobile_percentage asc