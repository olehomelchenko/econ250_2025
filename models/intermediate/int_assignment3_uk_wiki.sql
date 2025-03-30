{{ config(materialized='view')}}

with base_data as (
  select 
    src,
    title,
    datehour,
    views,
    date,
    day_of_week,
    hour_of_day,
    
    case
      when regexp_contains(title, r':[_ ]') then false
      when regexp_contains(title, r'^[^:]+:[^_\s]') then true
      else false
    end as is_meta_page,
    
    case
      when regexp_contains(title, r'^Вікіпедія:') then 'Вікіпедія'
      when regexp_contains(title, r'^Категорія:') then 'Категорія'
      when regexp_contains(title, r'^Файл:|^File:') then 'Файл'
      when regexp_contains(title, r'^Обговорення:') then 'Обговорення'
      when regexp_contains(title, r'^Користувач:|^Користувачка:|^User:') then 'Користувач'
      when regexp_contains(title, r'^Шаблон:|^Template:') then 'Шаблон'
      when regexp_contains(title, r'^Довідка:|^Help:') then 'Довідка'
      when regexp_contains(title, r'^Портал:') then 'Портал'
      when regexp_contains(title, r'^Модуль:|^Module:') then 'Модуль'
      when regexp_contains(title, r'^Спеціальна:|^Special:') then 'Спеціальна'
      when regexp_contains(title, r'^MediaWiki:') then 'MediaWiki'
      when regexp_contains(title, r'^Обговорення_користувача:|^Обговорення_користувачки:') then 'Обговорення користувача'
      else null
    end as meta_page_type
  from {{ ref('stg_assignment3_uk_wiki') }}
)

select * from base_data