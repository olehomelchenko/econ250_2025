{{
  config(
    materialized = 'table'
    )
}}

-- Assume we'd like to pay close attention to the purchasers on the website by analyzing their conversion paths
with cte_orders as (
select visitId, hitNumber, transaction, product
from {{ source('test_dataset', 'week5_web_transactions') }}
where transaction.transactionRevenue is not null
),
cte_visits as (
select date, fullvisitorId, visitId, operatingSystem, deviceCategory
from {{ source('test_dataset', 'week5_web_visits') }} 
where visitId in (select visitid, from cte_orders)
),
cte_hits as (
SELECT
    date,
    visitId,
    ARRAY_AGG(STRUCT( hitNumber,
        page.pagePath,
        eventInfo,
        TRANSACTION,
        cte_orders.product 
        )
    ORDER BY
      hitNumber) hit_info
  FROM {{ source('test_dataset', 'week5_hits') }}
    left join cte_orders using(visitId, hitNumber)
    group by date, visitId
)

select * from cte_visits
left join cte_hits using(date, visitId)