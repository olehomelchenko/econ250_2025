{{ config(materialized='view')}}

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    price + freight_value AS total_item_value,
FROM {{ source('asyvak', 'fp_order_items') }}
WHERE price >= 0 AND freight_value >= 0
