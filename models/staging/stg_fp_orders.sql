{{ config(materialized='view')}}

SELECT
    order_id,
    customer_id,
    COALESCE(order_status, 'undefined') AS order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    order_status = 'delivered' AS is_delivered,
    CASE
        WHEN order_purchase_timestamp IS NOT NULL AND order_delivered_customer_date IS NOT NULL
        THEN TIMESTAMP_DIFF(order_delivered_customer_date, order_purchase_timestamp, HOUR)
        ELSE NULL
    END AS delivery_time_hours
FROM {{ source('asyvak', 'fp_orders') }}