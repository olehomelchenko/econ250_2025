{{
  config(
    materialized='table',
    partition_by={
      "field": "order_purchase_timestamp",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["order_status", "customer_state", "is_delivered"],
    require_partition_filter=true
  )
}}

WITH order_payments AS (
  SELECT
    order_id,
    ARRAY_AGG(STRUCT(
      payment_sequential,
      payment_type,
      payment_installments,
      payment_value
    )) AS payments,
    SUM(payment_value) AS total_payment_value
  FROM {{ ref('stg_fp_order_payments') }}
  GROUP BY order_id
),

order_items AS (
  SELECT
    i.order_id,
    ARRAY_AGG(STRUCT(
      i.order_item_id,
      i.product_id,
      i.seller_id,
      s.seller_zip_code_prefix,
      s.seller_city,
      s.seller_state,
      i.shipping_limit_date,
      i.price,
      i.freight_value,
      i.total_item_value,
      p.product_category_name,
      t.product_category_name_english,
      p.product_name_length,
      p.product_description_length,
      p.product_photos_qty,
      p.product_weight_g,
      p.product_length_cm,
      p.product_height_cm,
      p.product_width_cm
    )) AS items,
    COUNT(*) AS item_count,
    SUM(i.price + i.freight_value) AS total_order_value
  FROM {{ ref('stg_fp_order_items') }} i
  LEFT JOIN {{ ref('stg_fp_products') }} p ON i.product_id = p.product_id
  LEFT JOIN {{ ref('stg_fp_product_category_name_translation') }} t 
    ON p.product_category_name = t.product_category_name
  LEFT JOIN {{ ref('stg_fp_sellers') }} s ON i.seller_id = s.seller_id
  GROUP BY i.order_id
)

SELECT
  o.order_id,
  o.customer_id,
  c.customer_unique_id,
  c.customer_zip_code_prefix,
  c.customer_city,
  c.customer_state,
  o.order_status,
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,
  o.is_delivered,
  o.delivery_time_hours,
  op.payments,
  oi.items,
  op.total_payment_value,
  oi.item_count,
  oi.total_order_value
FROM {{ ref('stg_fp_orders') }} o
LEFT JOIN {{ ref('stg_fp_customers') }} c ON o.customer_id = c.customer_id
LEFT JOIN order_payments op ON o.order_id = op.order_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id