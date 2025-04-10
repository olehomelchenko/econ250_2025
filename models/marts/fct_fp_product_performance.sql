{{ config(materialized='table')}}

SELECT
  FORMAT_DATE('%Y-%m', DATE(order_purchase_timestamp)) AS month,
  product_category_name_english AS product_category,
  customer_state,
  COUNT(DISTINCT order_id) AS orders_with_product,
  SUM(i.price) AS total_product_revenue,
  SUM(i.freight_value) AS total_freight_value,
  SUM(i.price) / NULLIF(COUNT(DISTINCT order_id), 0) AS avg_product_value_per_order,
  COUNT(DISTINCT customer_unique_id) AS unique_customers,
  COUNT(i.order_item_id) AS total_units_sold
FROM {{ ref("int_fp_sales_full") }},
  UNNEST(items) AS i
WHERE 
  i.product_category_name_english IS NOT NULL
  AND is_delivered = TRUE
  AND DATE(order_purchase_timestamp) BETWEEN DATE('2017-01-01') AND DATE('2018-12-31')
GROUP BY 
  month, product_category, customer_state
ORDER BY 
  month DESC, total_product_revenue DESC
