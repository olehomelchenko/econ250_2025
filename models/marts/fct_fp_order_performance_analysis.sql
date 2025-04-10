{{ config(materialized='table')}}

SELECT
  DATE(order_purchase_timestamp) AS order_date,
  customer_state,
  order_status,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(total_order_value) AS total_revenue,
  AVG(total_order_value) AS avg_order_value,
  SUM(item_count) AS total_items_sold
FROM 
  {{ ref("int_fp_sales_full") }}
WHERE 
  is_delivered = TRUE
  AND DATE(order_purchase_timestamp) BETWEEN DATE('2017-01-01') AND DATE('2018-12-31')
GROUP BY 
  order_date, customer_state, order_status
ORDER BY 
  order_date DESC, total_revenue DESC