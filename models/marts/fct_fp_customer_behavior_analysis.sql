{{ config(materialized='table')}}

WITH customer_metrics AS (
  SELECT
    customer_unique_id,
    customer_state,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(total_order_value) AS lifetime_value,
    MIN(DATE(order_purchase_timestamp)) AS first_order_date,
    MAX(DATE(order_purchase_timestamp)) AS last_order_date,
    DATE_DIFF(MAX(DATE(order_purchase_timestamp)), MIN(DATE(order_purchase_timestamp)), DAY) AS customer_duration_days
  FROM 
    {{ ref("int_fp_sales_full") }} 
  WHERE 
    is_delivered = TRUE
    AND DATE(order_purchase_timestamp) BETWEEN DATE('2017-01-01') AND DATE('2018-12-31')
  GROUP BY 
    customer_unique_id, customer_state
),

customer_segmentation AS (
  SELECT
    *,
    CASE
      WHEN order_count = 1 THEN 'New'
      WHEN order_count > 1 AND customer_duration_days <= 90 THEN 'Repeat (3m)'
      WHEN order_count > 1 AND customer_duration_days > 90 THEN 'Loyal (3m+)'
    END AS customer_type,
    
    lifetime_value / NULLIF(order_count, 0) AS avg_order_value,
    CASE
      WHEN customer_duration_days > 0 THEN order_count / (customer_duration_days / 30.0)
      ELSE NULL
    END AS orders_per_month,
    
    CASE
      WHEN lifetime_value >= PERCENTILE_CONT(lifetime_value, 0.9) OVER() THEN 'Top 10%'
      WHEN lifetime_value >= PERCENTILE_CONT(lifetime_value, 0.7) OVER() THEN 'High Value'
      WHEN lifetime_value >= PERCENTILE_CONT(lifetime_value, 0.4) OVER() THEN 'Medium Value'
      ELSE 'Low Value'
    END AS value_segment
  FROM 
    customer_metrics
)

SELECT
  customer_unique_id,
  customer_state,
  customer_type,
  value_segment,
  order_count,
  lifetime_value,
  avg_order_value,
  first_order_date,
  last_order_date,
  customer_duration_days,
  orders_per_month,
  
FROM 
  customer_segmentation
WHERE 
  order_count > 0
ORDER BY 
  first_order_date DESC, lifetime_value DESC