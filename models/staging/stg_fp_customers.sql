{{ config(materialized='view')}}

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    CASE 
        WHEN customer_city IS NULL THEN 'unknown_city'
        ELSE customer_city 
    END AS customer_city,
    CASE 
        WHEN customer_state IS NULL THEN 'unknown_state'
        ELSE customer_state 
    END AS customer_state,
FROM {{ source('asyvak', 'fp_customers') }}