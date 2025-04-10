{{ config(materialized='view')}}

SELECT 
    order_id,
    payment_sequential,
    COALESCE(payment_type, 'not_defined') AS payment_type,
    payment_installments,
    payment_value
FROM {{ source('asyvak', 'fp_order_payments') }}