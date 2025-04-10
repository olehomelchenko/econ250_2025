{{ config(materialized='view')}}

SELECT
    string_field_0 AS product_category_name,
    string_field_1 AS product_category_name_english
FROM {{ source('asyvak', 'fp_product_category_name_translation') }}
WHERE string_field_0 IS NOT NULL 
AND string_field_1 IS NOT NULL 