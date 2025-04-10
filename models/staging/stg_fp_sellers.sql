{{ config(materialized='view')}}

SELECT
    seller_id,
    seller_zip_code_prefix,
    CASE 
      WHEN LOWER(seller_city) IN ('sp / sp', 'sao paulo / sao paulo', 'sbc/sp', 'sao paulo - sp', 'sp', 'são paulo', 'sao paulop', 'sao pauo', 'sao paulo sp', 'sao  paulo', 'sao paluo') THEN 'sao paulo'
      WHEN LOWER(seller_city) IN ('rio de janeiro / rio de janeiro') THEN 'rio de janeiro'
      WHEN LOWER(seller_city) IN ('santo andre/sao paulo') THEN 'santo andre'
      WHEN LOWER(seller_city) IN ('mogi das cruzes / sp') THEN 'mogi das cruzes'
      WHEN LOWER(seller_city) IN ('maua/sao paulo') THEN 'maua'
      WHEN LOWER(seller_city) IN ('sao sebastiao da grama/sp') THEN 'sao sebastiao da grama'
      WHEN LOWER(seller_city) IN ('auriflama/sp') THEN 'auriflama'
      WHEN LOWER(seller_city) IN ('cariacica / es') THEN 'cariacica'
      WHEN LOWER(seller_city) IN ('barbacena/ minas gerais') THEN 'barbacena'
      WHEN LOWER(seller_city) IN ('pinhais/pr') THEN 'pinhais'
      WHEN LOWER(seller_city) IN ('andira-pr') THEN 'andira'
      WHEN LOWER(seller_city) IN ('lages - sc') THEN 'lages'
      ELSE LOWER(TRIM(seller_city))
    END AS seller_city,
    seller_state
FROM {{ source('asyvak', 'fp_sellers') }}
