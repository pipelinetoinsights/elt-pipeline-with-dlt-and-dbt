WITH source AS (
    SELECT * 
    FROM {{ source('case_study', 'orders') }}
)
SELECT 
    order_id,
    customer_id,
    order_date::timestamp AS order_date,
    total_amount::numeric AS total_amount
FROM source