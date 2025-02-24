WITH source AS (
    SELECT * 
    FROM {{ source('case_study', 'products') }}
)
SELECT 
    product_id,
    name AS product_name,
    category,
    price::numeric AS price
FROM source