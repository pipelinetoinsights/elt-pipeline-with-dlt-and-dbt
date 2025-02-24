WITH source AS (
    SELECT * 
    FROM {{ source('case_study', 'order_items') }}
)
SELECT 
    order_item_id,
    order_id,
    product_id,
    quantity::integer AS quantity,
    price_per_unit::numeric AS price_per_unit
FROM source