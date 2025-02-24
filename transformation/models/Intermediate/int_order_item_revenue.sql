WITH item_revenue AS (
    SELECT 
        oi.product_id,
        SUM(oi.quantity * oi.price_per_unit) AS total_product_revenue
    FROM {{ ref('stg_order_items') }} oi
    GROUP BY oi.product_id
)
SELECT * FROM item_revenue