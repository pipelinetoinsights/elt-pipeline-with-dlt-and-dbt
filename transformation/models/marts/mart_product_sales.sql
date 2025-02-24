WITH product_sales AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        i.total_product_revenue
    FROM {{ ref('stg_products') }} p
    JOIN {{ ref('int_order_item_revenue') }} i 
    ON p.product_id = i.product_id
)
SELECT 
    product_id,
    product_name,
    category,
    total_product_revenue
FROM product_sales
ORDER BY total_product_revenue DESC
