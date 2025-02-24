WITH order_totals AS (
    SELECT 
        o.customer_id,
        SUM(o.total_amount) AS total_spent
    FROM {{ ref('stg_orders') }} o
    GROUP BY o.customer_id
)
SELECT * FROM order_totals