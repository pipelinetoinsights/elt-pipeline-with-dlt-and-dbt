WITH customer_spend AS (
    SELECT 
        c.customer_id,
        c.name,
        i.total_spent
    FROM {{ ref('stg_customers') }} c
    JOIN {{ ref('int_revenue_per_customer') }} i 
    ON c.customer_id = i.customer_id
)
SELECT 
    customer_id,
    name,
    total_spent,
    CASE 
        WHEN total_spent > 500 THEN 'High Value'
        WHEN total_spent BETWEEN 200 AND 500 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment
FROM customer_spend
