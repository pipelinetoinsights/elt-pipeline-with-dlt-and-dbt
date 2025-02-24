WITH source AS (
    SELECT * 
    FROM {{ source('case_study', 'customers') }}
)
SELECT 
    customer_id,
    name,
    email,
    created_at::timestamp AS created_at
FROM source