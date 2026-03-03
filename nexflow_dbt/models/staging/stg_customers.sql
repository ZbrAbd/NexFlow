-- models/staging/stg_customers.sql

SELECT
    customer_id,
    customer_name,
    COALESCE(email, 'unknown@nexflow.com') AS email,
    segment,
    city,
    state,
    country,
    registration_date,
    -- How long have they been a customer?
    DATEDIFF('day', registration_date, CURRENT_DATE) AS customer_age_days
FROM {{ source('warehouse', 'dim_customers') }}