-- models/marts/customer_summary.sql
-- Final business table: one row per customer with all metrics
-- This is what Tableau will use

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

customer_metrics AS (
    SELECT
        customer_id,
        COUNT(order_line_id)                    AS total_orders,
        SUM(total_amount)                       AS total_revenue,
        AVG(total_amount)                       AS avg_order_value,
        MIN(order_date)                         AS first_order_date,
        MAX(order_date)                         AS last_order_date,
        DATEDIFF('day', MAX(order_date), CURRENT_DATE) AS recency_days,
        SUM(CASE WHEN order_status = 'Returned' 
            THEN 1 ELSE 0 END)                  AS total_returns,
        AVG(delivery_days)                      AS avg_delivery_days
    FROM orders
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.customer_name,
    c.segment,
    c.city,
    c.state,
    c.customer_age_days,
    m.total_orders,
    ROUND(m.total_revenue, 2)    AS total_revenue,
    ROUND(m.avg_order_value, 2)  AS avg_order_value,
    m.first_order_date,
    m.last_order_date,
    m.recency_days,
    m.total_returns,
    ROUND(m.avg_delivery_days, 1) AS avg_delivery_days,
    -- Customer value segment
    CASE
        WHEN m.total_revenue > 50000  THEN 'High Value'
        WHEN m.total_revenue > 10000  THEN 'Mid Value'
        ELSE                               'Low Value'
    END AS value_segment
FROM customers c
LEFT JOIN customer_metrics m ON c.customer_id = m.customer_id