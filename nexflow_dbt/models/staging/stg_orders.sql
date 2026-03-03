-- models/staging/stg_orders.sql
-- Staging model: clean column names, basic filtering
-- This is the first transformation step

SELECT
    order_line_id,
    customer_id,
    product_id,
    warehouse_id,
    order_date,
    ship_date,
    delivery_date,
    quantity,
    unit_price,
    discount_pct,
    total_amount,
    order_status,
    shipping_cost,
    -- Add derived columns
    DATEDIFF('day', order_date, delivery_date) AS delivery_days,
    CASE 
        WHEN order_status = 'Delivered' THEN 1 
        ELSE 0 
    END AS is_delivered
FROM {{ source('warehouse', 'fact_orders') }}
WHERE quantity > 0
  AND total_amount > 0