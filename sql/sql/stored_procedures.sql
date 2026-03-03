USE NexFlow;
GO

CREATE OR ALTER PROCEDURE sp_RFM_Segmentation
AS
BEGIN

    -- Step 1: Calculate R, F, M for each customer
    WITH rfm_base AS (
        SELECT
            customer_id,
            -- Recency: days since last order (lower = more recent = better)
            DATEDIFF(DAY, MAX(order_date), GETDATE()) AS recency_days,
            -- Frequency: how many orders
            COUNT(order_line_id) AS frequency,
            -- Monetary: total spent
            SUM(total_amount) AS monetary
        FROM fact_orders
        WHERE order_status != 'Cancelled'
        GROUP BY customer_id
    ),

    -- Step 2: Score each metric 1-5
    -- NTILE(5) splits customers into 5 equal groups
    rfm_scores AS (
        SELECT
            customer_id,
            recency_days,
            frequency,
            monetary,
            -- Recency: less days = better = higher score
            NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
            -- Frequency: more orders = better = higher score
            NTILE(5) OVER (ORDER BY frequency ASC)     AS f_score,
            -- Monetary: more money = better = higher score
            NTILE(5) OVER (ORDER BY monetary ASC)      AS m_score
        FROM rfm_base
    )

    -- Step 3: Label each customer
    SELECT
        customer_id,
        recency_days,
        frequency,
        ROUND(monetary, 2)  AS monetary,
        r_score,
        f_score,
        m_score,
        (r_score + f_score + m_score) AS total_score,
        -- Segment based on total score
        CASE
            WHEN (r_score + f_score + m_score) >= 13 THEN 'VIP'
            WHEN (r_score + f_score + m_score) >= 10 THEN 'Loyal'
            WHEN (r_score + f_score + m_score) >= 7  THEN 'Potential'
            WHEN (r_score + f_score + m_score) >= 4  THEN 'At Risk'
            ELSE                                          'Lost'
        END AS segment
    FROM rfm_scores
    ORDER BY total_score DESC;

END
GO


CREATE OR ALTER PROCEDURE sp_Inventory_Rebalancing
AS
BEGIN

    -- Step 1: Find overstocked and understocked warehouses
    WITH inventory_status AS (
        SELECT
            i.product_id,
            i.warehouse_id,
            i.quantity_on_hand,
            i.reorder_point,
            p.cost_price,
            -- Overstocked = has more than 2x reorder point
            CASE WHEN i.quantity_on_hand > (i.reorder_point * 2)
                 THEN 'Overstocked'
            -- Understocked = below reorder point
                 WHEN i.quantity_on_hand < i.reorder_point
                 THEN 'Understocked'
                 ELSE 'Normal'
            END AS stock_status
        FROM fact_inventory i
        JOIN dim_products p ON i.product_id = p.product_id
        WHERE i.quantity_on_hand IS NOT NULL
    ),

    -- Step 2: Find overstocked warehouses
    overstocked AS (
        SELECT
            product_id,
            warehouse_id AS source_warehouse_id,
            quantity_on_hand,
            reorder_point,
            cost_price,
            -- How much extra stock they have
            (quantity_on_hand - reorder_point) AS excess_qty
        FROM inventory_status
        WHERE stock_status = 'Overstocked'
    ),

    -- Step 3: Find understocked warehouses
    understocked AS (
        SELECT
            product_id,
            warehouse_id AS dest_warehouse_id,
            quantity_on_hand,
            reorder_point,
            -- How much stock they need
            (reorder_point - quantity_on_hand) AS needed_qty
        FROM inventory_status
        WHERE stock_status = 'Understocked'
    )

    -- Step 4: Match overstocked → understocked
    -- Recommend transferring stock between warehouses
    SELECT TOP 100
        o.product_id,
        o.source_warehouse_id,
        u.dest_warehouse_id,
        -- Transfer the smaller of: excess or needed
        CASE WHEN o.excess_qty < u.needed_qty
             THEN o.excess_qty
             ELSE u.needed_qty
        END AS recommended_qty,
        -- Estimated savings = qty transferred × cost
        ROUND(
            CASE WHEN o.excess_qty < u.needed_qty
                 THEN o.excess_qty
                 ELSE u.needed_qty
            END * o.cost_price, 2
        ) AS estimated_savings,
        CASE WHEN u.needed_qty > 100 THEN 'High'
             WHEN u.needed_qty > 50  THEN 'Medium'
             ELSE                        'Low'
        END AS priority
    FROM overstocked o
    JOIN understocked u ON o.product_id = u.product_id
    WHERE o.source_warehouse_id != u.dest_warehouse_id
    ORDER BY estimated_savings DESC;

END
GO


CREATE OR ALTER PROCEDURE sp_Warehouse_Scoring
AS
BEGIN

    SELECT
        w.warehouse_id,
        w.warehouse_name,
        w.region,
        -- Total orders handled
        COUNT(o.order_line_id)                    AS total_orders,
        -- Total revenue
        ROUND(SUM(o.total_amount), 2)             AS total_revenue,
        -- Average order value
        ROUND(AVG(o.total_amount), 2)             AS avg_order_value,
        -- On time delivery rate
        -- On time = delivered within 7 days of order
        ROUND(
            100.0 * SUM(
                CASE WHEN DATEDIFF(DAY, o.order_date, o.delivery_date) <= 7
                     THEN 1 ELSE 0
                END
            ) / COUNT(o.order_line_id), 2
        )                                          AS on_time_pct,
        -- Cancellation rate
        ROUND(
            100.0 * SUM(
                CASE WHEN o.order_status = 'Cancelled'
                     THEN 1 ELSE 0
                END
            ) / COUNT(o.order_line_id), 2
        )                                          AS cancellation_pct,
        -- Performance score out of 100
        ROUND(
            (COUNT(o.order_line_id) * 0.3) / 100 +
            (SUM(o.total_amount) * 0.0001) +
            (AVG(o.total_amount) * 0.01)
        , 2)                                       AS performance_score
    FROM dim_warehouses w
    LEFT JOIN fact_orders o ON w.warehouse_id = o.warehouse_id
    GROUP BY w.warehouse_id, w.warehouse_name, w.region
    ORDER BY performance_score DESC;

END
GO