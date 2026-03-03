# ml/feature_engineering.py
# Feature Engineering = creating the right input columns for ML
# Garbage in = Garbage out
# Good features = accurate predictions

import pandas as pd
import pyodbc
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.database import CONNECTION_STRING

def get_connection():
    return pyodbc.connect(CONNECTION_STRING)

def build_demand_features():
    """
    Pull orders from SQL Server and create features for ML
    Each row = one product + one warehouse + one month
    Target = how many units sold that month
    """
    print("Building demand features...")
    
    conn = get_connection()
    
    # Pull monthly sales per product per warehouse
    query = """
        SELECT 
            product_id,
            warehouse_id,
            -- Extract date parts for ML features
            YEAR(order_date)                    AS year,
            MONTH(order_date)                   AS month,
            -- Total quantity sold that month
            SUM(quantity)                       AS total_quantity,
            -- Total revenue that month
            SUM(total_amount)                   AS total_revenue,
            -- Number of orders that month
            COUNT(order_line_id)                AS order_count,
            -- Average order size
            AVG(CAST(quantity AS FLOAT))        AS avg_quantity
        FROM fact_orders
        WHERE order_status != 'Cancelled'
        GROUP BY product_id, warehouse_id, 
                 YEAR(order_date), MONTH(order_date)
        ORDER BY product_id, warehouse_id, year, month
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"  Raw data: {len(df)} rows")
    
    # ── Create lag features ───────────────────────────────
    # Lag = last month's value
    # "If last month sold 300, this month probably similar"
    
    df = df.sort_values(["product_id", "warehouse_id", "year", "month"])
    
    # Group by product + warehouse so lags don't cross products
    grp = df.groupby(["product_id", "warehouse_id"])
    
    # Last month's quantity
    df["lag_1_qty"] = grp["total_quantity"].shift(1)
    # 2 months ago
    df["lag_2_qty"] = grp["total_quantity"].shift(2)
    # 3 months ago
    df["lag_3_qty"] = grp["total_quantity"].shift(3)
    # Rolling average last 3 months
    df["rolling_3m_avg"] = grp["total_quantity"].transform(
        lambda x: x.shift(1).rolling(3).mean()
    )
    
    # ── Create season feature ─────────────────────────────
    # Seasons affect demand
    df["season"] = df["month"].map({
        12: "Winter", 1: "Winter",  2: "Winter",
        3:  "Spring", 4: "Spring",  5: "Spring",
        6:  "Summer", 7: "Summer",  8: "Summer",
        9:  "Fall",   10: "Fall",   11: "Fall"
    })
    
    # Convert season to number for ML
    # ML needs numbers, not text
    season_map = {"Winter": 4, "Spring": 1, 
                  "Summer": 2, "Fall": 3}
    df["season_num"] = df["season"].map(season_map)
    
    # ── Drop rows with nulls (lag creates nulls for first rows) ──
    df = df.dropna()
    
    print(f"  After feature engineering: {len(df)} rows")
    print(f"  Features created: {list(df.columns)}")
    
    return df
