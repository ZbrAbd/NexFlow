# export_for_tableau.py
# Export all data from SQL Server to CSV files
# Tableau Public will connect to these CSV files

import pandas as pd
import pyodbc
import os
import sys

sys.path.append(os.path.abspath("."))
from config.database import CONNECTION_STRING

def get_connection():
    return pyodbc.connect(CONNECTION_STRING)

os.makedirs("data/tableau", exist_ok=True)

conn = get_connection()
print("Exporting data for Tableau...\n")

# ── 1. Executive Overview ─────────────────────────────
print("1. Executive overview...")
df = pd.read_sql("""
    SELECT 
        o.order_date,
        o.order_status,
        o.total_amount,
        o.quantity,
        o.shipping_cost,
        p.category,
        p.sub_category,
        p.product_name,
        w.warehouse_name,
        w.region,
        c.segment AS customer_segment
    FROM fact_orders o
    JOIN dim_products p  ON o.product_id  = p.product_id
    JOIN dim_warehouses w ON o.warehouse_id = w.warehouse_id
    JOIN dim_customers c  ON o.customer_id  = c.customer_id
""", conn)
df.to_csv("data/tableau/executive_overview.csv", index=False)
print(f"   ✓ {len(df)} rows")

# ── 2. Customer Analytics ─────────────────────────────
print("2. Customer analytics...")

# Pull each table separately then merge in pandas
customers  = pd.read_sql("SELECT * FROM dim_customers", conn)
churn      = pd.read_sql("SELECT * FROM ml_churn_scores", conn)

# Merge in Python instead of SQL
df = customers.merge(churn, on="customer_id", how="left")
df.to_csv("data/tableau/customer_analytics.csv", index=False)
print(f"   ✓ {len(df)} rows")


# ── 3. Demand Forecast ────────────────────────────────
print("3. Demand forecast...")
df = pd.read_sql("""
    SELECT 
        f.forecast_date,
        f.predicted_demand,
        f.model_name,
        p.product_name,
        p.category,
        w.warehouse_name,
        w.region
    FROM ml_demand_forecasts f
    JOIN dim_products   p ON f.product_id   = p.product_id
    JOIN dim_warehouses w ON f.warehouse_id = w.warehouse_id
""", conn)
df.to_csv("data/tableau/demand_forecast.csv", index=False)
print(f"   ✓ {len(df)} rows")

# ── 4. Inventory Rebalancing ──────────────────────────
print("4. Inventory rebalancing...")
df = pd.read_sql("""
    SELECT 
        i.product_id,
        p.product_name,
        p.category,
        w.warehouse_name,
        w.region,
        i.quantity_on_hand,
        i.reorder_point,
        i.quantity_reserved,
        -- Stock status
        CASE 
            WHEN i.quantity_on_hand > i.reorder_point * 2 
                THEN 'Overstocked'
            WHEN i.quantity_on_hand < i.reorder_point     
                THEN 'Understocked'
            ELSE 'Normal'
        END AS stock_status
    FROM fact_inventory i
    JOIN dim_products   p ON i.product_id   = p.product_id
    JOIN dim_warehouses w ON i.warehouse_id = w.warehouse_id
    WHERE i.quantity_on_hand IS NOT NULL
""", conn)
df.to_csv("data/tableau/inventory_status.csv", index=False)
print(f"   ✓ {len(df)} rows")

# ── 5. Anomaly Detection ──────────────────────────────
print("5. Anomaly detection...")
df = pd.read_csv("data/quarantine/anomaly_orders.csv")
df.to_csv("data/tableau/anomaly_orders.csv", index=False)
print(f"   ✓ {len(df)} rows")

conn.close()
print("\n✓ All files saved to data/tableau/")
print("\nNow open Tableau Public and connect to these CSV files!")
