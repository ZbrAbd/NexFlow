# snowflake_load.py
# Load data from CSV files directly into Snowflake
# This simulates S3 → Snowflake pipeline

import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def get_snowflake_connection():
    return snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        database  = os.getenv("SNOWFLAKE_DATABASE"),
        schema    = os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    )

def load_dataframe(df, table_name, conn):
    """Load pandas DataFrame into Snowflake table"""
    cursor = conn.cursor()
    
    # Clear old data
    cursor.execute(f"DELETE FROM {table_name}")
    
    # Build INSERT statement
    cols         = ", ".join(df.columns)
    placeholders = ", ".join(["%s" for _ in df.columns])
    sql          = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    
    # Convert to list of tuples
    rows = [tuple(None if pd.isna(v) else v for v in row)
            for row in df.itertuples(index=False)]
    
    cursor.executemany(sql, rows)
    print(f"  ✓ Loaded {len(rows)} rows into {table_name}")

def run():
    print("="*50)
    print("  LOADING DATA INTO SNOWFLAKE")
    print("="*50)
    
    print("\nConnecting to Snowflake...")
    conn = get_snowflake_connection()
    print("  ✓ Connected!")
    
    # ── Load dimension tables first ───────────────────
    print("\nLoading dimension tables...")
    
    df = pd.read_csv("data/raw/dim_customers.csv")
    load_dataframe(df, "dim_customers", conn)
    
    df = pd.read_csv("data/raw/dim_products.csv")
    load_dataframe(df, "dim_products", conn)
    
    df = pd.read_csv("data/raw/dim_warehouses.csv")
    load_dataframe(df, "dim_warehouses", conn)
    
    # ── Load fact tables ──────────────────────────────
    print("\nLoading fact tables...")
    
    df = pd.read_csv("data/raw/fact_orders.csv")
    df = df.drop_duplicates(subset=["order_line_id"], keep="first")
    df = df[df["customer_id"].notna()]
    df = df[df["quantity"] > 0]
    df = df[df["total_amount"] > 0]
    df["customer_id"] = df["customer_id"].astype(int)
    load_dataframe(df, "fact_orders", conn)
    
    df = pd.read_csv("data/raw/fact_inventory.csv")
    df["quantity_on_hand"] = df["quantity_on_hand"].fillna(0)
    load_dataframe(df, "fact_inventory", conn)
    
    # ── Load ML outputs ───────────────────────────────
    print("\nLoading ML outputs...")
    
    df = pd.read_csv("data/tableau/demand_forecast.csv")
    # Keep only columns that match table
    df = pd.read_sql_query = df[["forecast_date", "predicted_demand", "model_name"]]
    
    df = pd.read_csv("data/tableau/customer_analytics.csv")
    churn_df = df[["customer_id", "churn_probability", 
                   "risk_segment", "scored_date"]].dropna()
    load_dataframe(churn_df, "ml_churn_scores", conn)
    
    conn.close()
    
    print("\n" + "="*50)
    print("  SNOWFLAKE LOADING COMPLETE!")
    print("="*50)

if __name__ == "__main__":
    run()