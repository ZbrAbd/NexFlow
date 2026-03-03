# medallion_architecture.py
# Implements Bronze/Silver/Gold data lake layers in AWS S3
# This is the modern standard for data lake organization

import pandas as pd
import boto3
import os
import sys
from io import StringIO
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.path.abspath("."))
from config.aws_config import (AWS_ACCESS_KEY, AWS_SECRET_KEY,
                                AWS_REGION, S3_BUCKET)

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id     = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY,
        region_name           = AWS_REGION
    )

def upload_df_to_s3(df, s3_key):
    """Upload DataFrame directly to S3 as CSV"""
    s3 = get_s3_client()
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(
        Bucket = S3_BUCKET,
        Key    = s3_key,
        Body   = csv_buffer.getvalue()
    )
    print(f"  ✓ {s3_key} ({len(df)} rows)")

def bronze_layer():
    """
    Bronze = Raw data exactly as generated
    No cleaning, no transformation
    Just store everything as-is
    """
    print("\n🥉 BRONZE LAYER — Raw Data")
    print("   Storing raw unprocessed data...")

    files = [
        ("data/raw/dim_customers.csv",  "bronze/dim_customers.csv"),
        ("data/raw/dim_products.csv",   "bronze/dim_products.csv"),
        ("data/raw/dim_warehouses.csv", "bronze/dim_warehouses.csv"),
        ("data/raw/fact_orders.csv",    "bronze/fact_orders.csv"),
        ("data/raw/fact_inventory.csv", "bronze/fact_inventory.csv"),
    ]

    for local_path, s3_key in files:
        df = pd.read_csv(local_path)
        upload_df_to_s3(df, s3_key)

    print(f"  Bronze layer complete!")

def silver_layer():
    """
    Silver = Cleaned and validated data
    Fix nulls, remove duplicates, validate types
    """
    print("\n🥈 SILVER LAYER — Cleaned Data")
    print("   Applying cleaning rules...")

    # Clean orders
    df = pd.read_csv("data/raw/fact_orders.csv")
    before = len(df)
    df = df.drop_duplicates(subset=["order_line_id"], keep="first")
    df = df[df["customer_id"].notna()]
    df = df[df["quantity"] > 0]
    df = df[df["total_amount"] > 0]
    df["customer_id"] = df["customer_id"].astype(int)
    upload_df_to_s3(df, "silver/fact_orders.csv")
    print(f"  Orders: {before} → {len(df)} rows (removed {before-len(df)} bad rows)")

    # Clean customers
    df = pd.read_csv("data/raw/dim_customers.csv")
    df["email"] = df["email"].fillna("unknown@nexflow.com")
    upload_df_to_s3(df, "silver/dim_customers.csv")

    # Clean products
    df = pd.read_csv("data/raw/dim_products.csv")
    df["cost_price"] = df["cost_price"].fillna(0)
    upload_df_to_s3(df, "silver/dim_products.csv")

    # Clean inventory
    df = pd.read_csv("data/raw/fact_inventory.csv")
    df["quantity_on_hand"] = df["quantity_on_hand"].fillna(0)
    upload_df_to_s3(df, "silver/fact_inventory.csv")

    # Warehouses (already clean)
    df = pd.read_csv("data/raw/dim_warehouses.csv")
    upload_df_to_s3(df, "silver/dim_warehouses.csv")

    print(f"  Silver layer complete!")

def gold_layer():
    """
    Gold = Business ready aggregated data
    Ready for Tableau, ML, and reporting
    """
    print("\n🥇 GOLD LAYER — Business Ready Data")
    print("   Creating aggregated business metrics...")

    # Gold 1: Monthly sales summary
    df = pd.read_csv("data/raw/fact_orders.csv")
    df = df[df["quantity"] > 0]
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["year_month"] = df["order_date"].dt.to_period("M").astype(str)

    monthly = df.groupby("year_month").agg(
        total_orders   = ("order_line_id", "count"),
        total_revenue  = ("total_amount",  "sum"),
        total_quantity = ("quantity",       "sum"),
        avg_order_value= ("total_amount",  "mean")
    ).reset_index()
    monthly["total_revenue"]   = monthly["total_revenue"].round(2)
    monthly["avg_order_value"] = monthly["avg_order_value"].round(2)
    upload_df_to_s3(monthly, "gold/monthly_sales_summary.csv")

    # Gold 2: Product performance
    product_perf = df.groupby("product_id").agg(
        total_orders   = ("order_line_id", "count"),
        total_revenue  = ("total_amount",  "sum"),
        total_quantity = ("quantity",       "sum")
    ).reset_index()
    upload_df_to_s3(product_perf, "gold/product_performance.csv")

    # Gold 3: Warehouse performance
    warehouse_perf = df.groupby("warehouse_id").agg(
        total_orders  = ("order_line_id", "count"),
        total_revenue = ("total_amount",  "sum"),
        avg_order_val = ("total_amount",  "mean")
    ).reset_index()
    upload_df_to_s3(warehouse_perf, "gold/warehouse_performance.csv")

    # Gold 4: ML outputs
    if os.path.exists("data/tableau/demand_forecast.csv"):
        df = pd.read_csv("data/tableau/demand_forecast.csv")
        upload_df_to_s3(df, "gold/demand_forecasts.csv")

    if os.path.exists("data/tableau/customer_analytics.csv"):
        df = pd.read_csv("data/tableau/customer_analytics.csv")
        upload_df_to_s3(df, "gold/customer_analytics.csv")

    print(f"  Gold layer complete!")

def run():
    print("="*50)
    print("  MEDALLION ARCHITECTURE — S3 DATA LAKE")
    print("="*50)
    print("""
    Bronze → Silver → Gold
    Raw    → Clean  → Business Ready
    """)

    bronze_layer()
    silver_layer()
    gold_layer()

    print("\n" + "="*50)
    print("  MEDALLION ARCHITECTURE COMPLETE!")
    print("="*50)
    print(f"""
S3 Bucket: {S3_BUCKET}
├── bronze/  🥉 Raw data (5 files)
├── silver/  🥈 Cleaned data (5 files)
└── gold/    🥇 Business metrics (5 files)
    """)

if __name__ == "__main__":
    run()