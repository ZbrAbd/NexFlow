# s3_upload.py
# Upload data files to AWS S3 Data Lake
# S3 = Simple Storage Service = cloud file storage

import boto3
import os
import sys

sys.path.append(os.path.abspath("."))
from config.aws_config import (AWS_ACCESS_KEY, AWS_SECRET_KEY, 
                                AWS_REGION, S3_BUCKET,
                                S3_RAW_PREFIX, S3_CLEAN_PREFIX,
                                S3_ML_PREFIX)

def get_s3_client():
    """Connect to AWS S3"""
    return boto3.client(
        "s3",
        aws_access_key_id     = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY,
        region_name           = AWS_REGION
    )

def upload_file(local_path, s3_key):
    """
    Upload one file to S3
    local_path = where file is on your computer
    s3_key     = where to store it in S3 (like a folder path)
    """
    s3 = get_s3_client()
    s3.upload_file(local_path, S3_BUCKET, s3_key)
    print(f"  ✓ Uploaded: {s3_key}")

def upload_all():
    print("="*50)
    print("  UPLOADING TO AWS S3 DATA LAKE")
    print("="*50)
    
    # ── Upload raw data ───────────────────────────────
    print("\nUploading raw data...")
    raw_files = [
        "data/raw/dim_customers.csv",
        "data/raw/dim_products.csv",
        "data/raw/dim_warehouses.csv",
        "data/raw/fact_orders.csv",
        "data/raw/fact_inventory.csv",
    ]
    
    for file in raw_files:
        filename = os.path.basename(file)  # just the filename
        s3_key   = f"{S3_RAW_PREFIX}{filename}"
        upload_file(file, s3_key)
    
    # ── Upload clean/tableau data ─────────────────────
    print("\nUploading clean data...")
    clean_files = [
        "data/tableau/executive_overview.csv",
        "data/tableau/customer_analytics.csv",
        "data/tableau/demand_forecast.csv",
        "data/tableau/inventory_status.csv",
        "data/tableau/anomaly_orders.csv",
    ]
    
    for file in clean_files:
        filename = os.path.basename(file)
        s3_key   = f"{S3_CLEAN_PREFIX}{filename}"
        upload_file(file, s3_key)
    
    # ── Upload ML outputs ─────────────────────────────
    print("\nUploading ML outputs...")
    ml_files = [
        "data/quarantine/anomaly_orders.csv",
        "data/quarantine/orders_no_customer.csv",
    ]
    
    for file in ml_files:
        if os.path.exists(file):
            filename = os.path.basename(file)
            s3_key   = f"{S3_ML_PREFIX}{filename}"
            upload_file(file, s3_key)
    
    print("\n" + "="*50)
    print("  ALL FILES UPLOADED TO S3!")
    print("="*50)
    print(f"\nView your data lake at:")
    print(f"https://s3.console.aws.amazon.com/s3/buckets/{S3_BUCKET}")

if __name__ == "__main__":
    upload_all()