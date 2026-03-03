# etl/extract.py
# Extract = just READ the data from wherever it lives
# Right now we read from CSV files
# Later this could read from Kafka stream instead

import pandas as pd

def extract_orders():
    """Read raw orders CSV and return as DataFrame"""
    df = pd.read_csv("data/raw/fact_orders.csv")
    print(f"Extracted {len(df)} raw order rows")
    return df

def extract_customers():
    """Read raw customers CSV"""
    df = pd.read_csv("data/raw/dim_customers.csv")
    print(f"Extracted {len(df)} customer rows")
    return df

def extract_products():
    """Read raw products CSV"""
    df = pd.read_csv("data/raw/dim_products.csv")
    print(f"Extracted {len(df)} product rows")
    return df

def extract_inventory():
    """Read raw inventory CSV"""
    df = pd.read_csv("data/raw/fact_inventory.csv")
    print(f"Extracted {len(df)} inventory rows")
    return df