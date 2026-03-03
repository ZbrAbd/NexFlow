# etl/transform.py
# Transform = apply business rules to clean the data
# Each function fixes one specific problem

import pandas as pd

def transform_orders(df):
    """Clean and validate orders data"""
    
    original_count = len(df)
    print(f"\nTransforming orders...")
    print(f"  Starting rows: {original_count}")

    # ── Rule 1: Remove duplicates ─────────────────────────
    # keep='first' means: if same order_line_id appears twice,
    # keep the first one, delete the rest
    df = df.drop_duplicates(subset=["order_line_id"], keep="first")
    print(f"  After removing duplicates: {len(df)} rows")

    # ── Rule 2: Quarantine missing customer_id ────────────
    # We save bad rows separately instead of deleting them
    # So we can investigate later
    bad_rows = df[df["customer_id"].isna()]
    if len(bad_rows) > 0:
        bad_rows.to_csv("data/quarantine/orders_no_customer.csv", 
                       index=False)
        print(f"  Quarantined {len(bad_rows)} rows with missing customer_id")
    
    # Now remove them from main data
    df = df[df["customer_id"].notna()]

    # ── Rule 3: Remove negative quantities ───────────────
    negative_qty = len(df[df["quantity"] <= 0])
    df = df[df["quantity"] > 0]
    print(f"  Removed {negative_qty} rows with negative quantity")

    # ── Rule 4: Remove negative totals ───────────────────
    negative_total = len(df[df["total_amount"] <= 0])
    df = df[df["total_amount"] > 0]
    print(f"  Removed {negative_total} rows with negative total")

    # ── Rule 5: Fix date formats ──────────────────────────
    # Convert text dates to proper date format
    df["order_date"]    = pd.to_datetime(df["order_date"])
    df["delivery_date"] = pd.to_datetime(df["delivery_date"])
    # ship_date has nulls so use errors='coerce' 
    # coerce = if it can't convert, just put NaT (empty date)
    df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce")

    # ── Rule 6: Fix data types ────────────────────────────
    df["customer_id"]  = df["customer_id"].astype(int)
    df["quantity"]     = df["quantity"].astype(int)
    df["unit_price"]   = df["unit_price"].round(2)
    df["total_amount"] = df["total_amount"].round(2)

    print(f"  Final clean rows: {len(df)}")
    print(f"  Removed total: {original_count - len(df)} bad rows")
    return df

def transform_customers(df):
    """Clean customers data"""
    print(f"\nTransforming customers...")
    
    # Fix null emails — replace with unknown
    null_emails = df["email"].isna().sum()
    df["email"] = df["email"].fillna("unknown@nexflow.com")
    print(f"  Fixed {null_emails} null emails")

    # Fix date format
    df["registration_date"] = pd.to_datetime(df["registration_date"])
    
    print(f"  Clean rows: {len(df)}")
    return df

def transform_products(df):
    """Clean products data"""
    print(f"\nTransforming products...")

    # Fix null cost_price — replace with 0
    null_costs = df["cost_price"].isna().sum()
    df["cost_price"] = df["cost_price"].fillna(0)
    print(f"  Fixed {null_costs} null cost prices")

    print(f"  Clean rows: {len(df)}")
    return df

def transform_inventory(df):
    """Clean inventory data"""
    print(f"\nTransforming inventory...")

    # Fix null quantity_on_hand — replace with 0
    null_qty = df["quantity_on_hand"].isna().sum()
    df["quantity_on_hand"] = df["quantity_on_hand"].fillna(0)
    print(f"  Fixed {null_qty} null quantities")

    print(f"  Clean rows: {len(df)}")
    return df