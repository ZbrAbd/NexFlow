# run_pipeline.py
# This is the main file that runs the entire ETL pipeline
# Extract → Transform → Load

import os
import sys
import pandas as pd

# import our ETL functions
from etl.extract   import (extract_orders, extract_customers, 
                            extract_products, extract_inventory)
from etl.transform import (transform_orders, transform_customers,
                            transform_products, transform_inventory)
from etl.load      import run as load_all

# Make quarantine folder if it doesn't exist
os.makedirs("data/quarantine", exist_ok=True)

print("=" * 50)
print("  NEXFLOW ETL PIPELINE STARTING")
print("=" * 50)

# ── EXTRACT ───────────────────────────────────────────
print("\n[ STEP 1: EXTRACT ]")
raw_orders    = extract_orders()
raw_customers = extract_customers()
raw_products  = extract_products()
raw_inventory = extract_inventory()

# ── TRANSFORM ─────────────────────────────────────────
print("\n[ STEP 2: TRANSFORM ]")
clean_orders    = transform_orders(raw_orders)
clean_customers = transform_customers(raw_customers)
clean_products  = transform_products(raw_products)
clean_inventory = transform_inventory(raw_inventory)

# ── LOAD ──────────────────────────────────────────────
print("\n[ STEP 3: LOAD ]")
load_all(
    clean_customers,
    clean_products,
    pd.read_csv("data/raw/dim_warehouses.csv"),
    clean_orders,
    clean_inventory
)
print("\n" + "=" * 50)
print("  PIPELINE COMPLETE!")
print("=" * 50)