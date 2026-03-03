# etl/load.py
import pandas as pd
import pyodbc
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.database import CONNECTION_STRING

def get_connection():
    return pyodbc.connect(CONNECTION_STRING)

def clear_table(table_name, conn):
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM {table_name}")
    conn.commit()

def load_table(df, table_name, conn):
    cursor = conn.cursor()
    cols         = ", ".join(df.columns)
    placeholders = ", ".join(["?" for _ in df.columns])
    sql          = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    rows         = [tuple(None if pd.isna(v) else v for v in row) 
                   for row in df.itertuples(index=False)]
    cursor.executemany(sql, rows)
    conn.commit()
    print(f"  ✓ Loaded {len(rows)} rows into {table_name}")

def run(customers, products, warehouses, orders, inventory):
    print("\nConnecting to SQL Server...")
    conn = get_connection()

    print("Clearing old data...")
    clear_table("fact_orders",    conn)
    clear_table("fact_inventory", conn)
    clear_table("dim_customers",  conn)
    clear_table("dim_products",   conn)
    clear_table("dim_warehouses", conn)

    print("\nLoading dimension tables...")
    load_table(customers,  "dim_customers",  conn)
    load_table(products,   "dim_products",   conn)
    load_table(warehouses, "dim_warehouses", conn)

    print("\nLoading fact tables...")
    load_table(orders,    "fact_orders",    conn)
    load_table(inventory, "fact_inventory", conn)

    conn.close()
    print("\n✓ All clean data loaded into SQL Server!")
