# ml/anomaly_detection.py
# Find suspicious/unusual orders using Isolation Forest
# Unsupervised = no labels needed, model finds anomalies itself

import pandas as pd
import numpy as np
import pickle
import os
import pyodbc
import sys

from sklearn.ensemble import IsolationForest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.database import CONNECTION_STRING

def get_connection():
    return pyodbc.connect(CONNECTION_STRING)

def detect_anomalies():
    
    print("\n" + "="*50)
    print("  ANOMALY DETECTION")
    print("="*50)
    
    # Step 1: Pull order data
    print("\nPulling order data...")
    
    conn = get_connection()
    
    query = """
        SELECT
            order_line_id,
            customer_id,
            product_id,
            warehouse_id,
            quantity,
            unit_price,
            total_amount,
            shipping_cost,
            discount_pct
        FROM fact_orders
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"  Orders loaded: {len(df)}")
    
    # Step 2: Features for anomaly detection
    # Only numerical columns
    feature_cols = [
        "quantity",
        "unit_price",
        "total_amount",
        "shipping_cost",
        "discount_pct"
    ]
    
    X = df[feature_cols]
    
    # Step 3: Train Isolation Forest
    print("\nTraining Isolation Forest...")
    
    model = IsolationForest(
        contamination=0.05,  # expect 5% anomalies
        random_state=42
    )
    
    # fit_predict returns:
    # 1  = normal
    # -1 = anomaly
    df["anomaly_score"] = model.fit_predict(X)
    df["is_anomaly"]    = (df["anomaly_score"] == -1).astype(int)
    # 1 = anomaly, 0 = normal
    
    print("  Model trained!")
    
    # Step 4: Results
    anomalies = df[df["is_anomaly"] == 1]
    
    print(f"\nResults:")
    print(f"  Total orders:    {len(df)}")
    print(f"  Anomalies found: {len(anomalies)} ({len(anomalies)/len(df)*100:.1f}%)")
    
    print(f"\nSample anomalies:")
    print(anomalies[feature_cols].head(5).to_string())
    
    # Step 5: Save model
    os.makedirs("models", exist_ok=True)
    with open("models/anomaly_model.pkl", "wb") as f:
        pickle.dump(model, f)
    print("\n  Model saved to models/anomaly_model.pkl")
    
    # Step 6: Save anomalies to CSV for investigation
    anomalies.to_csv("data/quarantine/anomaly_orders.csv", index=False)
    print(f"  ✓ Saved {len(anomalies)} anomalies to data/quarantine/anomaly_orders.csv")
    
    print("\n" + "="*50)
    print("  ANOMALY DETECTION COMPLETE!")
    print("="*50)
    
    return model, anomalies

if __name__ == "__main__":
    detect_anomalies()