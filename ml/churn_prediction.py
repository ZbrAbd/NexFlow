# ml/churn_prediction.py
# Predict which customers are likely to stop buying
# Uses RFM features + Random Forest

import pandas as pd
import numpy as np
import pickle
import os
import pyodbc
import sys

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.database import CONNECTION_STRING

def get_connection():
    return pyodbc.connect(CONNECTION_STRING)

def build_churn_features():
    """
    Build features for churn prediction
    One row per customer
    Target = 1 (churned) or 0 (active)
    """
    print("Building churn features...")
    
    conn = get_connection()
    
    query = """
        SELECT
            customer_id,
            -- Recency: days since last order
            DATEDIFF(DAY, MAX(order_date), GETDATE()) AS recency_days,
            -- Frequency: total orders
            COUNT(order_line_id)                       AS frequency,
            -- Monetary: total spent
            SUM(total_amount)                          AS monetary,
            -- Average order value
            AVG(total_amount)                          AS avg_order_value,
            -- How many different products bought
            COUNT(DISTINCT product_id)                 AS unique_products,
            -- How many different warehouses used
            COUNT(DISTINCT warehouse_id)               AS unique_warehouses,
            -- Cancellation rate
            SUM(CASE WHEN order_status = 'Cancelled' 
                THEN 1 ELSE 0 END) * 1.0 / 
            COUNT(order_line_id)                       AS cancel_rate
        FROM fact_orders
        GROUP BY customer_id
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    # ── Define churn ──────────────────────────────────────
    # Churn = no purchase in last 90 days
    # This is a business decision — you define what churn means
    df["churned"] = (df["recency_days"] > 90).astype(int)
    # 1 = churned, 0 = active
    
    print(f"  Total customers: {len(df)}")
    print(f"  Churned:  {df['churned'].sum()} ({df['churned'].mean()*100:.1f}%)")
    print(f"  Active:   {(df['churned']==0).sum()} ({(df['churned']==0).mean()*100:.1f}%)")
    
    return df

def train_churn_model():
    
    print("\n" + "="*50)
    print("  CHURN PREDICTION MODEL TRAINING")
    print("="*50)
    
    # Step 1: Get features
    df = build_churn_features()
    
    # Step 2: Define features and target
    feature_cols = [
        "frequency",
        "monetary",
        "avg_order_value",
        "unique_products",
        "unique_warehouses",
        "cancel_rate"
    ]
    
    X = df[feature_cols]
    y = df["churned"]
    
    print(f"\nTotal customers: {len(X)}")
    
    # Step 3: Split 80/20
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42
    )
    
    print(f"Training: {len(X_train)} customers")
    print(f"Testing:  {len(X_test)} customers")
    
    # Step 4: Train Random Forest
    print("\nTraining Random Forest...")
    
    model = RandomForestClassifier(
        n_estimators=100,  # 100 decision trees voting
        max_depth=10,      # how deep each tree goes
        random_state=42
    )
    
    model.fit(X_train, y_train)
    print("  Model trained!")
    
    # Step 5: Evaluate
    print("\nEvaluating model...")
    
    predictions  = model.predict(X_test)
    accuracy     = accuracy_score(y_test, predictions)
    
    print(f"  Accuracy: {accuracy*100:.1f}%")
    
    if accuracy > 0.65:
        print(f"  Result: ✅ PASS - Model is good!")
    else:
        print(f"  Result: ❌ FAIL - Need improvement")
    
    # Step 6: Save model
    os.makedirs("models", exist_ok=True)
    with open("models/churn_model.pkl", "wb") as f:
        pickle.dump(model, f)
    print("\n  Model saved to models/churn_model.pkl")
    
    # Step 7: Score ALL customers and save to SQL Server
    print("\nScoring all customers...")
    
    # Get churn probability for every customer
    # predict_proba returns [prob_not_churn, prob_churn]
    all_features   = df[feature_cols]
    churn_proba    = model.predict_proba(all_features)[:, 1]
    
    df["churn_probability"] = churn_proba
    
    # Label risk segments
    df["risk_segment"] = pd.cut(
        df["churn_probability"],
        bins=[0, 0.3, 0.6, 1.0],
        labels=["Low Risk", "Medium Risk", "High Risk"]
    )
    
    print(f"  High Risk:   {(df['risk_segment']=='High Risk').sum()} customers")
    print(f"  Medium Risk: {(df['risk_segment']=='Medium Risk').sum()} customers")
    print(f"  Low Risk:    {(df['risk_segment']=='Low Risk').sum()} customers")
    
    # Save to SQL Server
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM ml_churn_scores")
    
    from datetime import date
    today = date.today()
    
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO ml_churn_scores
            (customer_id, churn_probability, risk_segment, scored_date)
            VALUES (?, ?, ?, ?)
        """,
        int(row.customer_id),
        round(float(row.churn_probability), 4),
        str(row.risk_segment),
        today
        )
    
    conn.commit()
    conn.close()
    
    print(f"\n  ✓ Saved {len(df)} churn scores to SQL Server")
    print("\n" + "="*50)
    print("  CHURN PREDICTION COMPLETE!")
    print("="*50)
    
    return model, accuracy

if __name__ == "__main__":
    train_churn_model()