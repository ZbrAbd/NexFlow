# ml/demand_forecast.py
import pandas as pd
import numpy as np
import pickle
import os
import pyodbc
import sys

from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.database import CONNECTION_STRING
from ml.feature_engineering import build_demand_features

def get_connection():
    return pyodbc.connect(CONNECTION_STRING)

def train_demand_model():
    
    print("\n" + "="*50)
    print("  DEMAND FORECAST MODEL TRAINING")
    print("="*50)
    
    # Step 1: Get features
    df = build_demand_features()
    
    # Step 2: Define features and target
    feature_cols = [
        "product_id",
        "warehouse_id",
        "month",
        "year",
        "season_num",
        "lag_1_qty",
        "lag_2_qty",
        "lag_3_qty",
        "rolling_3m_avg"
    ]
    
    target_col = "total_quantity"
    
    X = df[feature_cols]
    y = df[target_col]
    
    print(f"\nTotal samples: {len(X)}")
    
    # Step 3: Split 80/20
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42
    )
    
    print(f"Training samples: {len(X_train)} (80%)")
    print(f"Testing samples:  {len(X_test)} (20%)")
    
    # Step 4: Train XGBoost
    print("\nTraining XGBoost model...")
    
    model = XGBRegressor(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        random_state=42
    )
    
    model.fit(X_train, y_train)
    print("  Model trained!")
    
    # Step 5: Evaluate
    print("\nEvaluating model...")
    
    predictions = model.predict(X_test)
    
    mae = mean_absolute_error(y_test, predictions)
    
    # MAPE only on rows where actual > 10
    mask = y_test > 10
    if mask.sum() > 0:
        mape = np.mean(np.abs(
            (y_test[mask] - predictions[mask]) / y_test[mask]
        )) * 100
    else:
        mape = 999
    
    print(f"  MAE:  {mae:.1f} units")
    print(f"  MAPE: {mape:.1f}%")
    
    if mae < 50:
        print(f"  Result: ✅ PASS - Model is good!")
    else:
        print(f"  Result: ❌ FAIL - Need improvement")
    
    # Step 6: Save model
    os.makedirs("models", exist_ok=True)
    with open("models/demand_forecast.pkl", "wb") as f:
        pickle.dump(model, f)
    print("\n  Model saved to models/demand_forecast.pkl")
    
    # Step 7: Save predictions to SQL Server
    print("\nSaving predictions to SQL Server...")
    
    test_df = X_test.copy()
    test_df["predicted_demand"] = predictions.round(0)
    test_df["model_name"]       = "XGBoost"
    test_df["forecast_date"]    = pd.to_datetime(
        test_df[["year", "month"]].assign(day=1)
    )
    
    forecast_df = test_df[[
        "product_id", "warehouse_id",
        "forecast_date", "predicted_demand", "model_name"
    ]]
    
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM ml_demand_forecasts")
    
    for _, row in forecast_df.iterrows():
        cursor.execute("""
            INSERT INTO ml_demand_forecasts
            (product_id, warehouse_id, forecast_date,
             predicted_demand, model_name)
            VALUES (?, ?, ?, ?, ?)
        """,
        int(row.product_id),
        int(row.warehouse_id),
        row.forecast_date.date(),
        float(row.predicted_demand),
        row.model_name
        )
    
    conn.commit()
    conn.close()
    
    print(f"  ✓ Saved {len(forecast_df)} forecasts to SQL Server")
    print("\n" + "="*50)
    print("  DEMAND FORECAST COMPLETE!")
    print("="*50)
    
    return model, mape

if __name__ == "__main__":
    train_demand_model()