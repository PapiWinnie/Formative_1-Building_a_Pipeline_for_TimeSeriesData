"""
train_model.py - Train and save the Random Forest model.

Mirrors the preprocessing pipeline from the notebook (Task 1) exactly,
then saves the trained model and feature column list so predict.py can load them.

Usage:
    python train_model.py
"""

import joblib
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error


# ──────────────────────────────────────────────────────────────────────────────
# Preprocessing (identical to the notebook pipeline)
# ──────────────────────────────────────────────────────────────────────────────

def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply the same preprocessing pipeline used in Task 1:
      1. Fill missing holiday values with "None"
      2. Parse date_time
      3. Sort chronologically
      4. Create is_holiday flag
      5. Compute 24-hour and 168-hour (7-day) moving averages
      6. Create lag features: 1h, 24h, 168h (7-day), 720h (30-day)
      7. One-hot encode holiday, weather_main, weather_description
      8. Drop helper columns and rows with NaN
    """
    df = df.copy()

    # 1. Missing holiday → "None"
    df["holiday"] = df["holiday"].fillna("None")

    # 2. Parse date_time
    df["date_time"] = pd.to_datetime(df["date_time"], format="%d-%m-%Y %H:%M")

    # 3. Sort chronologically
    df = df.sort_values("date_time").reset_index(drop=True)

    # 4. Holiday flag
    df["is_holiday"] = (df["holiday"] != "None").astype(int)

    # 5. Moving averages
    df["ma_24"]  = df["traffic_volume"].rolling(window=24).mean()
    df["ma_168"] = df["traffic_volume"].rolling(window=168).mean()

    # 6. Lag features
    df["lag_1"]   = df["traffic_volume"].shift(1)
    df["lag_24"]  = df["traffic_volume"].shift(24)
    df["lag_168"] = df["traffic_volume"].shift(168)
    df["lag_720"] = df["traffic_volume"].shift(720)

    # 7. One-hot encode categorical columns
    df = pd.get_dummies(
        df,
        columns=["holiday", "weather_main", "weather_description"],
        drop_first=True,
    )

    # 8. Drop helper columns and NaN rows
    df = df.drop(columns=["time_diff"], errors="ignore")
    df = df.dropna()

    return df


# ──────────────────────────────────────────────────────────────────────────────
# Training
# ──────────────────────────────────────────────────────────────────────────────

CSV_PATH   = Path("dataset/Metro_Interstate_Traffic_Volume.csv")
OUTPUT_DIR = Path("models")


def train() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[1/4] Loading dataset from: {CSV_PATH}")
    raw = pd.read_csv(CSV_PATH)
    print(f"      Raw shape: {raw.shape}")

    print("[2/4] Preprocessing…")
    df = preprocess(raw)
    print(f"      Processed shape: {df.shape}")

    # Chronological train/test split (80/20)
    split = int(len(df) * 0.8)
    train_df, test_df = df[:split], df[split:]

    feature_cols = [c for c in df.columns if c not in ("traffic_volume", "date_time")]

    X_train, y_train = train_df[feature_cols], train_df["traffic_volume"]
    X_test,  y_test  = test_df[feature_cols],  test_df["traffic_volume"]

    print(f"[3/4] Training RandomForest on {len(X_train)} samples, {len(feature_cols)} features…")
    # Best hyperparameters from GridSearchCV in the notebook
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    mae  = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    print(f"      Test MAE : {mae:.2f}")
    print(f"      Test RMSE: {rmse:.2f}")

    print("[4/4] Saving artifacts…")
    model_path   = OUTPUT_DIR / "traffic_model.pkl"
    columns_path = OUTPUT_DIR / "feature_columns.pkl"

    joblib.dump(model,        model_path)
    joblib.dump(feature_cols, columns_path)

    print(f"      Model saved   → {model_path}")
    print(f"      Columns saved → {columns_path}")
    print("Done.")


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    train()
