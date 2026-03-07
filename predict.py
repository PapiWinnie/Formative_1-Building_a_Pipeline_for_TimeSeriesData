"""
predict.py - Task 4: Fetch → Preprocess → Predict traffic volume.

Workflow
--------
  1. Fetch recent time-series records from the REST API (MongoDB endpoint).
  2. Apply the same preprocessing pipeline used in Task 1 (notebook).
  3. Load the saved Random Forest model.
  4. Predict traffic volume for the latest fetched record and display the result.

Usage
-----
  python predict.py
"""

import sys
from pathlib import Path

import joblib
import pandas as pd
import requests

API_BASE    = "http://localhost:8001"
FETCH_LIMIT = 200
MODEL_PATH  = Path("optimized_random_forest_model.joblib")


# ──────────────────────────────────────────────────────────────────────────────
# Step 1 – Fetch records from the API
# ──────────────────────────────────────────────────────────────────────────────

def fetch_records() -> list:
    """
    Fetch the most recent FETCH_LIMIT traffic records from the MongoDB endpoint.
    The endpoint returns records sorted newest-first; we sort chronologically
    during preprocessing.
    """
    url = f"{API_BASE}/mongo/traffic"
    try:
        resp = requests.get(url, params={"limit": FETCH_LIMIT}, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.ConnectionError:
        print(f"[ERROR] Could not connect to API at {API_BASE}.")
        print("        Make sure the API is running:  uvicorn app:app --reload --port 8001")
        sys.exit(1)
    except requests.exceptions.HTTPError as exc:
        print(f"[ERROR] API returned an error: {exc}")
        sys.exit(1)

    records = resp.json()
    print(f"[Step 1] Fetched {len(records)} records from {url}")
    return records


# ──────────────────────────────────────────────────────────────────────────────
# Step 2 – Preprocess
# ──────────────────────────────────────────────────────────────────────────────

def _flatten_mongo_record(rec: dict) -> dict:
    """
    MongoDB documents store weather and time_features as nested dicts,
    and use different field names (temp_kelvin, rain_1h_mm, clouds_pct, …).
    Flatten and rename to match the original CSV column names used during training.
    """
    weather = rec.get("weather") or {}
    return {
        "date_time":           rec.get("date_time"),
        "traffic_volume":      rec.get("traffic_volume", 0),
        # holiday: use the string label (same as the CSV "holiday" column)
        "holiday":             rec.get("holiday", "None") or "None",
        # temperature is stored in Kelvin in MongoDB (matches CSV which is also Kelvin)
        "temp":                rec.get("temp_kelvin",  rec.get("temp", 0.0)),
        "rain_1h":             rec.get("rain_1h_mm",   rec.get("rain_1h", 0.0)),
        "snow_1h":             rec.get("snow_1h_mm",   rec.get("snow_1h", 0.0)),
        "clouds_all":          rec.get("clouds_pct",   rec.get("clouds_all", 0)),
        "weather_main":        weather.get("main",        rec.get("weather_main", "Clear")),
        "weather_description": weather.get("description", rec.get("weather_description", "sky is clear")),
    }


def preprocess_records(records: list, feature_columns: list) -> pd.DataFrame:
    """
    Apply the Task 1 preprocessing pipeline to a list of raw API records
    and return a DataFrame aligned with the training feature columns.

    Steps:
      1. Flatten MongoDB document structure → CSV-like columns
      2. Parse date_time and sort chronologically
      3. Create is_holiday flag
      4. Compute moving averages: ma_24 (24-hour), ma_168 (7-day)
      5. Compute lag features: lag_1, lag_24, lag_168, lag_720
         (NaN lags occur when fewer records than the window are available;
          these are filled with 0 as a conservative fallback)
      6. One-hot encode: holiday, weather_main, weather_description
      7. Align columns with the training schema via reindex (fill missing → 0)
    """
    flat = [_flatten_mongo_record(r) for r in records]
    df = pd.DataFrame(flat)

    # 1. Parse and sort — after the MongoDB date migration, date_time is returned
    # as ISO format (e.g. "2018-09-30T23:00:00"); infer_datetime_format handles both
    df["date_time"] = pd.to_datetime(df["date_time"])
    df = df.sort_values("date_time").reset_index(drop=True)

    # 2. Holiday flag
    df["holiday"] = df["holiday"].fillna("None")
    df["is_holiday"] = (df["holiday"] != "None").astype(int)

    # 3. Moving averages (min_periods=1 so we always get a value even for short series)
    df["ma_24"]  = df["traffic_volume"].rolling(window=24,  min_periods=1).mean()
    df["ma_168"] = df["traffic_volume"].rolling(window=168, min_periods=1).mean()

    # 4. Lag features
    df["lag_1"]   = df["traffic_volume"].shift(1)
    df["lag_24"]  = df["traffic_volume"].shift(24)
    df["lag_168"] = df["traffic_volume"].shift(168)
    df["lag_720"] = df["traffic_volume"].shift(720)   # NaN unless 720+ records fetched

    # 5. One-hot encode categorical columns
    df = pd.get_dummies(
        df,
        columns=["holiday", "weather_main", "weather_description"],
        drop_first=True,
    )

    # Drop target and timestamp — not used as features
    df = df.drop(columns=["date_time", "traffic_volume"], errors="ignore")

    # 6. Fill NaN lag values with 0 (occurs when fewer records than lag window)
    df = df.fillna(0)

    # 7. Align with training feature schema:
    #    - Columns present in training but missing here → filled with 0
    #    - Extra columns not seen during training → dropped
    df = df.reindex(columns=feature_columns, fill_value=0)

    return df


# ──────────────────────────────────────────────────────────────────────────────
# Step 3 – Load model artifacts
# ──────────────────────────────────────────────────────────────────────────────

def load_artifacts():
    if not MODEL_PATH.exists():
        print(f"[ERROR] Model not found: {MODEL_PATH}")
        sys.exit(1)

    model        = joblib.load(MODEL_PATH)
    feature_cols = list(model.feature_names_in_)
    print(f"[Step 3] Model loaded from '{MODEL_PATH}'  ({len(feature_cols)} features)")
    return model, feature_cols


# ──────────────────────────────────────────────────────────────────────────────
# Step 4 – Predict
# ──────────────────────────────────────────────────────────────────────────────

def predict(model, X: pd.DataFrame) -> float:
    return float(model.predict(X)[0])


# ──────────────────────────────────────────────────────────────────────────────
# Main pipeline
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 55)
    print("  Task 4 – Traffic Volume Prediction Pipeline")
    print("=" * 55)

    # Step 1: Fetch
    records = fetch_records()
    if not records:
        print("[ERROR] API returned no records. Load data into the database first.")
        sys.exit(1)

    # Step 3: Load model (done before preprocessing so we can align columns)
    model, feature_columns = load_artifacts()

    # Step 2: Preprocess
    print(f"[Step 2] Preprocessing {len(records)} records…")
    X = preprocess_records(records, feature_columns)
    print(f"         Feature matrix shape: {X.shape}")

    # The latest (most recent) record is the last row after chronological sort
    X_latest = X.iloc[[-1]]

    # Step 4: Predict
    prediction = predict(model, X_latest)

    # Identify the latest record for display (records are returned newest-first by API,
    # but after our sort the last element of the original list is the most recent)
    sorted_records = sorted(records, key=lambda r: r.get("date_time", ""))
    latest = sorted_records[-1]
    latest_dt     = latest.get("date_time", "N/A")
    actual_volume = latest.get("traffic_volume", "N/A")

    print()
    print("=" * 55)
    print("  Prediction Result")
    print("=" * 55)
    print(f"  Timestamp          : {latest_dt}")
    print(f"  Holiday            : {latest.get('holiday', 'None')}")
    weather = latest.get("weather") or {}
    print(f"  Weather            : {weather.get('main', 'N/A')} – {weather.get('description', 'N/A')}")
    print(f"  Temperature (K)    : {latest.get('temp_kelvin', latest.get('temp', 'N/A'))}")
    print(f"  Actual volume      : {actual_volume} vehicles/hour")
    print(f"  Predicted volume   : {prediction:.0f} vehicles/hour")
    if isinstance(actual_volume, (int, float)):
        error = abs(actual_volume - prediction)
        print(f"  Absolute error     : {error:.0f} vehicles/hour")
    print("=" * 55)


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    main()
