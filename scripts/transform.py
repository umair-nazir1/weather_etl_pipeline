# scripts/transform.py
"""
Transform raw weather JSON (data/raw/*.json) into clean tabular CSVs (data/processed/).
Run from project root:
    python3 scripts/transform.py
"""

import sys
import os
import json
from glob import glob
from datetime import datetime
import pandas as pd

# Make sure project root is in sys.path so `config` imports work when run as a script
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import RAW_DIR, PROCESSED_DIR, HOURLY_VARS  # config constants

# Ensure processed dir exists
os.makedirs(PROCESSED_DIR, exist_ok=True)


def infer_city_and_dates_from_filename(filepath):
    """
    Example filename: data/raw/Karachi_2025-08-27_2025-08-28.json
    This returns: ("Karachi", "2025-08-27", "2025-08-28")
    If parsing fails, returns (basename_without_ext, None, None)
    """
    name = os.path.basename(filepath)
    name_no_ext = os.path.splitext(name)[0]
    parts = name_no_ext.rsplit("_", 2)
    if len(parts) == 3:
        city = parts[0]
        start_date = parts[1]
        end_date = parts[2]
        return city, start_date, end_date
    else:
        return name_no_ext, None, None


def process_single_raw_file(filepath):
    """
    - Load raw JSON
    - Normalize hourly -> pandas DataFrame
    - Clean/handle missing values
    - Add metadata columns (city, latitude, longitude)
    - Save per-file CSV and append to master CSV
    """
    print(f"Processing: {filepath}")
    with open(filepath, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # Basic validation
    hourly = raw.get("hourly")
    if not hourly or "time" not in hourly:
        print(f"  Skipped (no 'hourly'/'time' in JSON): {filepath}")
        return None

    times = hourly.get("time", [])
    # Choose which variables to use:
    # prefer those listed in HOURLY_VARS (config), but fall back to keys present in JSON
    available_vars = [k for k in HOURLY_VARS if k in hourly]
    if not available_vars:
        available_vars = [k for k in hourly.keys() if k != "time"]

    # Build dictionary for DataFrame
    data_dict = {"time": times}
    for var in available_vars:
        data_dict[var] = hourly.get(var, [None] * len(times))

    df = pd.DataFrame(data_dict)

    # Convert time column to pandas datetime for easier processing
    df["time"] = pd.to_datetime(df["time"])

    # Add metadata columns if available in raw JSON
    # Some APIs include latitude/longitude in root
    lat = raw.get("latitude")
    lon = raw.get("longitude")
    if lat is not None:
        df["latitude"] = lat
    if lon is not None:
        df["longitude"] = lon

    # Get city and dates from filename (fallback to root info)
    city, start_date, end_date = infer_city_and_dates_from_filename(filepath)
    df["city"] = city

    # ------------------ Cleaning steps ------------------
    # 1. Remove rows where all measured variables are NaN
    measure_cols = [c for c in available_vars]
    if measure_cols:
        df = df.dropna(axis=0, how="all", subset=measure_cols)

        # 2. Interpolate numeric gaps (linear). Then forward/back fill remaining edge NaNs.
        try:
            df[measure_cols] = df[measure_cols].interpolate(method="linear", limit_direction="both")
            df[measure_cols] = df[measure_cols].ffill().bfill()

        except Exception as e:
            # If interpolation fails (e.g., non-numeric), just forward/backfill
            df[measure_cols] = df[measure_cols].ffill().bfill()


    # 3. Reorder columns for readability: time, city, latitude, longitude, measures...
    cols = ["time", "city"]
    if "latitude" in df.columns:
        cols += ["latitude"]
    if "longitude" in df.columns:
        cols += ["longitude"]
    cols += measure_cols
    # keep only existing columns in that order
    cols = [c for c in cols if c in df.columns]
    df = df[cols]

    # ------------------ Save outputs ------------------
    # Save per-file processed CSV
    safe_city = city.replace(" ", "_")
    if start_date and end_date:
        out_name = f"{safe_city}_{start_date}_{end_date}.csv"
    else:
        # fallback to timestamped filename
        out_name = f"{safe_city}_processed_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    out_path = os.path.join(PROCESSED_DIR, out_name)
    df.to_csv(out_path, index=False)
    print(f"  Saved processed CSV: {out_path}")

    # Also append to a master CSV (all cities)
    master_path = os.path.join(PROCESSED_DIR, "all_cities_hourly.csv")
    if not os.path.exists(master_path):
        df.to_csv(master_path, index=False, mode="w", header=True)
        print(f"  Created master CSV: {master_path}")
    else:
        df.to_csv(master_path, index=False, mode="a", header=False)
        print(f"  Appended to master CSV: {master_path}")

    return out_path


def main():
    raw_files = glob(os.path.join(RAW_DIR, "*.json"))
    if not raw_files:
        print(f"No raw JSON files found in {RAW_DIR}. Run extract step first.")
        return

    for fpath in raw_files:
        try:
            process_single_raw_file(fpath)
        except Exception as e:
            print(f"  Error processing {fpath}: {e}")


if __name__ == "__main__":
    main()
