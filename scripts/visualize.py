# scripts/visualize.py
"""
Create simple charts (PNG images) from the ETL output.
- Prefers SQLite DB (data/weather.db, table: weather_data)
- Falls back to CSV (data/processed/all_cities_hourly.csv) if DB not found
- Saves charts into reports/ folder

Run from project root:
    python3 scripts/visualize.py
"""

import os
from datetime import datetime, timedelta
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# ---- Paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "weather.db"
CSV_PATH = PROJECT_ROOT / "data" / "processed" / "all_cities_hourly.csv"
REPORTS_DIR = PROJECT_ROOT / "reports"

# Ensure reports dir exists
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def load_dataset(days_back: int = 2) -> pd.DataFrame:
    """
    Load last N days of data.
    Priority: SQLite DB -> CSV fallback.
    Returns a DataFrame with at least: time, city, temperature_2m, relativehumidity_2m, precipitation
    """
    if DB_PATH.exists():
        conn = sqlite3.connect(DB_PATH)
        try:
            df = pd.read_sql_query("SELECT * FROM weather_data", conn)
        finally:
            conn.close()
    elif CSV_PATH.exists():
        df = pd.read_csv(CSV_PATH)
    else:
        raise FileNotFoundError(
            "No data source found. Run the pipeline first (extract -> transform -> load)."
        )

    # Ensure proper dtypes
    df["time"] = pd.to_datetime(df["time"])
    # Filter to last N days
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=days_back)
    df = df[df["time"] >= cutoff]

    # Sort for nice plotting
    df = df.sort_values(["city", "time"]).reset_index(drop=True)
    return df


def plot_city_timeseries(df: pd.DataFrame, city: str) -> None:
    """
    Create three plots for a given city:
    1) Temperature (line)
    2) Humidity (line)
    3) Precipitation (bar)
    Files are saved into reports/ as PNG.
    """
    cdf = df[df["city"] == city].copy()
    if cdf.empty:
        print(f"[skip] No rows for city: {city}")
        return

    # --- Temperature
    plt.figure()
    plt.plot(cdf["time"], cdf["temperature_2m"])
    plt.title(f"Temperature (°C) — {city}")
    plt.xlabel("Time")
    plt.ylabel("°C")
    plt.xticks(rotation=45)
    plt.tight_layout()
    out_path = REPORTS_DIR / f"temp_trend_{city.replace(' ', '_')}.png"
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"[ok] Saved: {out_path}")

    # --- Humidity
    if "relativehumidity_2m" in cdf.columns:
        plt.figure()
        plt.plot(cdf["time"], cdf["relativehumidity_2m"])
        plt.title(f"Relative Humidity (%) — {city}")
        plt.xlabel("Time")
        plt.ylabel("%")
        plt.xticks(rotation=45)
        plt.tight_layout()
        out_path = REPORTS_DIR / f"humidity_trend_{city.replace(' ', '_')}.png"
        plt.savefig(out_path, dpi=150)
        plt.close()
        print(f"[ok] Saved: {out_path}")

    # --- Precipitation
    if "precipitation" in cdf.columns:
        plt.figure()
        plt.bar(cdf["time"], cdf["precipitation"])
        plt.title(f"Precipitation (mm) — {city}")
        plt.xlabel("Time")
        plt.ylabel("mm")
        plt.xticks(rotation=45)
        plt.tight_layout()
        out_path = REPORTS_DIR / f"precipitation_{city.replace(' ', '_')}.png"
        plt.savefig(out_path, dpi=150)
        plt.close()
        print(f"[ok] Saved: {out_path}")


def main():
    print(">>> Starting visualize.py ...")
    print("DB exists?", DB_PATH.exists())
    print("CSV exists?", CSV_PATH.exists())

    df = load_dataset(days_back=2)
    print(">>> Loaded rows:", len(df))

    for city in sorted(df["city"].dropna().unique()):
        print(">>> Plotting for city:", city)
        plot_city_timeseries(df, city)

    print("\n✅ Charts created in the 'reports/' folder.")



if __name__ == "__main__":
    main()
