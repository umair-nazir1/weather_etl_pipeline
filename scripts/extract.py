# scripts/extract.py
"""
Extract weather data from Open-Meteo API and save raw JSON to data/raw/.
Usage:
    python3 scripts/extract.py
"""
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import os
import json
from datetime import date, timedelta
import requests
from config import LOCATIONS, BASE_URL, HOURLY_VARS, TIMEZONE, RAW_DIR


# Ensure raw dir exists
os.makedirs(RAW_DIR, exist_ok=True)

def build_params(lat, lon, start_date, end_date):
    return {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY_VARS),
        "start_date": start_date,
        "end_date": end_date,
        "timezone": TIMEZONE,
    }

def fetch_weather_for_location(name, lat, lon, start_date, end_date, timeout=10):
    """
    Fetch JSON from Open-Meteo for given lat/lon and date range.
    Returns JSON dict on success, raises requests.HTTPError on failure.
    """
    params = build_params(lat, lon, start_date, end_date)
    resp = requests.get(BASE_URL, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

def save_raw_json(city_name, start_date, end_date, data):
    """Save raw JSON to data/raw/<city>_<start>_<end>.json"""
    safe_name = city_name.replace(" ", "_")
    filename = f"{RAW_DIR}/{safe_name}_{start_date}_{end_date}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return filename

def main(days_back=1):
    """
    By default fetches last (today and yesterday) to have recent hourly data.
    days_back=1 -> start_date = today - 1 day, end_date = today
    """
    today = date.today()
    start_date = (today - timedelta(days=days_back)).isoformat()
    end_date = today.isoformat()

    print(f"Fetching weather data from {start_date} to {end_date} for {len(LOCATIONS)} locations.")
    for city, coords in LOCATIONS.items():
        lat = coords["lat"]
        lon = coords["lon"]
        try:
            print(f"-> Fetching {city} ({lat},{lon}) ...")
            data = fetch_weather_for_location(city, lat, lon, start_date, end_date)
            saved = save_raw_json(city, start_date, end_date, data)
            print(f"   Saved raw JSON to: {saved}")
        except requests.HTTPError as e:
            print(f"   HTTP error for {city}: {e}")
        except requests.RequestException as e:
            print(f"   Request failed for {city}: {e}")
        except Exception as e:
            print(f"   Unexpected error for {city}: {e}")

if __name__ == "__main__":
    main()
