# config.py
# Simple configuration for Weather ETL project

LOCATIONS = {
    "Karachi": {"lat": 24.8607, "lon": 67.0011},
    "Lahore": {"lat": 31.5204, "lon": 74.3587},
    "Islamabad": {"lat": 33.6844, "lon": 73.0479},
    "Multan": {"lat": 30.1575, "lon": 71.5249},
    "Quetta": {"lat": 30.1798, "lon": 66.9750},
}

# Date range for data extraction
DATE_RANGE = {
    "start": "2025-08-27",
    "end": "2025-08-28",
}

BASE_URL = "https://api.open-meteo.com/v1/forecast"
# hourly variables we want from the API
HOURLY_VARS = [
    "temperature_2m",
    "relativehumidity_2m",
    "precipitation",
    "weathercode",
]
TIMEZONE = "Asia/Karachi"

DATA_DIR = "data"
RAW_DIR = f"{DATA_DIR}/raw"
PROCESSED_DIR = f"{DATA_DIR}/processed"

DB_FILE = "weather.db"  # SQLite file (we'll use later)
