# Weather ETL Pipeline

**Short:** End-to-end ETL pipeline that extracts hourly weather data (Open-Meteo), transforms JSON → tabular CSV, loads into SQLite, and generates PNG reports. Designed to be reproducible, configurable, and easy to extend to Postgres / orchestration.

## Features
- Extracts hourly weather data for configurable cities.
- Stores raw JSON (immutable) and processed CSV (`data/processed/all_cities_hourly.csv`).
- Loads processed data into `data/weather.db` (SQLite).
- Generates per-city PNG reports in `reports/` (temperature, humidity, precipitation).
- Single-command runner: `./run_pipeline.sh`
- Configurable via `config.py` (cities, variables, timezone, date range)

## Quick demo (include screenshots)
![Temperature — Karachi](reports/temp_trend_Karachi.png)
![Humidity — Lahore](reports/humidity_trend_Lahore.png)

## Tech
- Python 3.10+
- pandas, requests, matplotlib
- SQLite (local); easily switchable to Postgres
- Optional: Docker + Airflow for production orchestration

## Getting started (local, reproducible)
1. Clone repo (or add files in GitHub Desktop).
2. Create venv & install deps:
```bash
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
