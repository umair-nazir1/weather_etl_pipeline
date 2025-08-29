import pandas as pd
import sqlite3
from pathlib import Path

# 1) Paths define karo
processed_file = Path("data/processed/all_cities_hourly.csv")
db_file = Path("data/weather.db")

# 2) CSV read karo
df = pd.read_csv(processed_file)

# 3) Database connection banao
conn = sqlite3.connect(db_file)

# 4) Data ko ek table me load karo
df.to_sql("weather_data", conn, if_exists="replace", index=False)

# 5) Connection close karo
conn.close()

print(f"✅ Data loaded into database: {db_file}, table: weather_data")
