# fetch_weather.py
import os
from pathlib import Path
import pandas as pd
import requests_cache
from retry_requests import retry
import openmeteo_requests

LAT = float(os.getenv("LAT", "44.34"))      # London by default; override in workflow env
LON = float(os.getenv("LON", "10.99"))
CSV_PATH = os.getenv("CSV_PATH", "data/wind_data.csv")
WIND_SPEED_UNIT = os.getenv("WIND_SPEED_UNIT", "ms")  # ms | kmh | mph | kn

# Prepare directories
csv_file = Path(CSV_PATH)
csv_file.parent.mkdir(parents=True, exist_ok=True)

# Setup Open-Meteo API client with cache + retry
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": LAT,
    "longitude": LON,
    "timezone": "UTC",  # ensure timestamps are UTC
    "current": [
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_gusts_10m",
    ],
    "wind_speed_unit": WIND_SPEED_UNIT,
}

responses = openmeteo.weather_api(url, params=params)
response = responses[0]

# Current block: order must match 'current' list above
current = response.Current()
ts_utc = pd.to_datetime(current.Time(), unit="s", utc=True).isoformat()

row = {
    "timestamp_utc": ts_utc,
    "latitude": response.Latitude(),
    "longitude": response.Longitude(),
    "elevation_m": response.Elevation(),
    "temperature_2m_c": current.Variables(0).Value(),
    "relative_humidity_2m_pct": current.Variables(1).Value(),
    f"wind_speed_10m_{WIND_SPEED_UNIT}": current.Variables(2).Value(),
    "wind_direction_10m_deg": current.Variables(3).Value(),
    f"wind_gusts_10m_{WIND_SPEED_UNIT}": current.Variables(4).Value(),
}

df = pd.DataFrame([row])
file_exists = csv_file.exists()
df.to_csv(csv_file, mode="a" if file_exists else "w", header=not file_exists, index=False)

print(f"Appended weather row at {ts_utc} to {csv_file}")
