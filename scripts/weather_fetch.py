#!/usr/bin/env python3
"""
Step 3 — Fetch historical weather from Open-Meteo and cache in MongoDB.

Run this on your host (WSL), NOT inside Docker.

Setup:
    python3 -m venv venv
    source venv/bin/activate
    pip install requests pymongo

Usage:
    python scripts/weather_fetch.py --centre "Marima"
    python scripts/weather_fetch.py --dry-run
    python scripts/weather_fetch.py --refetch
"""

import os
import sys
import time
import json
import argparse
from datetime import datetime
from pathlib import Path

import requests
from pymongo import MongoClient


MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/chaimterics")
DATA_DIR = Path(os.getenv("DATA_DIR", str(Path(__file__).resolve().parent.parent / "data")))
METADATA_FILE = DATA_DIR / "synthetic_metadata.json"

SEASON_START_YEAR = 2010
SEASON_END_YEAR = 2024
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
SLEEP_BETWEEN_CALLS = 0.4


COLLECTION_CENTRES_FALLBACK = {
    "Marima": {"lat": -0.325, "lon": 37.625, "factory": "WRU-01", "alt_m": 1780},
    "Chuka": {"lat": -0.338, "lon": 37.651, "factory": "WRU-01", "alt_m": 1720},
}


#  LOAD CENTRES 
def load_centres():
    if not METADATA_FILE.exists():
        print(f"[INFO] Metadata not found, using fallback centres.")
        return COLLECTION_CENTRES_FALLBACK

    with open(METADATA_FILE) as f:
        meta = json.load(f)

    centres = {}
    for factory, centre_list in meta.get("collection_centres", {}).items():
        for c in centre_list:
            centres[c["name"]] = {
                "lat": c["lat"],
                "lon": c["lng"],
                "factory": factory,
                "alt_m": c["altitude_m"],
            }

    print(f"[INFO] Loaded {len(centres)} centres.")
    return centres


#  FETCH + AGGREGATE 
def fetch_monthly_weather(lat, lon, year):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "daily": "precipitation_sum,temperature_2m_mean",
        "timezone": "Africa/Nairobi",
    }

    try:
        r = requests.get(OPEN_METEO_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json().get("daily", {})

        dates = data.get("time", [])
        rain = data.get("precipitation_sum", [])
        temp = data.get("temperature_2m_mean", [])

        if not dates:
            return None

        monthly_rain = [0] * 12
        monthly_temp = [[] for _ in range(12)]

        for d, r_val, t_val in zip(dates, rain, temp):
            month = int(d.split("-")[1]) - 1

            if r_val is not None:
                monthly_rain[month] += r_val

            if t_val is not None:
                monthly_temp[month].append(t_val)

        monthly_temp_avg = [
            (sum(m) / len(m)) if m else None for m in monthly_temp
        ]

        return {
            "rainfall_mm": monthly_rain,
            "temp_c": monthly_temp_avg,
        }

    except requests.RequestException as e:
        print(f"FAILED ({e})")
        return None


#  BUILD DOCUMENT 
def build_doc(centre, meta, season_year, this_year, next_year):
    return {
        "collection_centre": centre,
        "factory_code": meta["factory"],
        "lat": meta["lat"],
        "lon": meta["lon"],
        "altitude_m": meta["alt_m"],
        "year": season_year,
        "monthly_rainfall_mm": this_year["rainfall_mm"][6:] + next_year["rainfall_mm"][:6],
        "monthly_temp_c": this_year["temp_c"][6:] + next_year["temp_c"][:6],
        "fetched_at": datetime.utcnow().isoformat(),
    }


# MAIN 
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--centre", type=str)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--refetch", action="store_true")
    args = parser.parse_args()

    centres = load_centres()
    if args.centre:
        centres = {k: v for k, v in centres.items() if k == args.centre}

    if not centres:
        print("No centres found.")
        sys.exit(1)

    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    db = client.get_default_database()
    col = db["weather_history"]

    print(f"\nFetching weather for {len(centres)} centre(s)\n")

    fetched = skipped = errors = 0

    for centre, meta in centres.items():
        cache = {}

        for year in range(SEASON_START_YEAR, SEASON_END_YEAR + 1):

            if not args.refetch and col.find_one({"collection_centre": centre, "year": year}):
                skipped += 1
                continue

            for y in [year, year + 1]:
                if y not in cache:
                    print(f"Fetching {centre} {y}...", end=" ")
                    res = fetch_monthly_weather(meta["lat"], meta["lon"], y)
                    cache[y] = res

                    if res:
                        print("OK")
                    else:
                        print("FAIL")
                        errors += 1

                    time.sleep(SLEEP_BETWEEN_CALLS)

            if not cache.get(year) or not cache.get(year + 1):
                errors += 1
                continue

            doc = build_doc(centre, meta, year, cache[year], cache[year + 1])

            col.replace_one(
                {"collection_centre": centre, "year": year},
                doc,
                upsert=True
            )

            fetched += 1
            print(f"Saved {centre} {year}/{year+1}")

    print("\n--- DONE ---")
    print(f"Fetched : {fetched}")
    print(f"Skipped : {skipped}")
    print(f"Errors  : {errors}")
    print(f"Total   : {col.count_documents({})}")


if __name__ == "__main__":
    main()