#!/usr/bin/env python3
"""
Step 2 — Seed MongoDB with all three data files.

  farms.json              -> farms collection         (240 farm documents)
  ktda_pricing.json       -> ktda_pricing collection  (monthly rate history)
  synthetic_metadata.json -> factory_metadata         (single config document)

The metadata document is stored as a single record in factory_metadata and is
used by the preprocessing pipeline to load collection centre coordinates,
agronomic rule parameters, and pricing calibration constants without
hardcoding them in the training scripts.

Run inside the ml_engine container:
    docker compose exec ml_engine python scripts/seed_mongo.py

Full reset (drops all collections first):
    docker compose exec ml_engine python scripts/seed_mongo.py --drop
"""

import json
import os
import sys
from pathlib import Path

from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://maggie:6688diggy@diggy.rug2w.mongodb.net/chaimterics?retryWrites=true&w=majority&appName=Diggy")
DATA_DIR      = Path(os.getenv("DATA_DIR", str(Path(__file__).resolve().parent.parent / "data")))

# Array collections — each file contains a JSON array of documents
ARRAY_SEED_FILES = {
    "farms":        DATA_DIR / "farms.json",
    "ktda_pricing": DATA_DIR / "ktda_pricing.json",
}

# Single-document collection — file contains one JSON object, not an array
METADATA_FILE    = DATA_DIR / "synthetic_metadata.json"
METADATA_COLLECTION = "factory_metadata"


def load_json_array(path: Path) -> list:
    """Load a JSON file that contains an array (or an object wrapping one)."""
    if not path.exists():
        print(f"  [WARN] {path.name} not found — skipping.")
        return []
    with open(path) as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    # Support {"farms": [...]} style wrappers
    for key in ("farms", "pricing", "records", "data"):
        if key in data and isinstance(data[key], list):
            return data[key]
    print(f"  [WARN] {path.name}: unexpected structure — treating as single document.")
    return [data]


def load_json_object(path: Path) -> dict | None:
    """Load a JSON file that contains a single object."""
    if not path.exists():
        print(f"  [WARN] {path.name} not found — skipping.")
        return None
    with open(path) as f:
        return json.load(f)


def seed_array_collection(db, name: str, docs: list, drop_first: bool = False) -> int:
    if not docs:
        return 0
    col = db[name]
    if drop_first:
        col.drop()
        print(f"  Dropped '{name}'.")
    try:
        result = col.insert_many(docs, ordered=False)
        return len(result.inserted_ids)
    except BulkWriteError as e:
        inserted = e.details.get("nInserted", 0)
        dup_count = sum(1 for err in e.details.get("writeErrors", []) if err.get("code") == 11000)
        other_errors = [err for err in e.details.get("writeErrors", []) if err.get("code") != 11000]
        if dup_count:
            existing = col.count_documents({})
            print(f"  Skipped {dup_count} duplicates already in '{name}' ({existing} total docs).")
        if other_errors:
            print(f"  [WARN] {len(other_errors)} unexpected write errors on '{name}':")
            for err in other_errors[:3]:
                print(f"    {err.get('errmsg', '')[:120]}")
        return inserted + dup_count


def seed_metadata(db, drop_first: bool = False) -> bool:
    """
    Store synthetic_metadata.json as a single document in factory_metadata.
    Adds a _type field so downstream code can query it without guessing structure.
    """
    doc = load_json_object(METADATA_FILE)
    if doc is None:
        return False

    col = db[METADATA_COLLECTION]
    if drop_first:
        col.drop()
        print(f"  Dropped '{METADATA_COLLECTION}'.")

    doc["_type"] = "factory_metadata"
    col.replace_one({"_type": "factory_metadata"}, doc, upsert=True)

    # Confirm key sections landed correctly
    factories  = [f["factory_code"] for f in doc.get("factories", [])]
    centres    = {k: len(v) for k, v in doc.get("collection_centres", {}).items()}
    print(f"  Stored metadata: factories={factories}, centres={centres}")
    return True


def create_indexes(db):
    print("\nCreating indexes...")

    # farms
    db.farms.create_index([("ktda_member_no", ASCENDING)], unique=True, name="idx_member_no")
    db.farms.create_index([("factory_code", ASCENDING), ("collection_centre", ASCENDING)], name="idx_factory_centre")
    db.farms.create_index([("factory_code", ASCENDING)], name="idx_factory")
    print("  farms          : idx_member_no (unique), idx_factory_centre, idx_factory")

    # ktda_pricing
    db.ktda_pricing.create_index(
        [("season_year", ASCENDING), ("season_month_idx", ASCENDING), ("factory_code", ASCENDING)],
        name="idx_pricing_lookup",
    )
    db.ktda_pricing.create_index([("factory_code", ASCENDING)], name="idx_pricing_factory")
    print("  ktda_pricing   : idx_pricing_lookup, idx_pricing_factory")

    # factory_metadata — single document, light index
    db.factory_metadata.create_index([("_type", ASCENDING)], unique=True, name="idx_meta_type")
    print("  factory_metadata: idx_meta_type (unique)")

    # weather_history — populated by weather_fetch.py in Step 3
    db.weather_history.create_index(
        [("collection_centre", ASCENDING), ("year", ASCENDING)],
        unique=True, name="idx_weather_centre_year",
    )
    print("  weather_history: idx_weather_centre_year (unique) [ready for Step 3]")

    # model_outputs — populated by ML pipeline in Steps 4-6
    db.model_outputs.create_index([("ktda_member_no", ASCENDING)], unique=True, name="idx_output_member")
    db.model_outputs.create_index([("last_computed", ASCENDING)], name="idx_output_ts")
    print("  model_outputs  : idx_output_member (unique), idx_output_ts [ready for Steps 4-6]")


def main():
    drop = "--drop" in sys.argv
    if drop:
        print("WARNING: --drop flag set. All existing collections will be cleared.\n")

    print(f"Connecting to {MONGODB_URI} ...")
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)

    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        return  
    
    db = client.get_default_database()
    db.command("ping")
    print(f"Connected. Database: {db.name}\n")

    # 1. Array collections: farms + ktda_pricing
    for col_name, path in ARRAY_SEED_FILES.items():
        print(f"Loading {path.name} -> '{col_name}' ...")
        docs = load_json_array(path)
        if not docs:
            continue
        n = seed_array_collection(db, col_name, docs, drop_first=drop)
        print(f"  Inserted {n} documents into '{col_name}'.")

    # 2. Metadata: synthetic_metadata.json -> factory_metadata
    print(f"\nLoading {METADATA_FILE.name} -> '{METADATA_COLLECTION}' ...")
    seed_metadata(db, drop_first=drop)

    # 3. Indexes
    create_indexes(db)

    # 4. Summary
    print("\n-- Seed summary --")
    for col_name in ("farms", "ktda_pricing", METADATA_COLLECTION, "weather_history", "model_outputs"):
        count = db[col_name].count_documents({})
        print(f"  {col_name:<20}: {count} documents")

    print("\nDone. Run weather_fetch.py next to complete Step 3.")


if __name__ == "__main__":
    main()