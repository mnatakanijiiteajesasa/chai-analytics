#!/usr/bin/env python3
"""
Phase 2 — Step 1: Build the farm graph.

Constructs nodes and edges from the farms collection and saves the
graph structure to MongoDB (farm_graph collection) ready for GNN training.

Edge rules (from roadmap):
  - Shared collection_centre     → weight 1.0
  - Shared factory_code          → weight 0.6
  - GPS proximity < 5km          → weight proportional to inverse distance

Run on WSL host:
    python scripts/build_graph.py
    python scripts/build_graph.py --validate
"""

import os
import math
import json
import argparse
from datetime import datetime
from pathlib import Path

import numpy as np
from pymongo import MongoClient

MONGODB_URI = os.getenv("MONGODB_URI")
if not MONGODB_URI:
    raise RuntimeError("MONGODB_URI environment variable is not set")
DATA_DIR      = Path(os.getenv("DATA_DIR", str(Path(__file__).resolve().parent.parent / "data")))

GPS_EDGE_THRESHOLD_KM = 5.0


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance between two GPS points in kilometres."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi  = math.radians(lat2 - lat1)
    dlam  = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def extract_node_features(farm: dict) -> dict:
    """
    Extract normalisation-ready features for one farm node.
    Returns raw values — normalisation happens in train_gnn.py.
    """
    seasons = sorted(farm.get("historical_seasons", []),
                     key=lambda s: s["season_year"])

    # Yield history: mean monthly kg per season (up to 15 seasons)
    season_means = []
    for s in seasons:
        kg = [v for v in s.get("monthly_kg", []) if v is not None]
        if kg:
            season_means.append(float(np.mean(kg)))

    # Pad/truncate to 15 seasons
    padded = (season_means + [0.0] * 15)[:15]

    # Recent yield trend: slope of last 5 season means
    recent = season_means[-5:] if len(season_means) >= 2 else season_means
    if len(recent) >= 2:
        xs    = np.arange(len(recent), dtype=float)
        slope = float(np.polyfit(xs, recent, 1)[0])
    else:
        slope = 0.0

    # Average rainfall from historical seasons
    rain_vals = []
    for s in seasons:
        r = s.get("season_rainfall_mm", [])
        valid = [v for v in r if v is not None]
        if valid:
            rain_vals.append(float(np.mean(valid)))
    avg_rainfall = float(np.mean(rain_vals)) if rain_vals else 100.0

    return {
        "season_yield_means":  padded,           # 15-dim yield history
        "overall_yield_mean":  float(np.mean(season_means)) if season_means else 0.0,
        "yield_trend_slope":   slope,
        "hectares":            float(farm.get("hectares", 1.0)),
        "altitude_m":          float(farm.get("altitude_m", 1700)),
        "avg_rainfall_mm":     avg_rainfall,
        "years_active":        len(seasons),
        "fairtrade":           int(farm.get("fairtrade_certified", False)),
        "n_seasons":           len(seasons),
    }


def build_edges(farms: list[dict]) -> list[dict]:
    """
    Build edge list following roadmap edge rules.
    Returns list of {src, dst, weight, edge_type} dicts.
    Edges are undirected — each pair stored once (src < dst by index).
    """
    edges = []
    n = len(farms)

    for i in range(n):
        fi = farms[i]
        for j in range(i + 1, n):
            fj = farms[j]

            max_weight = 0.0
            edge_type  = None

            # Rule 1: shared collection centre (strongest signal)
            if fi["collection_centre"] == fj["collection_centre"]:
                max_weight = 1.0
                edge_type  = "same_centre"

            # Rule 2: shared factory zone
            elif fi["factory_code"] == fj["factory_code"]:
                max_weight = 0.6
                edge_type  = "same_factory"

            # Rule 3: GPS proximity < 5km
            lat_i = fi.get("latitude")
            lon_i = fi.get("longitude")
            lat_j = fj.get("latitude")
            lon_j = fj.get("longitude")

            if all(v is not None for v in [lat_i, lon_i, lat_j, lon_j]):
                dist_km = haversine_km(lat_i, lon_i, lat_j, lon_j)
                if dist_km < GPS_EDGE_THRESHOLD_KM and dist_km > 0:
                    gps_weight = 1.0 / dist_km   # inverse distance
                    # Normalise: max weight at 0.1km → 10.0, cap at 0.8
                    gps_weight = min(0.8, gps_weight / 10.0)
                    if gps_weight > max_weight:
                        max_weight = gps_weight
                        edge_type  = "gps_proximity"

            if max_weight > 0:
                edges.append({
                    "src":        i,
                    "dst":        j,
                    "src_member": fi["ktda_member_no"],
                    "dst_member": fj["ktda_member_no"],
                    "weight":     round(max_weight, 4),
                    "edge_type":  edge_type,
                })

    return edges


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate", action="store_true",
                        help="Print graph stats after building")
    args = parser.parse_args()

    print(f"Connecting to {MONGODB_URI} ...")
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    db     = client.get_default_database()
    db.command("ping")
    print(f"Connected to {db.name}\n")

    # Load all farms
    farms = list(db.farms.find({}, {"_id": 0}))
    print(f"Loaded {len(farms)} farms.")

    # Build node feature dicts
    print("Extracting node features ...")
    nodes = []
    for i, farm in enumerate(farms):
        features = extract_node_features(farm)
        nodes.append({
            "node_idx":          i,
            "ktda_member_no":    farm["ktda_member_no"],
            "factory_code":      farm["factory_code"],
            "collection_centre": farm["collection_centre"],
            "latitude":          farm.get("latitude"),
            "longitude":         farm.get("longitude"),
            "features":          features,
        })

    # Build edge list
    print("Building edges ...")
    edges = build_edges(farms)

    # Edge type breakdown
    type_counts = {}
    for e in edges:
        type_counts[e["edge_type"]] = type_counts.get(e["edge_type"], 0) + 1

    print(f"  Nodes : {len(nodes)}")
    print(f"  Edges : {len(edges)}")
    for etype, count in type_counts.items():
        print(f"    {etype:<20}: {count}")

    # Save to MongoDB
    graph_doc = {
        "_id":        "farm_graph_v1",
        "n_nodes":    len(nodes),
        "n_edges":    len(edges),
        "nodes":      nodes,
        "edges":      edges,
        "edge_types": type_counts,
        "built_at":   datetime.utcnow().isoformat(),
        "status":     "built",
    }

    db.farm_graph.replace_one({"_id": "farm_graph_v1"}, graph_doc, upsert=True)
    print(f"\nSaved farm_graph_v1 to MongoDB.")

    if args.validate:
        # Connectivity check — avg edges per node
        degree = {}
        for e in edges:
            degree[e["src"]] = degree.get(e["src"], 0) + 1
            degree[e["dst"]] = degree.get(e["dst"], 0) + 1
        avg_degree = sum(degree.values()) / len(nodes) if nodes else 0

        print(f"\n-- Graph validation --")
        print(f"  Avg degree per node : {avg_degree:.1f}")
        print(f"  Max degree          : {max(degree.values()) if degree else 0}")
        print(f"  Isolated nodes      : {len(nodes) - len(degree)}")

        # Sample 3 edges
        print(f"\n-- Sample edges --")
        for e in edges[:3]:
            print(f"  {e['src_member']} ↔ {e['dst_member']}  "
                  f"weight={e['weight']}  type={e['edge_type']}")

    print("\nStep done. Run train_gnn.py next.")


if __name__ == "__main__":
    main()