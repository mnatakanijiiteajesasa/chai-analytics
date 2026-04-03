#!/usr/bin/env python3
"""
Step 1 — Verify the Phase 1 environment is fully operational.

Run inside the ml_engine container to check packages and MongoDB:
    docker compose exec ml_engine python scripts/verify_env.py

Run on WSL host to check Ollama and Open-Meteo (they need direct network access):
    python scripts/verify_env.py --host-checks
"""
import sys
import importlib
import os
import argparse

REQUIRED_PACKAGES = [
    ("xgboost",     "xgboost"),
    ("lightgbm",    "lightgbm"),
    ("statsmodels", "statsmodels"),
    ("pmdarima",    "pmdarima"),
    ("sklearn",     "scikit-learn"),
    ("pandas",      "pandas"),
    ("numpy",       "numpy"),
    ("pymongo",     "pymongo"),
    ("joblib",      "joblib"),
    ("requests",    "requests"),
    ("flask",       "flask"),
]


def check_packages():
    print("── Package check ──────────────────────────────")
    all_ok = True
    for import_name, pip_name in REQUIRED_PACKAGES:
        try:
            mod = importlib.import_module(import_name)
            version = getattr(mod, "__version__", "?")
            print(f"  OK  {pip_name:<20} {version}")
        except ImportError:
            print(f"  MISSING  {pip_name}")
            all_ok = False
    return all_ok


def check_mongo():
    print("\n── MongoDB connection ─────────────────────────")
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/chaimterics")
    try:
        from pymongo import MongoClient
        client = MongoClient(uri, serverSelectionTimeoutMS=4000)
        db = client.get_default_database()
        db.command("ping")
        print(f"  OK  Connected to {uri}")
        collections = db.list_collection_names()
        print(f"  Collections: {collections if collections else '(empty — run seed_mongo.py)'}")
        for col in ("farms", "ktda_pricing", "factory_metadata"):
            count = db[col].count_documents({}) if col in collections else 0
            print(f"    {col:<20}: {count} documents")
        return True
    except Exception as e:
        print(f"  FAIL  MongoDB unreachable: {e}")
        return False


def check_ollama():
    print("\n── Ollama connection ──────────────────────────")
    # Ollama must bind to 0.0.0.0 to be reachable from Docker.
    # Start it on WSL host with: OLLAMA_HOST=0.0.0.0 ollama serve
    host = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    try:
        import requests
        resp = requests.get(f"{host}/api/tags", timeout=5)
        resp.raise_for_status()
        models = [m["name"] for m in resp.json().get("models", [])]
        print(f"  OK  Ollama reachable at {host}")
        if models:
            print(f"  Models: {models}")
            if any("mistral" in m or "llama" in m for m in models):
                print("  OK  mistral or llama3 found")
            else:
                print("  !   No mistral/llama3. Run: ollama pull mistral")
        else:
            print("  !   No models pulled yet. Run: ollama pull mistral")
        return True
    except Exception as e:
        print(f"  FAIL  Ollama unreachable: {e}")
        print()
        print("  To fix — start Ollama bound to all interfaces:")
        print("    OLLAMA_HOST=0.0.0.0 ollama serve")
        print()
        print("  Then in docker-compose.yml, OLLAMA_HOST should point to")
        print("  your WSL IP (run: ip route | grep default | awk '{print $3}')")
        print("  e.g.  OLLAMA_HOST: http://172.20.0.1:11434")
        return False


def check_open_meteo():
    print("\n── Open-Meteo API reachability ────────────────")
    try:
        import requests
        resp = requests.get(
            "https://archive-api.open-meteo.com/v1/archive",
            params={
                "latitude": -0.325, "longitude": 37.625,
                "start_date": "2020-01-01", "end_date": "2020-01-31",
                "monthly": "precipitation_sum",
            },
            timeout=15,
        )
        resp.raise_for_status()
        print("  OK  Open-Meteo archive API reachable")
        return True
    except Exception as e:
        print(f"  FAIL  Open-Meteo unreachable: {e}")
        print()
        print("  NOTE: Docker containers have no internet egress in this setup.")
        print("  Run weather_fetch.py directly on your WSL host, not inside Docker:")
        print("    python scripts/weather_fetch.py --centre Marima")
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host-checks", action="store_true",
                        help="Also check Ollama and Open-Meteo (run on WSL host, not in Docker)")
    args = parser.parse_args()

    print("ChaiMetrics — Phase 1 Environment Verification")
    print("=" * 50)

    results = {}
    results["Python packages"] = check_packages()
    results["MongoDB"]         = check_mongo()

    if args.host_checks:
        results["Ollama"]         = check_ollama()
        results["Open-Meteo API"] = check_open_meteo()
    else:
        print("\n(Skipping Ollama + Open-Meteo — run with --host-checks on WSL host)")

    print("\n── Summary ────────────────────────────────────")
    for label, ok in results.items():
        print(f"  {'OK  ' if ok else 'FAIL'} {label}")

    if all(results.values()):
        print("\nAll checks passed.")
    else:
        print("\nSome checks failed — see above.")
        sys.exit(1)


if __name__ == "__main__":
    main()