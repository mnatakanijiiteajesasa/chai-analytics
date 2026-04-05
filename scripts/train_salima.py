#!/usr/bin/env python3
"""
Step 5b — Fit SARIMA models: per-farm yield forecasts + pricing forecasts.

Per-farm: one SARIMA per farm, fitted on that farm's monthly_kg time series.
Pricing:  three SARIMA per factory (monthly_rate, minibonus_rate, annual_bonus_rate).

All models persisted under models/sarima/.

Run inside ml_engine container (after preprocess.py):
    docker compose exec ml_engine python scripts/train_sarima.py

Options:
    --factory    Only fit farms in this factory (WRU-01 or RKR-01)
    --member     Only fit this one farm (ktda_member_no)
    --pricing    Only fit pricing SARIMA models (skip farm models)
    --farms-only Skip pricing models
    --validate   Print forecast sample for first farm after fitting
"""

import os
import sys
import json
import argparse
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import joblib
from pymongo import MongoClient
from pmdarima import auto_arima

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/chaimterics")
MODELS_DIR  = Path(os.getenv("MODELS_DIR", str(Path(__file__).resolve().parent.parent / "models")))
SARIMA_DIR  = MODELS_DIR / "sarima"
PRICING_DIR = SARIMA_DIR / "pricing"

MINIBONUS_MONTH_IDXS = {0, 1, 2, 4, 5}   # Jul=0, Aug=1, Sep=2, Nov=4, Dec=5
ANNUAL_BONUS_IDX     = 11                  # Jun


def fit_farm_sarima(series: pd.Series, member_no: str) -> dict | None:
    """
    Fit auto_arima on a July-anchored monthly yield series.
    Returns dict with model, forecast, and metadata. None on failure.
    """
    # Need at least 2 full seasons (24 months) for meaningful SARIMA
    if len(series) < 24:
        return {"status": "skipped", "reason": f"only {len(series)} months of data"}

    try:
        model = auto_arima(
            series,
            seasonal      = True,
            m             = 12,           # 12-month seasonality (July-anchored)
            stepwise      = True,
            suppress_warnings = True,
            error_action  = "ignore",
            max_p=3, max_q=3, max_P=2, max_Q=2,
            d=None, D=None,              # let auto_arima determine differencing
            information_criterion = "aic",
            n_jobs        = 1,
        )

        forecast, conf_int = model.predict(n_periods=6, return_conf_int=True, alpha=0.2)
        _, conf_int_95     = model.predict(n_periods=6, return_conf_int=True, alpha=0.05)

        return {
            "status":        "ok",
            "order":         model.order,
            "seasonal_order": model.seasonal_order,
            "aic":           float(model.aic()),
            "n_obs":         len(series),
            "forecast_6mo":  [max(0.0, float(v)) for v in forecast],
            "ci_80_lower":   [max(0.0, float(v)) for v in conf_int[:, 0]],
            "ci_80_upper":   [max(0.0, float(v)) for v in conf_int[:, 1]],
            "ci_95_lower":   [max(0.0, float(v)) for v in conf_int_95[:, 0]],
            "ci_95_upper":   [max(0.0, float(v)) for v in conf_int_95[:, 1]],
            "model":         model,
        }
    except Exception as e:
        return {"status": "failed", "reason": str(e)[:200]}


def fit_pricing_sarima(series: pd.Series, label: str) -> dict | None:
    """Fit SARIMA on a pricing series (monthly_rate, minibonus, annual_bonus)."""
    if len(series) < 6:
        return {"status": "skipped", "reason": f"only {len(series)} data points"}
    try:
        m = auto_arima(
            series,
            seasonal         = True,
            m                = min(12, len(series) // 2),
            stepwise         = True,
            suppress_warnings= True,
            error_action     = "ignore",
            max_p=2, max_q=2, max_P=1, max_Q=1,
            information_criterion = "aic",
            n_jobs           = 1,
        )
        periods  = 12
        fc, ci80 = m.predict(n_periods=periods, return_conf_int=True, alpha=0.2)
        _, ci95  = m.predict(n_periods=periods, return_conf_int=True, alpha=0.05)

        return {
            "status":    "ok",
            "label":     label,
            "order":     m.order,
            "aic":       float(m.aic()),
            "n_obs":     len(series),
            "forecast":  [max(0.0, float(v)) for v in fc],
            "ci_80_lower": [max(0.0, float(v)) for v in ci80[:, 0]],
            "ci_80_upper": [max(0.0, float(v)) for v in ci80[:, 1]],
            "ci_95_lower": [max(0.0, float(v)) for v in ci95[:, 0]],
            "ci_95_upper": [max(0.0, float(v)) for v in ci95[:, 1]],
            "model":     m,
        }
    except Exception as e:
        return {"status": "failed", "reason": str(e)[:200]}


def build_farm_series(farm: dict) -> pd.Series:
    """
    Flatten historical_seasons into a single ordered monthly_kg time series.
    Index is a simple integer — SARIMA only needs the sequence, not dates.
    """
    rows = []
    for season in sorted(farm.get("historical_seasons", []), key=lambda s: s["season_year"]):
        monthly_kg = season.get("monthly_kg", [])
        for idx, kg in enumerate(monthly_kg):
            rows.append(float(kg) if kg is not None else np.nan)
    series = pd.Series(rows, dtype=float)
    # Forward-fill isolated NaNs (don't interpolate pruning months out of existence)
    series = series.ffill().bfill()
    return series


def build_pricing_series(db, factory_code: str) -> dict[str, pd.Series]:
    """
    Build three pricing series for a factory:
      - monthly_rate:    all months
      - minibonus_rate:  minibonus months only (Jul/Aug/Sep/Nov/Dec)
      - annual_bonus:    Jun only (one value per season)
    """
    docs = list(db.ktda_pricing.find(
        {"factory_code": factory_code},
        {"_id": 0}
    ).sort([("season_year", 1), ("season_month_idx", 1)]))

    if not docs:
        print(f"  [WARN] No pricing data for {factory_code}")
        return {}

    df = pd.DataFrame(docs)

    # Inspect available fields
    rate_field = None
    for candidate in ("monthly_rate_kes_per_kg", "monthly_rate", "rate_kes_per_kg", "payment_rate", "rate"):
        if candidate in df.columns:
            rate_field = candidate
            break

    if rate_field is None:
        print(f"  [WARN] Cannot find rate field in ktda_pricing. Columns: {list(df.columns)}")
        return {}

    df = df.sort_values(["season_year", "season_month_idx"])

    monthly_rate  = pd.Series(df[rate_field].values, dtype=float)
    # Use boolean columns from schema if present, else derive from month index
    if "is_minibonus_month" in df.columns:
        minibonus_mask = df["is_minibonus_month"].astype(bool)
    else:
        minibonus_mask = df["season_month_idx"].isin(MINIBONUS_MONTH_IDXS)

    if "is_annual_bonus_month" in df.columns:
        annual_mask = df["is_annual_bonus_month"].astype(bool)
    else:
        annual_mask = df["season_month_idx"] == ANNUAL_BONUS_IDX

    minibonus_field = None
    for candidate in ("minibonus_rate_kes_per_kg", "minibonus_rate", "minibonus", "bonus_rate"):
        if candidate in df.columns:
            minibonus_field = candidate
            break

    annual_field = None
    for candidate in ("annual_bonus_rate_kes_per_kg", "annual_bonus_rate", "annual_bonus", "yearly_bonus_rate"):
        if candidate in df.columns:
            annual_field = candidate
            break

    series = {"monthly_rate": monthly_rate}
    if minibonus_field:
        series["minibonus_rate"] = pd.Series(
            df.loc[minibonus_mask, minibonus_field].values, dtype=float)
    elif minibonus_mask.any():
        series["minibonus_rate"] = pd.Series(
            df.loc[minibonus_mask, rate_field].values, dtype=float)

    if annual_field:
        series["annual_bonus"] = pd.Series(
            df.loc[annual_mask, annual_field].values, dtype=float)
    elif annual_mask.any():
        series["annual_bonus"] = pd.Series(
            df.loc[annual_mask, rate_field].values, dtype=float)

    return series


def fit_farm_models(db, args, sarima_dir: Path) -> tuple[int, int, int]:
    query = {}
    if args.factory:
        query["factory_code"] = args.factory
    if args.member:
        query["ktda_member_no"] = args.member

    farms = list(db.farms.find(query, {"ktda_member_no": 1, "factory_code": 1,
                                        "historical_seasons": 1, "_id": 0}))
    print(f"\nFitting SARIMA for {len(farms)} farms ...")

    ok = skipped = failed = 0
    manifest = []

    for i, farm in enumerate(farms):
        member_no = farm["ktda_member_no"]
        factory   = farm["factory_code"]
        series    = build_farm_series(farm)
        n_seasons = len(farm.get("historical_seasons", []))

        result = fit_farm_sarima(series, member_no)
        model_path = sarima_dir / factory / f"{member_no}.pkl"
        meta_path  = sarima_dir / factory / f"{member_no}_meta.json"
        model_path.parent.mkdir(parents=True, exist_ok=True)

        if result and result["status"] == "ok":
            model_obj = result.pop("model")
            joblib.dump(model_obj, model_path)
            with open(meta_path, "w") as f:
                json.dump({**result, "member_no": member_no, "factory": factory,
                           "n_seasons": n_seasons, "series_length": len(series)}, f)
            manifest.append({"member_no": member_no, "factory": factory,
                              "status": "ok", "aic": result["aic"],
                              "order": str(result["order"])})
            ok += 1
            if (i + 1) % 20 == 0:
                print(f"  [{i+1}/{len(farms)}] fitted {ok} OK, {skipped} skipped, {failed} failed")

        elif result and result["status"] == "skipped":
            manifest.append({"member_no": member_no, "factory": factory,
                              "status": "skipped", "reason": result["reason"]})
            skipped += 1
        else:
            reason = result["reason"] if result else "unknown"
            manifest.append({"member_no": member_no, "factory": factory,
                              "status": "failed", "reason": reason})
            print(f"  [FAIL] {member_no}: {reason[:80]}")
            failed += 1

    manifest_path = sarima_dir / "farm_manifest.parquet"
    pd.DataFrame(manifest).to_parquet(manifest_path, index=False)
    print(f"\n  Farm SARIMA — OK: {ok}  Skipped: {skipped}  Failed: {failed}")
    print(f"  Manifest saved: {manifest_path}")

    if args.validate and ok > 0:
        first_ok = next(m for m in manifest if m["status"] == "ok")
        member   = first_ok["member_no"]
        factory  = first_ok["factory"]
        meta_path = sarima_dir / factory / f"{member}_meta.json"
        with open(meta_path) as f:
            meta = json.load(f)
        print(f"\n  Sample forecast for {member}:")
        for i, (fc, lo, hi) in enumerate(zip(
                meta["forecast_6mo"], meta["ci_80_lower"], meta["ci_80_upper"])):
            print(f"    Month+{i+1}: {fc:.1f}kg  [80% CI: {lo:.1f} – {hi:.1f}]")

    return ok, skipped, failed


def fit_pricing_models(db, pricing_dir: Path):
    pricing_dir.mkdir(parents=True, exist_ok=True)
    factories = db.ktda_pricing.distinct("factory_code")
    print(f"\nFitting pricing SARIMA for factories: {factories} ...")

    for factory in factories:
        print(f"\n  {factory}:")
        series_map = build_pricing_series(db, factory)
        if not series_map:
            continue

        for label, series in series_map.items():
            series = series.dropna()
            print(f"    {label}: {len(series)} data points ...", end=" ", flush=True)
            result = fit_pricing_sarima(series, label)
            if result and result["status"] == "ok":
                model_obj  = result.pop("model")
                pkl_path   = pricing_dir / f"{factory}_{label}.pkl"
                meta_path  = pricing_dir / f"{factory}_{label}_meta.json"
                joblib.dump(model_obj, pkl_path)
                with open(meta_path, "w") as f:
                    json.dump({**result, "factory": factory}, f)
                print(f"OK  order={result['order']}  AIC={result['aic']:.1f}")
            else:
                reason = result["reason"] if result else "unknown"
                print(f"FAILED — {reason[:80]}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory",    type=str, default=None)
    parser.add_argument("--member",     type=str, default=None)
    parser.add_argument("--pricing",    action="store_true", help="Only fit pricing models")
    parser.add_argument("--farms-only", action="store_true", help="Skip pricing models")
    parser.add_argument("--validate",   action="store_true")
    args = parser.parse_args()

    SARIMA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Connecting to {MONGODB_URI} ...")
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    db = client.get_default_database()
    db.command("ping")
    print("Connected.\n")

    if not args.pricing:
        fit_farm_models(db, args, SARIMA_DIR)

    if not args.farms_only:
        fit_pricing_models(db, PRICING_DIR)

    print("\nStep 5b done. Run build_rule_engine.py + test_ollama.py next.")


if __name__ == "__main__":
    main()