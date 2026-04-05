#!/usr/bin/env python3
"""
Step 5a — Train the XGBoost yield prediction model.

Loads the feature matrix from preprocess.py, trains XGBoost with 5-fold CV,
extracts feature importances, and persists the model to models/xgb_yield.pkl.

Run inside ml_engine container (after preprocess.py):
    docker compose exec ml_engine python scripts/train_xgboost.py
    docker compose exec ml_engine python scripts/train_xgboost.py --validate
"""

import os
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import joblib
import xgboost as xgb
from sklearn.model_selection import KFold, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error

MODELS_DIR = Path(os.getenv("MODELS_DIR", str(Path(__file__).resolve().parent.parent / "models")))


def load_artifacts(models_dir: Path):
    X         = pd.read_parquet(models_dir / "X_features.parquet")
    y         = pd.read_parquet(models_dir / "y_target.parquet")["monthly_kg"]
    row_ids   = pd.read_parquet(models_dir / "row_ids.parquet")
    pre       = joblib.load(models_dir / "preprocessor.pkl")
    return X, y, row_ids, pre


def train(X_scaled: np.ndarray, y: np.ndarray, feature_names: list) -> tuple:
    """
    Train XGBoost regressor and evaluate with 5-fold cross-validation.
    Returns (fitted model, cv_results dict).
    """
    model = xgb.XGBRegressor(
        n_estimators      = 400,
        max_depth         = 6,
        learning_rate     = 0.05,
        subsample         = 0.8,
        colsample_bytree  = 0.8,
        min_child_weight  = 5,
        gamma             = 0.1,
        reg_alpha         = 0.1,   # L1
        reg_lambda        = 1.0,   # L2
        objective         = "reg:squarederror",
        random_state      = 42,
        n_jobs            = -1,
        tree_method       = "hist",   # fast on CPU
    )

    print("  Running 5-fold cross-validation ...")
    kf      = KFold(n_splits=5, shuffle=True, random_state=42)
    mae_scores  = []
    rmse_scores = []
    mape_scores = []

    for fold, (train_idx, val_idx) in enumerate(kf.split(X_scaled)):
        X_tr, X_val = X_scaled[train_idx], X_scaled[val_idx]
        y_tr, y_val = y[train_idx],        y[val_idx]

        m = xgb.XGBRegressor(**model.get_params())
        m.fit(X_tr, y_tr,
              eval_set=[(X_val, y_val)],
              verbose=False)

        preds = m.predict(X_val)
        mae   = mean_absolute_error(y_val, preds)
        rmse  = mean_squared_error(y_val, preds, squared=False)
        # MAPE — exclude near-zero targets to avoid division instability
        mask  = y_val > 10
        mape  = np.mean(np.abs((y_val[mask] - preds[mask]) / y_val[mask])) * 100 if mask.any() else np.nan

        mae_scores.append(mae)
        rmse_scores.append(rmse)
        mape_scores.append(mape)
        print(f"    Fold {fold+1}: MAE={mae:.1f}kg  RMSE={rmse:.1f}kg  MAPE={mape:.1f}%")

    cv_results = {
        "mae_mean":  float(np.mean(mae_scores)),
        "mae_std":   float(np.std(mae_scores)),
        "rmse_mean": float(np.mean(rmse_scores)),
        "mape_mean": float(np.nanmean(mape_scores)),
    }

    print(f"\n  CV mean — MAE: {cv_results['mae_mean']:.1f}kg  "
          f"RMSE: {cv_results['rmse_mean']:.1f}kg  "
          f"MAPE: {cv_results['mape_mean']:.1f}%")

    mean_yield = float(np.mean(y))
    pct = cv_results["mae_mean"] / mean_yield * 100
    target_ok = pct < 15.0
    print(f"  MAE as % of mean yield ({mean_yield:.0f}kg): {pct:.1f}%  "
          f"{'OK — within 15% target' if target_ok else 'ABOVE 15% target — check features'}")

    # Refit on full dataset
    print("\n  Fitting on full dataset ...")
    model.fit(X_scaled, y, verbose=False)

    return model, cv_results


def extract_importances(model, feature_names: list) -> pd.DataFrame:
    imp = pd.DataFrame({
        "feature":    feature_names,
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=False).reset_index(drop=True)
    return imp


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate", action="store_true",
                        help="Print feature importances and sample predictions after training.")
    args = parser.parse_args()

    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Loading feature matrix from {MODELS_DIR} ...")
    X, y, row_ids, preprocessor = load_artifacts(MODELS_DIR)
    print(f"  X shape: {X.shape}  y shape: {y.shape}")

    print("\nApplying preprocessor ...")
    X_scaled = preprocessor.transform(X)
    feature_names = (
        list(X.columns[:len([c for c in X.columns if X[c].dtype != object])]) +
        list(X.select_dtypes(include="object").columns)
    )
    # Simpler: use column order from X directly
    feature_names = list(X.columns)

    print(f"\nTraining XGBoost on {X_scaled.shape[0]} rows x {X_scaled.shape[1]} features ...")
    model, cv_results = train(X_scaled, y.values, feature_names)

    # Feature importances
    importances = extract_importances(model, feature_names)

    # Save
    model_path = MODELS_DIR / "xgb_yield.pkl"
    imp_path   = MODELS_DIR / "xgb_feature_importances.parquet"
    cv_path    = MODELS_DIR / "xgb_cv_results.parquet"

    joblib.dump(model, model_path)
    importances.to_parquet(imp_path, index=False)
    pd.DataFrame([cv_results]).to_parquet(cv_path, index=False)

    print(f"\n-- Saved --")
    print(f"  {model_path}")
    print(f"  {imp_path}")
    print(f"  {cv_path}")

    print(f"\n-- Top 10 feature importances --")
    print(importances.head(10).to_string(index=False))

    if args.validate:
        print(f"\n-- Sample predictions vs actuals (10 rows) --")
        X_scaled_full = preprocessor.transform(X)
        preds = model.predict(X_scaled_full[:10])
        sample = row_ids.head(10).copy()
        sample["actual_kg"]    = y.values[:10]
        sample["predicted_kg"] = preds.round(1)
        sample["error_kg"]     = (sample["predicted_kg"] - sample["actual_kg"]).round(1)
        print(sample[["ktda_member_no","season_year","season_month_name",
                       "actual_kg","predicted_kg","error_kg"]].to_string(index=False))

    print("\nStep 5a done. Run train_sarima.py next.")


if __name__ == "__main__":
    main()