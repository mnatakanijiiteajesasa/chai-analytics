"""
api/blueprints/pricing.py

GET /pricing/trends/<factory_code>
    Returns SARIMA pricing forecasts for monthly_rate, minibonus_rate,
    and annual_bonus for a given factory (WRU-01 or RKR-01).
    Also returns historical rate data for charting.

GET /pricing/centres
    Returns list of factories and collection centres.
"""

from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required

from api.utils import get_db, get_pricing_meta

pricing_bp = Blueprint("pricing", __name__)

SEASON_MONTH_NAMES = ["Jul","Aug","Sep","Oct","Nov","Dec","Jan","Feb","Mar","Apr","May","Jun"]
MINIBONUS_MONTHS   = {0, 1, 2, 4, 5}


@pricing_bp.route("/trends/<factory_code>", methods=["GET"])
@jwt_required()
def pricing_trends(factory_code: str):
    db = get_db()

    valid_factories = db.ktda_pricing.distinct("factory_code")
    if factory_code not in valid_factories:
        return jsonify({
            "error": f"Unknown factory '{factory_code}'",
            "valid": valid_factories,
        }), 404

    #  Historical pricing data 
    docs = list(
        db.ktda_pricing.find(
            {"factory_code": factory_code},
            {"_id": 0},
        ).sort([("season_year", 1), ("season_month_idx", 1)])
    )

    # Normalise field names across schema versions
    def rate_val(doc, *candidates):
        for c in candidates:
            if c in doc and doc[c] is not None:
                return doc[c]
        return None

    historical = []
    for doc in docs:
        historical.append({
            "season_year":       doc.get("season_year"),
            "season_month_idx":  doc.get("season_month_idx"),
            "season_month":      doc.get("season_month",
                                    SEASON_MONTH_NAMES[doc.get("season_month_idx", 0)]),
            "period":            doc.get("period"),
            "monthly_rate":      rate_val(doc, "monthly_rate_kes_per_kg", "monthly_rate", "rate"),
            "minibonus_rate":    rate_val(doc, "minibonus_rate_kes_per_kg", "minibonus_rate"),
            "annual_bonus_rate": rate_val(doc, "annual_bonus_rate_kes_per_kg", "annual_bonus_rate"),
            "is_minibonus_month":  doc.get("is_minibonus_month",
                                    doc.get("season_month_idx") in MINIBONUS_MONTHS),
            "is_annual_bonus_month": doc.get("is_annual_bonus_month",
                                    doc.get("season_month_idx") == 11),
        })

    #  SARIMA forecasts 
    pricing_meta = get_pricing_meta(factory_code)
    forecasts = {}
    for label, meta in pricing_meta.items():
        if meta.get("status") == "ok":
            forecasts[label] = {
                "forecast":    meta.get("forecast", []),
                "ci_80_lower": meta.get("ci_80_lower", []),
                "ci_80_upper": meta.get("ci_80_upper", []),
                "n_periods":   len(meta.get("forecast", [])),
                "order":       meta.get("order"),
                "aic":         meta.get("aic"),
            }

    # Summary stats 
    rates = [h["monthly_rate"] for h in historical if h["monthly_rate"]]
    summary = {
        "factory_code":   factory_code,
        "seasons_on_record": len(set(h["season_year"] for h in historical)),
        "rate_min":       round(min(rates), 2) if rates else None,
        "rate_max":       round(max(rates), 2) if rates else None,
        "rate_latest":    rates[-1] if rates else None,
        "forecast_available": list(forecasts.keys()),
    }

    return jsonify({
        "summary":    summary,
        "historical": historical,
        "forecasts":  forecasts,
    })


@pricing_bp.route("/centres", methods=["GET"])
@jwt_required()
def list_centres():
    """
    GET /pricing/centres
    Returns factory + collection centre structure from factory_metadata.
    Useful for populating filter dropdowns in the dashboard.
    """
    db   = get_db()
    meta = db.factory_metadata.find_one({"_type": "factory_metadata"}, {"_id": 0})
    if not meta:
        return jsonify({"error": "factory_metadata not seeded"}), 500

    result = []
    for factory in meta.get("factories", []):
        code    = factory["factory_code"]
        centres = [
            {"name": c["name"], "altitude_m": c["altitude_m"], "lat": c["lat"], "lng": c["lng"]}
            for c in meta.get("collection_centres", {}).get(code, [])
        ]
        result.append({
            "factory_code": code,
            "factory_name": factory["factory_name"],
            "county":       factory["county"],
            "ktda_zone":    factory["ktda_zone"],
            "centres":      centres,
        })

    return jsonify(result)