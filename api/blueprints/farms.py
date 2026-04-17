"""
api/blueprints/farms.py

GET  /farms                         List farms (filterable by factory, centre, search)
GET  /farms/<member_no>             Single farm detail + season history
POST /farms/<member_no>/daily       Append a daily weighment record
"""

from datetime import datetime
from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity

from utils import get_db, farm_summary

farms_bp = Blueprint("farms", __name__)


@farms_bp.route("", methods=["GET"])
@jwt_required()
def list_farms():
    """
    GET /farms
    Query params:
        factory     WRU-01 | RKR-01
        centre      collection centre name
        search      partial match on name or member_no
        page        default 1
        per_page    default 20, max 100
    """
    current_user = get_jwt_identity()

    db = get_db()

    factory = request.args.get("factory")
    centre  = request.args.get("centre")
    search  = request.args.get("search", "").strip()
    page    = max(1, int(request.args.get("page", 1)))
    per_page = min(100, max(1, int(request.args.get("per_page", 20))))

    query = {}
    if factory:
        query["factory_code"] = factory
    if centre:
        query["collection_centre"] = centre
    if search:
        query["$or"] = [
            {"ktda_member_no": {"$regex": search, "$options": "i"}},
            {"name":           {"$regex": search, "$options": "i"}},
            {"owner_name":     {"$regex": search, "$options": "i"}},
        ]

    total = db.farms.count_documents(query)
    farms = list(
        db.farms.find(query, {"_id": 0, "historical_seasons": 0})
                .skip((page - 1) * per_page)
                .limit(per_page)
                .sort("ktda_member_no", 1)
    )

    return jsonify({
        "total":    total,
        "page":     page,
        "per_page": per_page,
        "pages":    -(-total // per_page),  # ceiling division
        "farms":    [farm_summary(f) for f in farms],
    })


@farms_bp.route("/<member_no>", methods=["GET"])
@jwt_required()
def get_farm(member_no: str):
    """
    GET /farms/<member_no>
    Returns full farm document including season history summary.
    Excludes raw monthly arrays to keep payload manageable.
    """
    current_user = get_jwt_identity()
    db   = get_db()
    farm = db.farms.find_one({"ktda_member_no": member_no}, {"_id": 0})
    if not farm:
        return jsonify({"error": f"Farm {member_no} not found"}), 404

    # Build season summary (totals only — raw monthly arrays omitted)
    season_summaries = []
    for s in sorted(farm.get("historical_seasons", []), key=lambda x: x["season_year"]):
        kg      = [v for v in s.get("monthly_kg", []) if v is not None]
        earnings = [v for v in s.get("monthly_earn", []) if v is not None]
        season_summaries.append({
            "season_year":    s["season_year"],
            "total_kg":       round(sum(kg), 1),
            "total_earnings": round(sum(earnings), 2),
            "yearly_bonus":   s.get("yearly_bonus", 0),
            "monthly_kg":     s.get("monthly_kg", []),       # kept for charts
            "monthly_earn":   s.get("monthly_earn", []),
            "season_rainfall_mm": s.get("season_rainfall_mm", []),
        })

    response = {**farm_summary(farm), "seasons": season_summaries}

    # Append any current-season daily records if they exist
    if "current_season_daily" in farm:
        response["current_season_daily"] = farm["current_season_daily"]

    return jsonify(response)


@farms_bp.route("/<member_no>/daily", methods=["POST"])
@jwt_required()
def post_daily(member_no: str):
    """
    POST /farms/<member_no>/daily
    Body: {"date": "2024-08-15", "kg": 12.5, "collection_centre": "Marima"}

    Appends a daily weighment record to current_season_daily.
    Invalidates the model_outputs cache for this farm so the next
    /insights call recomputes fresh predictions.
    """
    current_user = get_jwt_identity()
    db   = get_db()
    farm = db.farms.find_one({"ktda_member_no": member_no}, {"_id": 1})
    if not farm:
        return jsonify({"error": f"Farm {member_no} not found"}), 404

    body = request.get_json(silent=True) or {}
    date_str = body.get("date", "").strip()
    kg       = body.get("kg")
    centre   = body.get("collection_centre", "").strip()

    # Validate
    errors = []
    if not date_str:
        errors.append("date is required (YYYY-MM-DD)")
    else:
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            errors.append("date must be YYYY-MM-DD")

    if kg is None:
        errors.append("kg is required")
    elif not isinstance(kg, (int, float)) or kg < 0:
        errors.append("kg must be a non-negative number")

    if errors:
        return jsonify({"error": "validation failed", "details": errors}), 400

    record = {
        "date":               date_str,
        "kg":                 float(kg),
        "collection_centre":  centre or None,
        "recorded_at":        datetime.utcnow().isoformat(),
    }

    # Append to current_season_daily array
    db.farms.update_one(
        {"ktda_member_no": member_no},
        {"$push": {"current_season_daily": record}},
    )

    # Invalidate model_outputs cache so next /insights recomputes
    db.model_outputs.delete_one({"ktda_member_no": member_no})

    return jsonify({"status": "recorded", "record": record}), 201

    @farms_bp.route("/all", methods=["GET"])
@jwt_required()
def list_all_farms():
    """
    GET /farms/all
    Admin only — returns all farms for the farm selector dropdown.
    """
    from flask_jwt_extended import get_jwt
    claims = get_jwt()
    if claims.get("role") != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    db = get_db()
    farms = list(
        db.farms.find({}, {
            "_id": 0,
            "ktda_member_no": 1,
            "name": 1,
            "owner_name": 1,
            "factory_code": 1,
            "collection_centre": 1,
        }).sort("ktda_member_no", 1)
    )
    return jsonify({"farms": farms})