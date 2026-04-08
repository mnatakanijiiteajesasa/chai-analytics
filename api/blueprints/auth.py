"""
api/blueprints/auth.py

POST /auth/login
    Body: {"ktda_member_no": "KTD-13033", "password": "..."}
    Returns: {"access_token": "..."}

For the Phase 1 demo, password is the member number itself (no user table yet).
Replace with a proper credential store before any real farmer data goes live.
"""

from flask import Blueprint, request, jsonify, current_app
from flask_jwt_extended import create_access_token

from utils import get_db

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/login", methods=["POST"])
def login():
    body = request.get_json(silent=True) or {}
    member_no = body.get("ktda_member_no", "").strip()
    password  = body.get("password", "").strip()

    if not member_no:
        return jsonify({"error": "ktda_member_no is required"}), 400

    db   = get_db()
    farm = db.farms.find_one({"ktda_member_no": member_no}, {"_id": 0, "ktda_member_no": 1,
                                                               "name": 1, "factory_code": 1})
    if not farm:
        return jsonify({"error": "member not found"}), 404

    # Phase 1 demo auth: password == member_no
    # Replace with hashed credential check before production
    if password != member_no:
        return jsonify({"error": "invalid credentials"}), 401

    token = create_access_token(identity={
        "ktda_member_no": farm["ktda_member_no"],
        "factory_code":   farm["factory_code"],
        "name":           farm.get("name", ""),
    })

    return jsonify({
        "access_token":   token,
        "ktda_member_no": farm["ktda_member_no"],
        "name":           farm.get("name", ""),
        "factory_code":   farm["factory_code"],
    })


@auth_bp.route("/me", methods=["GET"])
def me():
    """Quick check — returns 401 if token missing, identity if valid."""
    from flask_jwt_extended import verify_jwt_in_request, get_jwt_identity
    try:
        verify_jwt_in_request()
        return jsonify(get_jwt_identity())
    except Exception as e:
        return jsonify({"error": str(e)}), 401