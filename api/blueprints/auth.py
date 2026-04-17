"""
api/blueprints/auth.py

POST /auth/login
    Body: {"ktda_member_no": "KTD-13033", "password": "..."}
    Returns: {"access_token": "..."}

For the Phase 1 demo, password is the member number itself (no user table yet).
Replace with a proper credential store before any real farmer data goes live.
"""

from flask import Blueprint, request, jsonify, current_app
from flask_jwt_extended import create_access_token,jwt_required, get_jwt_identity

from utils import get_db

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/login", methods=["POST"], strict_slashes=False)
def login():
    body = request.get_json(silent=True) or {}
    member_no = body.get("ktda_member_no", "").strip()
    password  = body.get("password", "").strip()

    if not member_no:
        return jsonify({"error": "ktda_member_no is required"}), 400

    db   = get_db()
    #check admins collections first
    admin = db.admins.find_one({"username": member_no}, {"_id": 0, "username": 1, "role": 1, "password": 1, "name": 1})
    if admin:
        if password != admin["password"]:
            return jsonify({"error": "invalid credentials"}), 401

        token =create_access_token(identity=admin["username"])
        return jsonify({
            "access_token": token,
            "role": "admin",
            "name": admin.get("name", "admin"),
            })
    
    #fall to farmers check
    
    farm = db.farms.find_one({"ktda_member_no": member_no}, {"_id": 0, "ktda_member_no": 1,
                                                               "name": 1, "factory_code": 1})
    if not farm:
        return jsonify({"error": "member not found"}), 404

    # Phase 1 demo auth: password == member_no
    # Replace with hashed credential check before production
    if password != member_no:
        return jsonify({"error": "invalid credentials"}), 401

    token = create_access_token(identity=farm["ktda_member_no"])

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

@auth_bp.route('/farms', methods=['GET'])
@jwt_required
def get_all_farms():
    if current_user['role'] != 'admin':
        return jsonify({"error": "Unauthorized"}), 403
    farms = list(db.farms.find({}, {"_id": 0, "ktda_member_no": 1, "name": 1, "owner_name": 1}))
    return jsonify(farms)