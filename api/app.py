"""
ChaiMetrics Flask API — app factory.

Start locally (WSL host or inside ml_engine container):
    python api/app.py

Production (gunicorn):
    gunicorn "api.app:create_app()" --bind 0.0.0.0:5000 --workers 2
"""

import os
from flask import Flask, jsonify
from flask_jwt_extended import JWTManager

from api.blueprints.farms    import farms_bp
from api.blueprints.insights import insights_bp
from api.blueprints.pricing  import pricing_bp
from api.blueprints.auth     import auth_bp


def create_app() -> Flask:
    app = Flask(__name__)

    #  Config 
    app.config["JWT_SECRET_KEY"]          = os.getenv("JWT_SECRET_KEY", "dev-secret-change-in-prod")
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = False   # long-lived for demo; tighten in prod
    app.config["MONGODB_URI"]             = os.getenv("MONGODB_URI", "mongodb://localhost:27017/chaimterics")
    app.config["MODELS_DIR"]              = os.getenv("MODELS_DIR",  "/app/models")
    app.config["OLLAMA_HOST"]             = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    app.config["OLLAMA_MODEL"]            = os.getenv("OLLAMA_MODEL", "qwen")

    #  Extensions 
    JWTManager(app)

    #  Blueprints 
    app.register_blueprint(auth_bp,     url_prefix="/auth")
    app.register_blueprint(farms_bp,    url_prefix="/farms")
    app.register_blueprint(insights_bp, url_prefix="/farms")
    app.register_blueprint(pricing_bp,  url_prefix="/pricing")

    #  Error handlers 
    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"error": "not found"}), 404

    @app.errorhandler(500)
    def server_error(e):
        return jsonify({"error": "internal server error", "detail": str(e)}), 500

    @app.route("/health")
    def health():
        return jsonify({"status": "ok", "service": "ChaiMetrics API v1"})

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5000, debug=True)