# routes/district.py
from flask import Blueprint, current_app, jsonify
from utils.geojson_loader import load_names_from_geojson

district_bp = Blueprint("district", __name__)


@district_bp.get("/districts")
def get_districts():
    cfg = current_app.config
    names = load_names_from_geojson(
        cfg["DISTRICTS_GEOJSON_PATH"],
        cfg["DISTRICTS_NAME_FIELD"]
    )
    return jsonify(names)
