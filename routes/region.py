# routes/region.py
from flask import Blueprint, current_app, jsonify, request
from utils.geojson_loader import load_names_from_geojson

region_bp = Blueprint("region", __name__)


@region_bp.get("/regions")
def get_regions():
    cfg = current_app.config
    names = load_names_from_geojson(
        cfg["REGIONS_GEOJSON_PATH"],
        cfg["REGIONS_NAME_FIELD"]
    )
    return jsonify(names)
