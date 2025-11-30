# routes/city.py
from flask import Blueprint, current_app, jsonify
from utils.geojson_loader import load_names_from_geojson

city_bp = Blueprint("city", __name__)


@city_bp.get("/cities")
def get_cities():
    cfg = current_app.config
    names = load_names_from_geojson(
        cfg["CITIES_GEOJSON_PATH"],
        cfg["CITIES_NAME_FIELD"]
    )
    return jsonify(names)
