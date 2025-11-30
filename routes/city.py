from flask import Blueprint, request, jsonify
from utils.geojson_loader import load_geojson
from utils.match import match_name
from db.queries import fetch_population

city_bp = Blueprint("city", __name__)

@city_bp.route("/stats_city")
def stats_city():
    city_id = request.args.get("city_id")
    city_name = request.args.get("city_name")

    data = load_geojson("cities.geojson")

    for feat in data.get("features", []):
        props = feat.get("properties", {}) if isinstance(feat, dict) else feat

        if city_id and str(props.get("city_id")) == city_id:
            pop = fetch_population(city_id=city_id)
            return jsonify({"city": props, "population": pop, "feature": feat})

        if city_name and match_name(city_name, props):
            pop = fetch_population(city_id=props.get("city_id"))
            return jsonify({"city": props, "population": pop, "feature": feat})

    return jsonify({"error": "city not found"}), 404
