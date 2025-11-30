from flask import Blueprint, request, jsonify
from utils.geojson_loader import load_geojson
from utils.match import match_name
from db.queries import fetch_population

district_bp = Blueprint("district", __name__)

@district_bp.route("/stats_district")
def stats_district():
    district_id = request.args.get("district_id")
    district_name = request.args.get("district_name")

    data = load_geojson("districts.geojson")

    for feat in data.get("features", []):
        props = feat.get("properties", {}) if isinstance(feat, dict) else feat

        if district_id and str(props.get("district_id")) == district_id:
            pop = fetch_population(district_id=district_id)
            return jsonify({"district": props, "population": pop, "feature": feat})

        if district_name and match_name(district_name, props):
            pop = fetch_population(district_id=props.get("district_id"))
            return jsonify({"district": props, "population": pop, "feature": feat})

    return jsonify({"error": "district not found"}), 404
