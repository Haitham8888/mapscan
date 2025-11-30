from flask import Blueprint, request, jsonify
from utils.geojson_loader import load_geojson
from utils.match import match_name
from db.queries import fetch_population

region_bp = Blueprint("region", __name__)

@region_bp.route("/stats_region")
def stats_region():
    region_id = request.args.get("region_id")
    region_name = request.args.get("region_name")

    data = load_geojson("regions.geojson")

    for feat in data.get("features", []):
        props = feat.get("properties", {}) if isinstance(feat, dict) else feat

        if region_id and str(props.get("region_id")) == region_id:
            pop = fetch_population(region_id=region_id)
            return jsonify({"region": props, "population": pop, "feature": feat})

        if region_name and match_name(region_name, props):
            pop = fetch_population(region_id=props.get("region_id"))
            return jsonify({"region": props, "population": pop, "feature": feat})

    return jsonify({"error": "region not found"}), 404
