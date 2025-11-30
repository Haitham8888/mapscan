import json
import os

# Base folder for geojson files (relative to project root)
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'geojson'))


def load_geojson(file_name):
    path = os.path.join(BASE_PATH, file_name)
    if not os.path.isfile(path):
        return {"features": []}

    with open(path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except Exception:
            return {"features": []}

    if isinstance(data, dict) and "features" in data and isinstance(data["features"], list):
        return data
    # Some files contain a list of features
    if isinstance(data, list):
        return {"features": data}
    return {"features": []}
