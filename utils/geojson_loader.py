# utils/geojson_loader.py
import json
from typing import List


def load_names_from_geojson(path: str, name_field: str) -> List[str]:
    """
    قراءة أسماء (مناطق/مدن/أحياء) من ملف GeoJSON وإرجاع قائمة أسماء بدون تكرار.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        return []

    features = data.get("features", [])
    names = set()

    for feat in features:
        props = feat.get("properties", {})
        name = props.get(name_field)
        if isinstance(name, str) and name.strip():
            names.add(name.strip())

    # نرجّعها مرتبة
    return sorted(names)
