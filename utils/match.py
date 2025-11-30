# utils/match.py
import json
from typing import Optional, Dict


def load_id_name_map(json_path: str, id_field: str, name_field: str) -> Dict[str, int]:
    """
    يقرأ ملف JSON عادي (مو GeoJSON) فيه قائمة مناطق/مدن/أحياء
    ويطلع منه mapping من الاسم إلى ID.
    """
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

    mapping = {}
    for item in data:
        _id = item.get(id_field)
        name = item.get(name_field)
        if _id is not None and isinstance(name, str) and name.strip():
            mapping[name.strip()] = _id

    return mapping


def get_id_by_name(name: str, mapping: Dict[str, int]) -> Optional[int]:
    if not name:
        return None
    return mapping.get(name.strip())
