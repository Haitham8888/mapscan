def match_name(value, props, fields=("name", "NAME", "name_en", "name_ar")):
    needle = str(value).strip().lower()

    for f in fields:
        v = props.get(f)
        if v and str(v).strip().lower() == needle:
            return True

    # Soft match
    for f in fields:
        v = props.get(f)
        if v and str(v).strip().lower().replace("-", " ") == needle.replace("-", " "):
            return True

    return False
