def get_dc_value(item: dict, key: str) -> list:
    """Retrieve DublinCore (DC) metadata"""
    return [m["value"] for m in item["metadata"] if m["key"] == key]
