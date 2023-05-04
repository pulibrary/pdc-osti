def get_dc_value(item: dict, key: str) -> list:
    """Retrieve DublinCore (DC) metadata"""
    return [m["value"] for m in item["metadata"] if m["key"] == key]


def get_datacite_awards(item: dict) -> list:
    return [m["award_number"] for m in item["resource"].get("funders")]
