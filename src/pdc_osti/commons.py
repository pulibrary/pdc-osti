def get_ark(record: dict, princeton_source: str) -> str:
    """Retrieves ARK (e.g., 88435/dsp012j62s808w) depending on Princeton source"""
    if princeton_source == "dspace":
        return record["handle"]
    elif princeton_source == "pdc":
        return record["resource"]["ark"].replace("ark:/", "")
    else:
        raise NotImplementedError


def get_dc_value(item: dict, key: str) -> list:
    """Retrieve DublinCore (DC) metadata"""
    return [m["value"] for m in item["metadata"] if m["key"] == key]


def get_datacite_awards(item: dict) -> list:
    return [m["award_number"] for m in item["resource"].get("funders")]
