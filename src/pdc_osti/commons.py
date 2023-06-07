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


def get_description(item: dict, princeton_source: str) -> str:
    """Retrieve description of dataset"""
    if princeton_source == "dspace":
        abstract = get_dc_value(item, "dc.description.abstract")
    elif princeton_source == "pdc":
        abstract = [item["resource"].get("description")]
    else:
        raise NotImplementedError

    if len(abstract) != 0:
        return "\n\n".join(abstract)
    else:
        return ""


def get_is_referenced_by(item: dict, princeton_source: str) -> str:
    """Retrieve IsReferencedBy for dataset"""
    if princeton_source == "dspace":
        isreferencedby = get_dc_value(item, "dc.relation.isreferencedby")
    elif princeton_source == "pdc":
        related_objects = item["resource"].get("related_objects")
        if related_objects:
            isreferencedby = [
                m["related_identifier"]
                for m in related_objects
                if m["relation_type"] == "IsCitedBy"
            ]
        else:
            isreferencedby = []
    else:
        raise NotImplementedError

    return isreferencedby


def get_keywords(item: dict, princeton_source: str) -> str:
    if princeton_source == "dspace":
        keywords = get_dc_value(item, "dc.subject")
    elif princeton_source == "pdc":
        keywords = item["resource"].get("keywords")
    else:
        raise NotImplementedError

    if len(keywords) != 0:
        return "; ".join(keywords)
    else:
        return ""
