def get_ark(record: dict) -> str:
    """Retrieves ARK (e.g., 88435/dsp012j62s808w) depending on Princeton source"""

    try:
        return record["resource"]["ark"].replace("ark:/", "")
    except AttributeError:
        return ""


def get_author(creator: dict) -> dict:
    """Retrieve individual author from PDC metadata"""

    c_dict = {
        "first_name": creator.get("given_name"),
        "last_name": creator.get("family_name"),
    }

    c_id = creator["identifier"]
    if c_id:
        c_dict["orcid_id"] = c_id["value"]

    affils = creator["affiliations"]
    if affils:
        c_dict["affiliation_name"] = ";".join(item["value"] for item in affils)
    return c_dict


def get_authors(record: dict) -> list[dict]:
    """Retrieve author with ORCID and affiliation from PDC"""

    creators = record["resource"].get("creators")
    if creators:
        return [get_author(creator) for creator in creators]


def get_datacite_awards(item: dict) -> list:
    return [m["award_number"] for m in item["resource"].get("funders")]


def get_description(item: dict) -> str:
    """Retrieve description of dataset"""
    abstract = [item["resource"].get("description")]

    if len(abstract) != 0:
        return "\n\n".join(abstract)
    else:
        return ""


def get_doi(record: dict) -> str:
    """Retrieves DOI from PDC"""

    return record["resource"]["doi"].replace("https://doi.org/", "")


def get_is_referenced_by(item: dict) -> str:
    """Retrieve IsReferencedBy for dataset"""
    related_objects = item["resource"].get("related_objects")
    if related_objects:
        isreferencedby = [
            m["related_identifier"]
            for m in related_objects
            if m["relation_type"] in ["IsCitedBy", "IsReferencedBy"]
        ]
    else:
        isreferencedby = []

    return isreferencedby


def get_keywords(item: dict) -> str:
    keywords = item["resource"].get("keywords")

    return "; ".join(keywords)
