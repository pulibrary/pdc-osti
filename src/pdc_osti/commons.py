from itertools import groupby


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


def get_sponsors(item: dict, log, contract_nos, nondoe_nos) -> list:
    """Retrieve funder info and convert for E-Link 2 API"""

    def _group_by_doe_nondoe(_name, _funders, _contract_nos, doe=False):
        def _remove_prefix(inp: str, doe=False):
            try:
                return inp.replace("DE-", "") if doe else inp
            except AttributeError:
                return None

        return [
            _remove_prefix(a.get("award_number"), doe)
            for a in _funders
            if a.get("funder_name") == _name
            and _remove_prefix(a.get("award_number"), doe) in _contract_nos
        ]

    # Sort the list by the 'name' field
    sorted_data = sorted(
        item.get("resource").get("funders"), key=lambda x: x["funder_name"]
    )

    funder_groups = {}
    for name, group in groupby(sorted_data, key=lambda x: x["funder_name"]):
        funder_groups[name] = list(group)

    sponsors = []
    if funders := item.get("resource").get("funders"):
        funder_names = set([a.get("funder_name") for a in funders])

        for name in funder_names:
            t_dict = {"type": "SPONSOR"}
            ror_ids = list(
                set(
                    [
                        a.get("ror")
                        for a in funders
                        if a.get("funder_name") == name and a.get("ror")
                    ]
                )
            )
            if len(ror_ids) > 1:
                log.warning(f"MULTIPLE RORs for {name}: {''.join(ror_ids)}")
                break
            if len(ror_ids) == 1:
                t_dict["ror"] = ror_ids[0]
            else:
                t_dict["name"] = name

            doe_award_numbers = _group_by_doe_nondoe(
                name, funders, contract_nos, doe=True
            )
            nondoe_award_numbers = _group_by_doe_nondoe(name, funders, nondoe_nos)
            if doe_award_numbers:
                t_dict["identifiers"] = [
                    {"type": "CN_DOE", "value": award_number}
                    for award_number in doe_award_numbers
                ]
            if nondoe_award_numbers:
                t_dict["identifiers"] = [
                    {"type": "CN_NONDOE", "value": award_number}
                    for award_number in nondoe_award_numbers
                ]

            sponsors.append(t_dict)
    return sponsors


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


def get_keywords(item: dict) -> list[str]:
    keywords = item["resource"].get("keywords")
    # Handles ending of list with a comma -> null
    return [key for key in keywords if key]
