__version__ = "0.1.0"

LOG_NAME = "pdc-osti"

DATASPACE_URI = "https://dataspace.princeton.edu"
DSPACE_ID = "DSpace ID"

PDC_URI = "https://datacommons.princeton.edu/discovery/catalog.json"
PDC_QUERY = {
    "f[community_root_name_ssi][]": "Princeton+Plasma+Physics+Laboratory",
    "search_field": "all_fields",
    "fl": "*",
    "q": "",
    "format": "json",
    "per_page": 100,
}
