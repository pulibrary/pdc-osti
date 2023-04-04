import argparse
import json
import re
import ssl
from logging import Logger
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests
import requests.adapters
import urllib3
from rich.prompt import Prompt

from . import DATASPACE_URI, DSPACE_ID, PDC_URI
from .commons import get_dc_value
from .logger import pdc_log, script_log_end, script_log_init

SCRIPT_NAME = Path(__file__).stem

# NOTE: The Dataspace REST API can now support requests from handles.
#  Shifting this scrape to collection handles instead of IDs may make
#  this script clearer and easier to change if necessary.
PPPL_COLLECTIONS = {
    "NSTX": 1282,
    "NSTX-U": 1304,
    "Stellarators": 1308,
    "Plasma Science & Technology": 1422,
    "Theory and Computation": 2266,
    "ITER and Tokamaks PPPL Collaborations": 3378,
    "Theory": 3379,
    "Computational Science PPPL Collaborations": 3380,
    "Engineering Research": 3381,
    "ESH Technical Reports": 3382,
    "IT PPPL Collaborations": 3383,
    "Advanced Projects Other Projects": 3386,
    "Advanced Projects System Studies": 1309,
    "MAST-U": 3515,
}

PPPL_COMMUNITY_ID = 346

# All possible prefix: https://regex101.com/r/SxNHJg
REGEX_DOE = r"^(DE|AC|SC|FC|FG|AR|EE|EM|FE|NA|NE)"
REGEX_DOE_SUB = "^(DE)+(-?)"  # https://regex101.com/r/NsZbRJ
REGEX_FUNDING = r"\b(?:[A-Z0-9\/\-]{6,})"  # https://regex101.com/r/4fNYVm
REGEX_BARE_DOE = re.compile(
    r"(^((U.S.|U. S.) (Department of Energy))|FES)$"
)  # https://regex101.com/r/2s3dA3

REPLACE_DICT = {
    "- ": "-",  # Extra white space inside DoE grant
    "AC02 ": "AC02-",  # Missing hyphen
    "AC-02": "AC02",  # Extra hyphen
    "SC-0": "SC0",  # Extra hyphen for Office of Science grants
    "DC": "DE",  # Common typo
    "DE ": "DE",  # Extra white space
    "DOE-": "DE",  # Proper prefix
    "DOE ": "",  # Extra DOE
    "DOE": "",  # Remove DOE if still present
}


# Fix for OpenSSL issue: https://github.com/pulibrary/pdc-osti/issues/31
class CustomHttpAdapter(requests.adapters.HTTPAdapter):
    # "Transport adapter" that allows us to use custom ssl_context.

    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=self.ssl_context,
        )


class Scraper:
    """
    Pipeline to collect data from OSTI & DataSpace/PDC, comparing which datasets
    are not yet posted, and generating a form for a user to manually enter
    additional needed information

    :param data_dir: Local data folder for save files
    :param osti_scrape: JSON output file containing OSTI metadata
    :param princeton_source: Princeton data repository name
    :param entry_form_full_path: TSV file containing DataSpace/PDC
           records not in OSTI
    :param form_input_full_path: TSV file containing DataSpace/PDC
           records and DOE metadata for submission
    :param to_upload: JSON output file containing metadata for OSTI upload
    :param redirects: JSON output file containing DOI redirects
    :param log: ``Logger`` for stdout and file logging

    :ivar osti_scrape: JSON output file containing OSTI metadata
    :ivar princeton_source: Princeton data repository name
    :ivar entry_form: TSV file containing DataSpace/PDC records not in OSTI
    :ivar form_input: TSV file containing DataSpace/PDC
           records and DOE metadata for submission
    :ivar to_upload: JSON output file containing metadata for OSTI upload
    :ivar redirects: JSON output file containing DOI redirects
    :ivar princeton_scrape: JSON output file containing DataSpace/PDC metadata
    """

    def __init__(
        self,
        data_dir: Path = Path("data"),
        osti_scrape: str = "osti_scrape.json",
        princeton_source: str = "dspace",
        entry_form_full_path: str = "entry_form.tsv",
        form_input_full_path: str = "form_input.tsv",
        to_upload: str = "metadata_to_upload.json",
        redirects: str = "redirects.json",
        log: Logger = pdc_log,
    ) -> None:
        self.log = log
        self.osti_scrape = data_dir / osti_scrape
        self.princeton_source = princeton_source
        self.entry_form = Path(entry_form_full_path)
        self.form_input = Path(form_input_full_path)
        self.redirects = data_dir / redirects

        self.princeton_scrape = data_dir / f"{princeton_source}_scrape.json"
        self.to_upload = data_dir / f"{princeton_source}_{to_upload}"

        if not data_dir.exists():
            data_dir.mkdir()

    def get_existing_datasets(self) -> None:
        """
        Paginate through OSTI's Data Explorer API to find datasets that have
        been submitted
        """
        self.log.info("[bold yellow]Get existing datasets")

        MAX_PAGE_COUNT = 15
        existing_datasets = []

        for page in range(MAX_PAGE_COUNT):
            url = (
                "https://www.osti.gov/dataexplorer/api/v1/records?"
                f"site_ownership_code=PPPL&page={page}"
            )
            r = get_legacy_session().get(url)  # fix for #31
            j = json.loads(r.text)
            if len(j) != 0:
                existing_datasets.extend(j)
            else:
                self.log.info(f"Pulled {len(existing_datasets)} records from OSTI.")
                break
        else:
            msg = "Didn't reach the final OSTI page! Increase MAX_PAGE_COUNT"
            self.log.error(f"[bold red]{msg}")
            raise BaseException(msg)

        state = "Updating" if self.osti_scrape.exists() else "Writing"
        self.log.info(f"[yellow]{state}: {self.osti_scrape}")
        with open(self.osti_scrape, "w") as f:
            json.dump(existing_datasets, f, indent=4)
        self.log.info("[bold green]✔ Existing datasets obtained!")

    def get_princeton_metadata(self) -> None:
        """Collect metadata on all items from all DataSpace/PDC PPPL collections"""

        if self.princeton_source == "dspace":
            repo_name = "DataSpace"
        elif self.princeton_source == "pdc":
            repo_name = "PDC"
        else:
            raise ValueError("Incorrect repository source!!!")

        self.log.info(f"[bold yellow]Collecting {repo_name} metadata")

        all_items = []
        if self.princeton_source == "dspace":
            for c_name, c_id in PPPL_COLLECTIONS.items():
                url = f"{DATASPACE_URI}/rest/collections/{c_id}/items?expand=metadata"
                r = requests.get(url)
                j = json.loads(r.text)
                all_items.extend(j)

            # Confirm that all collections were included
            url_all = f"{DATASPACE_URI}/rest/communities/{PPPL_COMMUNITY_ID}"
            r = requests.get(url_all)
            self.log.info(f"countItems: {json.loads(r.text)['countItems']}")
            self.log.info(f"all_items: {len(all_items)}")
            assert json.loads(r.text)["countItems"] == len(all_items), (
                "The number of items in the PPPL community does not equal the "
                "number of items collected. Review the list of collections we "
                "search through (variable COLLECTION_IDS) and ensure that all "
                "PPPL collections are included. Or write a recursive function "
                "to prevent this from happening again."
            )
        elif self.princeton_source == "pdc":
            r = requests.get(PDC_URI)
            j = r.json()
            all_items.extend(j)

        self.log.info(f"Pulled {len(all_items)} records from {repo_name}.")

        state = "Updating" if self.princeton_scrape.exists() else "Writing"
        self.log.info(f"[yellow]{state}: {self.princeton_scrape}")
        with open(self.princeton_scrape, "w") as f:
            json.dump(all_items, f, indent=4)

        self.log.info(f"[bold green]✔ {repo_name} metadata collected!")

    def get_unposted_metadata(self) -> None:
        """Compare OSTI and DataSpace/PDC JSON to identify records for uploading"""

        def get_handle(doi: str, redirects_j: List[dict]) -> str:
            if doi not in redirects_j:
                r = get_legacy_session().get(doi)  # fix for #31
                assert r.status_code == 200, f"Error parsing DOI: {doi}"
                handle = r.url.split("handle/")[-1]
                redirects_j[doi] = handle
                return handle
            else:
                return redirects_j[doi]

        def princeton_metadata_handle(record: dict):
            """Retrieves ARK handle depending on Princeton source"""
            h_key = "handle" if self.princeton_source == "dspace" else "handle_ssi"
            return record[h_key]

        self.log.info("[bold yellow]Identifying new records for uploading")

        self.log.info(f"[yellow]Loading: {self.redirects}")
        with open(self.redirects) as f:
            redirects_j = json.load(f)

        self.log.info(f"[yellow]Loading: {self.princeton_scrape}")
        with open(self.princeton_scrape) as f:
            princeton_j = json.load(f)

        self.log.info(f"[yellow]Loading: {self.osti_scrape}")
        with open(self.osti_scrape) as f:
            osti_j = json.load(f)

        # Find handles in DSpace whose handles aren't linked in OSTI's DOIs
        # HACK: returning proper DOI while also updating redirects_j
        osti_handles = [get_handle(record["doi"], redirects_j) for record in osti_j]

        to_be_published = []
        for record in princeton_j:
            if princeton_metadata_handle(record) not in osti_handles:
                to_be_published.append(record)

        state = "Updating" if self.to_upload.exists() else "Writing"
        self.log.info(f"[yellow]{state}: {self.to_upload}")
        with open(self.to_upload, "w") as f:
            json.dump(to_be_published, f, indent=4)

        state = "Updating" if self.redirects.exists() else "Writing"
        self.log.info(f"[yellow]{state}: {self.redirects}")
        with open(self.redirects, "w") as f:
            json.dump(redirects_j, f, indent=4)

        # Check for records in OSTI but not DataSpace/PDC
        princeton_handles = [
            princeton_metadata_handle(record) for record in princeton_j
        ]
        errors = [
            record
            for record in osti_j
            if redirects_j[record["doi"]] not in princeton_handles
        ]
        if len(errors) > 0:
            self.log.warning(
                "[bold red]The following records were found on OSTI but not in DSpace "
                "(that shouldn't happen). If they closely resemble records we are "
                "about to upload, please remove those records from the upload process."
            )
            for error in errors:
                self.log.info(f"\t{error['title']}")

        self.log.info("[bold green]✔ New records for uploading identified!")

    def generate_contract_entry_form(self) -> None:
        """
        Create a CSV where a user can enter Sponsoring Organizations, DOE
        Contract, and Datatype, additional information required by OSTI
        """
        self.log.info("[bold yellow]Generating entry form")

        self.log.info(f"[yellow]Loading: {self.to_upload}")
        with open(self.to_upload) as f:
            to_upload_j = json.load(f)

        df = pd.DataFrame()
        df[DSPACE_ID] = [item["id"] for item in to_upload_j]
        if self.princeton_source == "dspace":
            title_key = "name"
        elif self.princeton_source == "pdc":
            title_key = "title_tesim"

        df["Issue Date"] = [
            get_dc_value(item, "dc.date.issued")[0] for item in to_upload_j
        ]
        df["Title"] = [item[title_key] for item in to_upload_j]
        df["Author"] = [
            ";".join(get_dc_value(item, "dc.contributor.author"))
            for item in to_upload_j
        ]
        df["Dataspace Link"] = [
            f"{DATASPACE_URI}/handle/{item['handle']}" for item in to_upload_j
        ]

        # Retrieve funding data
        funding_text_list = [
            get_dc_value(item, "dc.contributor.funder") for item in to_upload_j
        ]

        # Generate lists of lists per each dc.contributor.funder entry
        funding_result = [
            list(filter(None, map(get_funder, f_list))) for f_list in funding_text_list
        ]
        funding_result_simple = [  # All grants for each DSpace record
            ";".join([";".join(value) if value else "" for value in res])
            for res in funding_result
        ]
        funding_source_dict = list(map(get_doe_funding, funding_result_simple))

        df["DOE Contract"] = [
            ";".join(sorted(d.get("doe"))) for d in funding_source_dict
        ]
        df["Non-DOE Contract"] = [
            ";".join(sorted(d.get("other"))) for d in funding_source_dict
        ]

        # Sponsoring organizations is always Office of Science
        df["Sponsoring Organizations"] = "USDOE Office of Science (SC)"

        df["Datatype"] = None  # To be filled in

        df = df.sort_values("Issue Date")
        state = "Updating" if self.entry_form.exists() else "Writing"
        self.log.info(f"[yellow]{state}: {self.entry_form}")
        df.to_csv(self.entry_form, index=False, sep="\t")

        self.log.info(
            f"[purple]{df.shape[0]} unpublished records were found in the PPPL "
            f"Dataspace/PDC community that have not been registered with OSTI."
        )
        self.log.info(f"[purple]They've been saved to the form {self.entry_form}.")
        self.log.info(
            "[purple]You're now expected to manually update that form and "
            "save as a new file before running Poster.py"
        )
        for i, row in df.iterrows():
            self.log.info(f"\t{repr(row['Title'])}")
            self.log.info(f"\t\t{row['Dataspace Link']}")

        self.log.info("[bold green]✔ Entry form generated!")

    def update_form_input(self) -> None:
        """
        Update form_input.tsv by adding new records or removing DataSpace/PDC
        records that were removed/withdrawn

        In most cases, this will update form_input.tsv. This further
        supports CI
        """
        self.log.info("[bold yellow]Updating form input")

        if self.form_input.exists():
            self.log.info(f"File exists. Will update: {self.form_input}")

            entry_df = pd.read_csv(self.entry_form, index_col=DSPACE_ID, sep="\t")
            input_df = pd.read_csv(self.form_input, index_col=DSPACE_ID, sep="\t")
            self.log.info("Identifying DataSpace/PDC records to add and remove ...")
            entry_id = set(entry_df.index)
            input_id = set(input_df.index)
            drops = input_id - entry_id
            adds = list(entry_id - input_id)
            commons = entry_id & input_id
            self.log.info(f"Commons records : {len(commons):3}")
            self.log.info(f"New records     : {len(adds):3}")
            self.log.info(f"Records to drop : {len(drops):3}")
            self.log.info(f"Removing : {','.join([str(drop) for drop in drops])} ...")
            input_df.drop(drops, inplace=True)
            self.log.info(f"Appending : {','.join([str(add) for add in adds])}")
            revised_df = pd.concat([input_df, entry_df.loc[adds]], axis=0)

            # "AS" is a placeholder - not included in DataSpace metadata
            revised_df.loc[adds, "Datatype"] = "AS"

            state = "Updating" if self.form_input.exists() else "Writing"
            self.log.info(f"[yellow]{state}: {self.form_input}")
            revised_df.to_csv(self.form_input, sep="\t")
        else:
            self.log.warning(
                f"[bold red]{(msg := f'{self.form_input} does not exist!')}"
            )
            raise FileNotFoundError(msg)

        self.log.info("[bold green]✔ Form input updated!")

    def run_pipeline(self, scrape=True) -> None:
        self.log.info(f"[bold yellow]Running {SCRIPT_NAME} pipeline")
        if scrape:
            self.get_existing_datasets()
            self.get_princeton_metadata()

        self.get_unposted_metadata()
        self.generate_contract_entry_form()
        self.update_form_input()
        self.log.info(f"[bold green]✔ Pipeline run completed for {SCRIPT_NAME}!")


def get_funder(text: str) -> List[str]:
    """Aggregate funding grant numbers from text"""

    # Clean up text by fixing any whitespace to get full grant no.
    for key, value in REPLACE_DICT.items():
        text = text.replace(key, value)

    for hyphen in ["\u2010", "\u2013"]:
        text = text.replace(hyphen, "-")

    base_match = re.match(REGEX_BARE_DOE, text)
    if base_match:  # DOE/FES funded but no grant number
        return ["AC02-09CH11466"]
    else:
        matches = re.finditer(REGEX_FUNDING, text)
        return [m.group() for m in matches]


def get_doe_funding(grant_nos: str) -> Dict[str, set]:
    """Separate DOE from other funding. Prefix DE prefix"""

    grant_dict = {"doe": set(), "other": set()}

    if not grant_nos:  # Empty case
        grant_dict["doe"].update(["AC02-09CH11466"])
    else:
        grants = grant_nos.split(";")
        for grant in grants:
            if re.match(REGEX_DOE, grant):
                grant_dict["doe"].update([re.sub(REGEX_DOE_SUB, "", grant)])
            else:
                grant_dict["other"].update([grant])

    return grant_dict


# Fix for OpenSSL issue: https://github.com/pulibrary/pdc-osti/issues/31
def get_legacy_session():
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    session = requests.session()
    session.mount("https://", CustomHttpAdapter(ctx))
    return session


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Script to retrieve Princeton data repository and DOE/OSTI records"
    )
    parser.add_argument(
        "-s",
        "--source",
        required=False,
        default="",
        type=str,
        help="Source for Princeton data (dspace or pdc)",
    )
    args = parser.parse_args()

    log = script_log_init(SCRIPT_NAME)

    if not args.source:
        princeton_source = Prompt.ask(
            "Princeton data repository source?",
            choices=["dspace", "pdc"],
            default="dspace",
        )
    else:
        princeton_source = args.source
    log.info(f"Will retrieve Princeton data repository data from {princeton_source}")

    s = Scraper(log=log, princeton_source=princeton_source)
    # NOTE: It may be useful to implement a CLI command (e.g. --no-scrape) to
    #       allow for debugging the get_unposted_metadata or
    #       generate_contract_entry_form functions
    s.run_pipeline()
    script_log_end(SCRIPT_NAME, log)
