import json
import re
import ssl
from logging import Logger
from pathlib import Path

import pandas as pd
import requests
import requests.adapters
import urllib3

from . import PDC_QUERY, PDC_URI
from .commons import get_ark, get_datacite_awards, get_doi
from .logger import pdc_log, script_log_end, script_log_init

SCRIPT_NAME = Path(__file__).stem

# All possible prefix: https://regex101.com/r/SxNHJg
REGEX_DOE = r"^(DE|AC|SC|FC|FG|AR|EE|EM|FE|NA|NE)"
REGEX_DOE_SUB = "^(DE)+(-?)"  # https://regex101.com/r/NsZbRJ
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

DATAEXPLORER_HEADER = {"accept": "application/json"}


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
    Pipeline to collect data from OSTI & PDC, comparing which datasets
    are not yet posted, and generating a form for a user to manually enter
    additional needed information

    :param data_dir: Local data folder for save files
    :param osti_scrape: JSON output file containing OSTI metadata
    :param entry_form_full_path: TSV file containing PDC
           records not in OSTI
    :param form_input_full_path: TSV file containing PDC
           records and DOE metadata for submission
    :param to_upload: JSON output file containing metadata for OSTI upload
    :param redirects: JSON output file containing DOI redirects
    :param log: ``Logger`` for stdout and file logging

    :ivar osti_scrape: JSON output file containing OSTI metadata
    :ivar entry_form: TSV file containing PDC records not in OSTI
    :ivar form_input: TSV file containing PDC
           records and DOE metadata for submission
    :ivar to_upload: JSON output file containing metadata for OSTI upload
    :ivar redirects: JSON output file containing DOI redirects
    :ivar princeton_scrape: JSON output file containing PDC metadata
    """

    def __init__(
        self,
        data_dir: Path = Path("data"),
        osti_scrape: str = "osti_scrape.json",
        entry_form_full_path: str = "entry_form.tsv",
        form_input_full_path: str = "form_input.tsv",
        to_upload: str = "metadata_to_upload.json",
        redirects: str = "redirects.json",
        log: Logger = pdc_log,
    ) -> None:
        self.log = log
        self.osti_scrape = data_dir / osti_scrape
        self.entry_form = Path(f"pdc_{entry_form_full_path}")
        self.form_input = Path(f"pdc_{form_input_full_path}")
        self.redirects = data_dir / redirects

        self.princeton_scrape = data_dir / "pdc_scrape.json"
        self.to_upload = data_dir / f"pdc_{to_upload}"

        if not data_dir.exists():
            data_dir.mkdir()

    def get_existing_datasets(self) -> None:
        """
        Paginate through OSTI's Data Explorer API to find datasets that have
        been submitted
        """

        def _extract_id(obj):  # For iss#55
            """Extract OSTI_ID for sorting"""
            try:
                return int(obj["osti_id"])
            except KeyError:
                return 0

        self.log.info("[bold yellow]Get existing datasets")

        MAX_PAGE_COUNT = 15
        existing_datasets = []

        for page in range(MAX_PAGE_COUNT):
            url = (
                "https://www.osti.gov/dataexplorer/api/v1/records?"
                f"site_ownership_code=PPPL&page={page}"
            )
            r = get_legacy_session().get(
                url, headers=DATAEXPLORER_HEADER
            )  # fix for #31
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

        existing_datasets.sort(key=_extract_id)  # Sort by ID iss#55
        clean_existing_dataset = [  # Remove duplicates iss#7
            i
            for n, i in enumerate(existing_datasets)
            if i not in existing_datasets[n + 1 :]
        ]

        self.log.info(
            f"OSTI list has been truncated to {len(existing_datasets)} records."
        )

        state = "Updating" if self.osti_scrape.exists() else "Writing"
        self.log.info(f"[yellow]{state}: {self.osti_scrape}")
        with open(self.osti_scrape, "w") as f:
            json.dump(clean_existing_dataset, f, indent=4)
        self.log.info("[bold green]✔ Existing datasets obtained!")

    def get_princeton_metadata(self) -> None:
        """Collect metadata on all items from all PDC PPPL collections"""

        repo_name = "PDC"

        self.log.info(f"[bold yellow]Collecting {repo_name} metadata")

        all_items = []

        next_page = 1
        while True:
            query = PDC_QUERY | {"page": next_page}
            r = requests.get(PDC_URI, params=query)
            j = r.json()
            if not j:  # End of records
                break
            else:
                for j_item in j:
                    all_items.append(json.loads(j_item["pdc_describe_json_ss"]))
                next_page += 1

        self.log.info(f"Pulled {len(all_items)} records from {repo_name}.")

        state = "Updating" if self.princeton_scrape.exists() else "Writing"
        self.log.info(f"[yellow]{state}: {self.princeton_scrape}")
        with open(self.princeton_scrape, "w") as f:
            json.dump(all_items, f, indent=4)

        self.log.info(f"[bold green]✔ {repo_name} metadata collected!")

    def get_unposted_metadata(self) -> None:
        """Compare OSTI and PDC JSON to identify records for uploading"""

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

        # Update redirects
        for record in osti_j:
            doi = record["doi"]
            if doi not in redirects_j:
                site_url = record["site_url"]
                if "ark" in site_url:
                    redirects_j[doi] = site_url.split("ark:/")[-1]
                elif "doi" in site_url:
                    redirects_j[doi] = site_url.split("doi.org/")[-1]

        state = "Updating" if self.redirects.exists() else "Writing"
        self.log.info(f"[yellow]{state}: {self.redirects}")
        with open(self.redirects, "w") as f:
            json.dump(redirects_j, f, indent=4)

        to_be_published = []
        for record in princeton_j:
            doi_url = f"https://doi.org/{get_doi(record)}"
            if doi_url not in redirects_j:
                to_be_published.append(record)

        state = "Updating" if self.to_upload.exists() else "Writing"
        self.log.info(f"[yellow]{state}: {self.to_upload}")
        with open(self.to_upload, "w") as f:
            json.dump(to_be_published, f, indent=4)

        # Check for records in OSTI but not PDC
        princeton_handles = [get_doi(record) for record in princeton_j]
        errors = [
            record
            for record in osti_j
            if record["doi"].replace("https://doi.org/", "") not in princeton_handles
        ]
        if len(errors) > 0:
            self.log.warning(
                "[bold red]The following records were found on OSTI but not in PDC "
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
        df["DOI"] = [item["resource"].get("doi") for item in to_upload_j]
        df["ARK"] = list(map(get_ark, to_upload_j))

        df["Issue Date"] = [f"{item['date_approved']}" for item in to_upload_j]
        df["Title"] = [item["resource"]["titles"][0]["title"] for item in to_upload_j]
        df["Author"] = [
            ";".join([value["value"] for value in item["resource"]["creators"]])
            for item in to_upload_j
        ]

        funding_text_list = [get_datacite_awards(item) for item in to_upload_j]

        # Generate lists of lists per each dc.contributor.funder entry
        funding_result = [
            list(filter(None, map(get_funder, f_list))) for f_list in funding_text_list
        ]
        funding_result_simple = [
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

        df.sort_values("Issue Date", ascending=True, axis=0)
        # Fixes #56
        # df["Datatype"] = None  # To be filled in

        df = df.sort_values("Issue Date")
        state = "Updating" if self.entry_form.exists() else "Writing"
        self.log.info(f"[yellow]{state}: {self.entry_form}")
        df.to_csv(self.entry_form, index=False, sep="\t")

        self.log.info(
            f"[purple]{df.shape[0]} unpublished records were found in the PPPL "
            "PDC community that have not been registered with OSTI."
        )
        self.log.info(f"[purple]They've been saved to the form {self.entry_form}.")
        for i, row in df.iterrows():
            self.log.info(f"\t{repr(row['Title'])}")

        self.log.info("[bold green]✔ Entry form generated!")

    def run_pipeline(self, scrape=True) -> None:
        self.log.info(f"[bold yellow]Running {SCRIPT_NAME} pipeline")
        if scrape:
            self.get_existing_datasets()
            self.get_princeton_metadata()

        self.get_unposted_metadata()
        self.generate_contract_entry_form()
        self.log.info(f"[bold green]✔ Pipeline run completed for {SCRIPT_NAME}!")


def get_funder(text: str | None) -> list[str]:
    """Aggregate funding grant numbers from text"""

    # Clean up text by fixing any whitespace to get full grant no.
    if text:
        for key, value in REPLACE_DICT.items():
            text = text.replace(key, value)

        for hyphen in ["\u2010", "\u2013"]:
            text = text.replace(hyphen, "-")

        base_match = re.match(REGEX_BARE_DOE, text)
        if base_match:  # DOE/FES funded but no grant number
            return ["AC02-09CH11466"]
        else:
            if text and text != "N/A":
                return [text]
    else:
        return [""]


def get_doe_funding(grant_nos: str) -> dict[str, set]:
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
    log = script_log_init(SCRIPT_NAME)

    log.info("Will retrieve Princeton data repository data from PDC")

    s = Scraper(log=log)
    # NOTE: It may be useful to implement a CLI command (e.g. --no-scrape) to
    #       allow for debugging the get_unposted_metadata or
    #       generate_contract_entry_form functions
    s.run_pipeline()
    script_log_end(SCRIPT_NAME, log)
