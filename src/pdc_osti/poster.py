import argparse
import datetime
import json
from logging import Logger
from pathlib import Path

import pandas as pd
from rich.prompt import Confirm

from .commons import (
    get_authors,
    get_description,
    get_doi,
    get_is_referenced_by,
    get_keywords,
)
from .config import settings
from .logger import pdc_log, script_log_end, script_log_init

SCRIPT_NAME = Path(__file__).stem

ACCEPTED_DATATYPE = ["AS", "GD", "IM", "ND", "IP", "FP", "SM", "MM", "I"]


class Poster:
    """
    Use the form input and PDC metadata to generate the JSON necessary for
    OSTI ingestion. Then post to OSTI using their API
    """

    def __init__(
        self,
        mode: str,
        data_dir: Path = Path("data"),
        to_upload: str = "metadata_to_upload.json",
        form_input_full_path: str = "form_input.tsv",
        osti_upload: str = "osti.json",
        response_dir: Path = Path("responses"),
        log: Logger = pdc_log,
    ) -> None:
        self.log = log
        self.mode = mode

        # Prepare all paths
        self.form_input = f"pdc_{form_input_full_path}"
        self.data_dir = data_dir
        self.to_upload = data_dir / f"pdc_{to_upload}"
        self.osti_upload = data_dir / f"pdc_{osti_upload}"

        timestamp = str(datetime.datetime.now()).replace(":", "")
        self.response_output = response_dir / f"{mode}_osti_response_{timestamp}.json"
        assert data_dir.exists()
        assert response_dir.exists()

        # Ensure minimum (test/prod) environment variables are prepared
        if mode in ["test", "prod"]:
            environment_vars = [f"ELINK2_TOKEN_{mode}"]
        if mode == "dry-run":
            environment_vars = ["ELINK2_TOKEN_TEST", "ELINK2_TOKEN_PROD"]

        settings_dict = settings.dict()
        assert all([var in settings_dict for var in environment_vars]), (
            f"All {mode} environment variables need to be set. "
            f"See the README for more information."
        )

    def generate_upload_json(self) -> None:
        """
        Validate the form input provided by the user and combine new data with
        PDC data to generate JSON that is prepared for OSTI ingestion
        """

        self.log.info("[bold yellow]Generating upload data")

        self.log.info(f"[yellow]Loading: {self.to_upload}")
        with open(self.to_upload) as f:
            to_upload_j = json.load(f)

        self.log.info(f"[yellow]Loading: {self.form_input}")
        df = pd.read_csv(
            self.form_input, index_col="ARK", sep="\t", keep_default_na=False
        )

        # Validate Input CSV
        def no_empty_cells(series) -> bool:
            return series.shape[0] == series.dropna().shape[0]

        expected_columns = [
            "Sponsoring Organizations",
            "DOE Contract",
            "Datatype",
        ]
        assert all(
            [col in df.columns for col in expected_columns]
        ), f"You're missing one of these columns {expected_columns}"
        for column in expected_columns:
            assert no_empty_cells(
                df[column]
            ), f"Empty values in required {column} column"

        assert all([dt in ACCEPTED_DATATYPE for dt in df["Datatype"]]), (
            "The Datatype column contains improper datatype values. "
            f"The accepted datatype values are: {ACCEPTED_DATATYPE}"
        )

        # Generate final JSON to post to OSTI
        osti_format = []
        for ark, row in df.iterrows():
            princeton_data = [
                item for item in to_upload_j if get_doi(item) == row["DOI"]
            ]
            assert len(princeton_data) == 1, princeton_data
            princeton_data = princeton_data[0]

            # Collect all required information
            # site_url and accession_num are initial settings
            item_dict = {
                "title": row["Title"],
                "dataset_type": row["Datatype"],
                "site_url": f"https://arks.princeton.edu/ark:/{ark}",
                "contract_nos": row["DOE Contract"],
                "sponsor_org": row["Sponsoring Organizations"],
                "research_org": "PPPL",
                "accession_num": ark,
                "publication_date": row["Issue Date"],
                "othnondoe_contract_nos": row["Non-DOE Contract"],
                "abstract": get_description(princeton_data),
                "keywords": get_keywords(princeton_data),
            }

            # Add existing DOI if it exists
            doi = row["DOI"]
            if doi:
                if not doi.startswith("10.11578"):
                    item_dict["doi"] = doi
                    # Uses DOI moving forward #50
                    item_dict["accession_num"] = doi
                    item_dict["site_url"] = f"https://doi.org/{doi}"
                else:
                    self.log.debug(f"OSTI DOI minted: {doi}")
            else:
                self.log.warning("[bold red]No DOI!!!")

            authors = get_authors(princeton_data)
            if authors:
                item_dict["authors"] = authors
            else:
                item_dict["creators"] = row["Author"]

            # Collect optional required information
            is_referenced_by = get_is_referenced_by(princeton_data)
            if len(is_referenced_by) != 0:
                item_dict["related_identifiers"] = []
                for irb in is_referenced_by:
                    item_dict["related_identifiers"].append(
                        {
                            "related_identifier": irb,
                            "relation_type": "IsReferencedBy",
                            "related_identifier_type": "DOI",
                        }
                    )
            osti_format.append(item_dict)

        state = "Updating" if self.osti_upload.exists() else "Writing"
        self.log.info(f"[yellow]{state}: {self.osti_upload}")
        with open(self.osti_upload, "w") as f:
            json.dump(osti_format, f, indent=4)

        self.log.info("[bold green]✔ Upload data generated!")

    def _fake_post(self, records: dict) -> dict:
        """A fake JSON response that mirrors OSTI's"""
        self.log.info("[bold yellow]Fake posting")

        return {
            "record": [
                {
                    "osti_id": "1488485",
                    "accession_num": record["accession_num"],
                    "product_nos": "None",
                    "title": record["title"],
                    "contract_nos": record["contract_nos"],
                    "other_identifying_nos": None,
                    "othnondoe_contract_nos": record["othnondoe_contract_nos"],
                    "doi": (
                        record.get("doi") if record.get("doi") else "10.11578/1488485"
                    ),
                    "doi_status": "PENDING",
                    "status": "SUCCESS",
                    "status_message": None,
                    "@status": "UPDATED",
                }
                for record in records
            ]
        }

    def post_to_osti(self) -> None:
        """
        Post the collected metadata to OSTI's test or prod server. If in
        dry-run mode, call our _fake_post method
        """

        def _log_status(record):
            if record["status"] == "SUCCESS":
                self.log.info(f"[green]\t✔ {record['title']}")
            else:
                self.log.info(f"[red]\t✗ {record['title']}")

        self.log.info("[bold yellow]Posting to OSTI")

        self.log.info(f"[yellow]Loading: {self.osti_upload}")
        with open(self.osti_upload) as f:
            osti_j = json.load(f)

        self.log.info("[bold yellow]Posting data")
        if self.mode == "dry-run":
            response_data = self._fake_post(osti_j)
        else:
            pass
            # response_data = ostiapi.post(osti_j, self.username, self.password)

        self.log.info(f"[yellow]Writing: {self.response_output}")
        with open(self.response_output, "w") as f:
            json.dump(response_data, f, indent=4)

        # output results to the shell:
        if isinstance(response_data["record"], list):
            for item in response_data["record"]:
                _log_status(item)
        elif isinstance(response_data["record"], dict):
            _log_status(response_data["record"])

        if self.mode != "dry-run":
            status = []
            if isinstance(response_data["record"], list):
                status.extend(
                    [item["status"] == "SUCCESS" for item in response_data["record"]]
                )
            elif isinstance(response_data["record"], dict):
                status.extend([response_data["record"]["status"] == "SUCCESS"])

            if all(status):
                self.log.info("Congrats 🚀 OSTI says that all records were uploaded!")
            else:
                self.log.info(
                    "Some of OSTI's responses do not have 'SUCCESS' as their "
                    f"status. Look at the file {self.response_output} to "
                    "see which records were not successfully uploaded."
                )

        self.log.info("[bold green]✔ Posted to OSTI!")

    def run_pipeline(self) -> None:
        self.log.info(f"[bold yellow]Running {SCRIPT_NAME} pipeline")
        self.generate_upload_json()
        self.post_to_osti()
        self.log.info(f"[bold green]✔ Pipeline run completed for {SCRIPT_NAME}!")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Script to post new Princeton datasets to DOE/OSTI"
    )
    parser.add_argument(
        "-m",
        "--mode",
        default="dry-run",
        type=str,
        help="Mode of KPI operation (dry-run, test, or execute)",
    )
    args = parser.parse_args()

    log = script_log_init(SCRIPT_NAME)

    log.info("Will use data from PDC")

    mode = args.mode
    p = Poster(mode)
    if mode == "dry-run":
        user_response = True
    if mode in ["test", "prod"]:
        log.warning("[bold red]" f"Running in {mode} mode...!")
        user_response = Confirm.ask("Are you sure you wish you proceed?")
    log.info(f"{user_response=}")
    if user_response:
        p.run_pipeline()
    else:
        log.info("[bold red]Exiting!!! You must respond with a Y/y")

    script_log_end(SCRIPT_NAME, log)
