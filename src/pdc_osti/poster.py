import datetime
import json
import sys
from logging import Logger
from pathlib import Path

import ostiapi
import pandas as pd
from rich.prompt import Confirm

from . import DATASPACE_URI, DSPACE_ID
from .commons import get_dc_value
from .config import settings
from .logger import pdc_log, script_log_end, script_log_init

SCRIPT_NAME = Path(__file__).stem

ACCEPTED_DATATYPE = ["AS", "GD", "IM", "ND", "IP", "FP", "SM", "MM", "I"]


class Poster:
    """
    Use the form input and DSpace metadata to generate the JSON necessary for
    OSTI ingestion. Then post to OSTI using their API
    """

    def __init__(
        self,
        mode: str,
        data_dir: Path = Path("data"),
        to_upload: str = "dspace_metadata_to_upload.json",
        form_input_full_path: str = "form_input.tsv",
        osti_upload: str = "osti.json",
        response_dir: Path = Path("responses"),
        log: Logger = pdc_log,
    ) -> None:
        self.mode = mode
        self.log = log

        # Prepare all paths
        self.form_input = form_input_full_path
        self.data_dir = data_dir
        self.to_upload = data_dir / to_upload
        self.osti_upload = data_dir / osti_upload

        timestamp = str(datetime.datetime.now()).replace(":", "")
        self.response_output = response_dir / f"{mode}_osti_response_{timestamp}.json"
        assert data_dir.exists()
        assert response_dir.exists()

        # Ensure minimum (test/prod) environment variables are prepared
        if mode in ["test", "prod"]:
            environment_vars = [
                f"{v}_{mode.upper()}" for v in ["OSTI_USERNAME", "OSTI_PASSWORD"]
            ]
        if mode == "dry-run":
            environment_vars = [
                "OSTI_USERNAME_TEST",
                "OSTI_PASSWORD_TEST",
                "OSTI_USERNAME_PROD",
                "OSTI_PASSWORD_PROD",
            ]

        settings_dict = settings.dict()
        assert all([var in settings_dict for var in environment_vars]), (
            f"All {mode} environment variables need to be set. "
            f"See the README for more information."
        )

        # Assign username and password depending on where data is being posted
        if mode in ["test", "prod"]:
            self.username = settings_dict.get(f"OSTI_USERNAME_{mode.upper()}")
            self.password = settings_dict.get(f"OSTI_PASSWORD_{mode.upper()}")
        else:
            self.username, self.password = None, None

    def generate_upload_json(self) -> None:
        """
        Validate the form input provided by the user and combine new data with
        DSpace data to generate JSON that is prepared for OSTI ingestion
        """
        self.log.info("[bold yellow]Generating upload data")

        self.log.info(f"[yellow]Loading: {self.to_upload}")
        with open(self.to_upload) as f:
            to_upload_j = json.load(f)

        self.log.info(f"[yellow]Loading: {self.form_input}")
        df = pd.read_csv(self.form_input, sep="\t", keep_default_na=False)
        df = df.set_index(DSPACE_ID)

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
        for dspace_id, row in df.iterrows():
            dspace_data = [item for item in to_upload_j if item["id"] == dspace_id]
            assert len(dspace_data) == 1, dspace_data
            dspace_data = dspace_data[0]

            # get publication date
            date_info = get_dc_value(dspace_data, "dc.date.available")
            assert len(date_info) == 1
            date_info = date_info[0]
            pub_dt = datetime.datetime.strptime(date_info, "%Y-%m-%dT%H:%M:%S%z")
            pub_date = pub_dt.strftime("%m/%d/%Y")

            # Collect all required information
            item_dict = {
                "title": dspace_data["name"],
                "creators": ";".join(
                    get_dc_value(dspace_data, "dc.contributor.author")
                ),
                "dataset_type": row["Datatype"],
                "site_url": f"{DATASPACE_URI}/handle/{dspace_data['handle']}",
                "contract_nos": row["DOE Contract"],
                "sponsor_org": row["Sponsoring Organizations"],
                "research_org": "PPPL",
                "accession_num": dspace_data["handle"],
                "publication_date": pub_date,
                "othnondoe_contract_nos": row["Non-DOE Contract"],
            }

            # Collect optional required information
            abstract = get_dc_value(dspace_data, "dc.description.abstract")
            if len(abstract) != 0:
                item_dict["description"] = "\n\n".join(abstract)

            keywords = get_dc_value(dspace_data, "dc.subject")
            if len(keywords) != 0:
                item_dict["keywords"] = "; ".join(keywords)

            is_referenced_by = get_dc_value(dspace_data, "dc.relation.isreferencedby")
            if len(is_referenced_by) != 0:
                item_dict["related_identifiers"] = []
                for irb in is_referenced_by:
                    item_dict["related_identifiers"].append(
                        {
                            "related_identifier": irb.split("doi.org/")[1],
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
        try:
            ostiapi.datatoxml(records)  # Check that JSON can be parsed into XML
        except AttributeError:
            raise AttributeError(
                "Failure to load data into XML!\n"
                "Check your dicttoxml version (requires > 1.7.4)"
            )
        else:
            self.log.info("[bold green]Data loaded into XML!")

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
                    "doi": "10.11578/1488485",
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
        self.log.info("[bold yellow]Posting to OSTI")
        if self.mode == "test":
            ostiapi.testmode()

        self.log.info(f"[yellow]Loading: {self.osti_upload}")
        with open(self.osti_upload) as f:
            osti_j = json.load(f)

        self.log.info("[bold yellow]Posting data")
        if self.mode == "dry-run":
            response_data = self._fake_post(osti_j)
        else:
            response_data = ostiapi.post(osti_j, self.username, self.password)

        self.log.info(f"[yellow]Writing: {self.response_output}")
        with open(self.response_output, "w") as f:
            json.dump(response_data, f, indent=4)

        # output results to the shell:
        for item in response_data["record"]:
            if item["status"] == "SUCCESS":
                self.log.info(f"[green]\t✔ {item['title']}")
            else:
                self.log.info(f"[red]\t✗ {item['title']}")

        if self.mode != "dry-run":
            status = [item["status"] == "SUCCESS" for item in response_data["record"]]
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
    log = script_log_init(SCRIPT_NAME)
    args = sys.argv

    help_s = """
    Choose one of the following options:
    --dry-run: Make fake requests locally to test workflow.
    --test: Post to OSTI's test server.
    --prod: Post to OSTI's prod server.
    """

    commands = ["--dry-run", "--test", "--prod"]

    if (len(args) != 2) or (args[1] in ["--help", "-h"]) or (args[1] not in commands):
        print(help_s)
    else:
        mode = args[1][2:]
        p = Poster(mode)
        if mode == "dry-run":
            user_response = True
        if mode in ["test", "prod"]:
            log.warning(
                "[bold red]"
                f"Running in {mode} mode will trigger emails to PPPL and OSTI!"
            )
            user_response = Confirm.ask("Are you sure you wish you proceed?")
        log.info(f"{user_response=}")
        if user_response:
            p.run_pipeline()
        else:
            log.info("[bold red]Exiting!!! You must respond with a Y/y")

    script_log_end(SCRIPT_NAME, log)
