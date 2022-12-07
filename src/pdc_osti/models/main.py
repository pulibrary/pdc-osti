from typing import List

from pydantic import BaseModel

from .commons import Author, RelatedIdentifier


class OSTI(BaseModel):
    title: str
    dataset_type: str
    site_url: str
    contract_nos: str
    sponsor_org: str
    research_org: str
    accession_num: str
    publication_date: str
    othnondoe_contract_nos: str
    description: str | None = None
    related_identifiers: List[RelatedIdentifier] | None = None
    keywords: str | None = None


class DSpaceOSTI(BaseModel, OSTI):
    creators: str


class DataCommonsOSTI(BaseModel, OSTI):
    authors: List[Author]
    doi: str
