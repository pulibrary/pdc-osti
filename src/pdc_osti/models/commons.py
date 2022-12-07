from pydantic import BaseModel


class Author(BaseModel):
    first_name: str
    middle_name: str | None = None
    last_name: str
    affiliation_name: str | None = None
    private_email: str | None = None
    orcid_id: str | None = None
    contributorType: str | None = None


class RelatedIdentifier(BaseModel):
    related_identifier: str
    relation_type: str = "IsReferencedBy"
    related_identifier_type: str = "DOI"
