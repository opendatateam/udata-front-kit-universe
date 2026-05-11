import requests

from collections.abc import Sequence
from dataclasses import dataclass
from enum import auto, StrEnum

from universe.datagouv import DatagouvObject
from universe.util import sanitize_string, uniquify


class GristAction(StrEnum):
    """
    Grist Action column values.
    The main use for this class is to map/validate the grist input before it is converted to a
    simple flag in GristEntry.exclude.
    """

    INCLURE = auto()
    EXCLURE = auto()


@dataclass(frozen=True, kw_only=True)
class GristEntry[T: DatagouvObject]:
    identifier: str
    object_class: type[T]
    exclude: bool = False
    category: str | None = None  # LATER: drop (backcompat ecologie for now)


class GristApi:
    def __init__(self, base_url: str, table: str, token: str):
        self.base_url = base_url
        self.table = table
        self.token = token

    @property
    def records_url(self):
        return f"{self.base_url}/tables/{self.table}/records"

    def get_entries(self) -> Sequence[GristEntry]:
        r = requests.get(
            self.records_url,
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
            params={"limit": 0},
        )
        r.raise_for_status()
        records = r.json()["records"]
        # index entries from start=1 to match Grist line-numbering
        entries = [self._make_entry(rec["fields"], idx) for idx, rec in enumerate(records, start=1)]
        return [entry for entry in uniquify(entries) if entry is not None]

    @staticmethod
    def _make_entry(record: dict[str, str], index: int) -> GristEntry | None:
        identifier = sanitize_string(record["Identifiant"])
        type = record["Type"]
        action = record["Action"]
        category = sanitize_string(record.get("Categorie"))
        if not (identifier and type and action):
            print(f"Warning: Invalid grist entry line {index}")
            return None
        return GristEntry(
            identifier=identifier,
            object_class=DatagouvObject.class_from_name(type),
            exclude=GristAction(action) is GristAction.EXCLURE,
            category=category,
        )
