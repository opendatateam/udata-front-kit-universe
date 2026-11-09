from responses import RequestsMock
from responses.matchers import query_param_matcher

from universe.config import Config
from universe.datagouv import DatagouvObject
from universe.grist import GristAction, GristEntry


class GristMock:
    responses: RequestsMock
    config: Config

    def __init__(self, responses: RequestsMock, config: Config):
        self.responses = responses
        self.config = config

    def entry(
        self, object: DatagouvObject, exclude: bool = False, category: str | None = None
    ) -> GristEntry:
        return GristEntry(
            identifier=object.id,
            object_class=type(object),
            exclude=exclude,
            category=category,
        )

    def raw_entry(
        self,
        identifier: str,
        object_class: type[DatagouvObject],
        exclude: bool = False,
        category: str | None = None,
    ) -> GristEntry:
        return GristEntry(
            identifier=identifier,
            object_class=object_class,
            exclude=exclude,
            category=category,
        )

    def mock(self, universe: list[GristEntry]) -> None:
        # grist.get_entries()
        _ = self.responses.get(
            url=f"{self.config.grist.url}/tables/{self.config.grist.table}/records",
            match=[query_param_matcher({"limit": 0, "hidden": "true"})],
            json={
                "records": [
                    {
                        "fields": {
                            "Type": entry.object_class.model_name(),
                            "Identifiant": entry.identifier,
                            "Action": GristAction.EXCLURE if entry.exclude else GristAction.INCLURE,
                            "Categorie": entry.category,
                        }
                    }
                    for entry in universe
                ]
            },
        )
