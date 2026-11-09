from dataclasses import dataclass

import pytest
from responses import RequestsMock

from universe.config import Config
from universe.datagouv import Dataservice, Dataset, Organization, Tag, Topic
from universe.grist import GristAction, GristApi, GristEntry


@dataclass
class GristRecord:
    identifier: str | None
    type: str | None
    action: str | None = str(GristAction.INCLURE)
    category: str | None = None


class TestGristApi:
    def test_init(self):
        api = GristApi("https://www.example.com/grist", "token", "table")

        assert api.base_url == "https://www.example.com/grist"
        assert api.token == "token"
        assert api.table == "table"
        assert api.filter is None

    def test_get_entries_query_params_defaults(self, responses: RequestsMock, config: Config):
        api = GristApi(
            config.grist.url, config.grist.token, config.grist.table, config.grist.filter
        )
        responses.get(url=api.records_url, json={"records": []})

        assert api.get_entries() == []

        request = responses.calls[0].request
        assert request.params == {"limit": "0", "hidden": "true"}
        assert request.headers["Authorization"] == f"Bearer {api.token}"

    def test_get_entries_query_params_with_filter(self, responses: RequestsMock, config: Config):
        api = GristApi(config.grist.url, config.grist.token, config.grist.table, {"Coeur": [True]})
        responses.get(url=api.records_url, json={"records": []})

        assert api.get_entries() == []

        request = responses.calls[0].request
        assert request.params == {"limit": "0", "hidden": "true", "filter": '{"Coeur": [true]}'}

    @pytest.mark.parametrize("filter", [None, {}])
    def test_get_entries_query_params_empty_filter(
        self, responses: RequestsMock, config: Config, filter
    ):
        api = GristApi(config.grist.url, config.grist.token, config.grist.table, filter)
        responses.get(url=api.records_url, json={"records": []})

        assert api.get_entries() == []

        assert "filter" not in responses.calls[0].request.params

    @pytest.mark.parametrize(
        "inputs, expected",
        [
            # identifier
            (
                [GristRecord(" abc \n ", "dataset")],
                [GristEntry(identifier="abc", object_class=Dataset)],
            ),
            (
                [GristRecord("", "dataset")],
                [],
            ),
            (
                [GristRecord(None, "dataset")],
                [],
            ),
            # type
            (
                [GristRecord("abc", "dataset")],
                [GristEntry(identifier="abc", object_class=Dataset)],
            ),
            (
                [GristRecord("abc", "dataservice")],
                [GristEntry(identifier="abc", object_class=Dataservice)],
            ),
            (
                [GristRecord("abc", "organization")],
                [GristEntry(identifier="abc", object_class=Organization)],
            ),
            (
                [GristRecord("abc", "tag")],
                [GristEntry(identifier="abc", object_class=Tag)],
            ),
            (
                [GristRecord("abc", "topic")],
                [GristEntry(identifier="abc", object_class=Topic)],
            ),
            (
                [GristRecord("abc", "")],
                [],
            ),
            (
                [GristRecord("abc", None)],
                [],
            ),
            # action
            (
                [GristRecord("abc", "dataset", action="inclure")],
                [GristEntry(identifier="abc", object_class=Dataset, exclude=False)],
            ),
            (
                [GristRecord("abc", "dataset", action="exclure")],
                [GristEntry(identifier="abc", object_class=Dataset, exclude=True)],
            ),
            (
                [GristRecord("abc", "dataset", action="")],
                [],
            ),
            (
                [GristRecord("abc", "dataset", action=None)],
                [],
            ),
            # category
            (
                [GristRecord("abc", "dataset", category="foo bar")],
                [GristEntry(identifier="abc", object_class=Dataset, category="foo bar")],
            ),
            (
                [GristRecord("abc", "dataset", category=" foo\nbar \n ")],
                [GristEntry(identifier="abc", object_class=Dataset, category="foo\nbar")],
            ),
            (
                [GristRecord("abc", "dataset", category="")],
                [GristEntry(identifier="abc", object_class=Dataset, category=None)],
            ),
            (
                [GristRecord("abc", "dataset", category=None)],
                [GristEntry(identifier="abc", object_class=Dataset, category=None)],
            ),
        ],
    )
    def test_get_entries_results(
        self,
        responses: RequestsMock,
        config: Config,
        inputs: list[GristRecord],
        expected: list[GristEntry],
    ):
        api = GristApi(
            config.grist.url, config.grist.token, config.grist.table, config.grist.filter
        )
        responses.get(
            url=api.records_url,
            json={
                "records": [
                    {
                        "fields": {
                            "Type": input.type,
                            "Identifiant": input.identifier,
                            "Action": input.action,
                            "Categorie": input.category,
                        }
                    }
                    for input in inputs
                ]
            },
        )
        entries = api.get_entries()
        assert entries == expected
