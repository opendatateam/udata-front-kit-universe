from dataclasses import dataclass

import pytest
from responses import RequestsMock

from universe.config import Config
from universe.datagouv import Dataservice, Dataset, Organization, Tag, Topic
from universe.grist import GristAction, GristApi, GristEntry


@pytest.fixture
def api(config: Config) -> GristApi:
    return GristApi(config.grist.url, config.grist.table, config.grist.token)


@dataclass
class GristRecord:
    identifier: str | None
    type: str | None
    action: str | None = str(GristAction.INCLURE)
    category: str | None = None


class TestGristEntry:
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
    def test_get_entries(
        self,
        responses: RequestsMock,
        api: GristApi,
        inputs: list[GristRecord],
        expected: list[GristEntry],
    ):
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
