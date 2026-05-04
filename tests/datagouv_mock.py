from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field
from typing import cast

from responses import RequestsMock
from responses.matchers import header_matcher, json_params_matcher, query_param_matcher

from universe.config import Config
from universe.datagouv import (
    INACTIVE_OBJECT_MARKERS,
    DatagouvObject,
    Dataservice,
    Dataset,
    Organization,
    Owned,
    Tag,
    Topic,
    TopicElement,
    TopicObject,
)
from universe.grist import GristEntry
from universe.util import JSONObject, uniquify


@dataclass(frozen=True)
class Proxy[T: DatagouvObject]:
    object: T


@dataclass(frozen=True)
class ListProxy[T: DatagouvObject](Proxy[T]):
    children: list[TopicObject] = field(default_factory=list)


class DatagouvMock:
    responses: RequestsMock
    config: Config
    _id_counter: int
    # _objects is the register of all mocked objects, keyed by object id.
    # proxies keep track of the actual object, and children of those objects when applicable.
    _objects: dict[str, Proxy]

    def __init__(self, responses: RequestsMock, config: Config):
        self.responses = responses
        self.config = config
        self._id_counter = 0
        self._objects = {}

    @staticmethod
    def owning_organizations(*objects: Owned) -> Sequence[Organization]:
        return uniquify(org for obj in objects if (org := obj.organization))

    def dataservice(
        self,
        organization: Organization | None = None,
        tags: list[Tag] | None = None,
        topics: list[Topic] | None = None,
    ) -> Dataservice:
        id = self._next_id()
        dataservice = Dataservice(
            id=f"dataservice-{id}",
            slug=f"dataservice-{id}",
            title=f"Dataservice {id}",
            organization=organization,
            tags=[tag.id for tag in tags or []],
        )
        self._register_proxy(dataservice, organization, tags, topics)
        return dataservice

    def dataset(
        self,
        organization: Organization | None = None,
        tags: list[Tag] | None = None,
        topics: list[Topic] | None = None,
    ) -> Dataset:
        id = self._next_id()
        dataset = Dataset(
            id=f"dataset-{id}",
            slug=f"dataset-{id}",
            title=f"Dataset {id}",
            organization=organization,
            tags=[tag.id for tag in tags or []],
        )
        self._register_proxy(dataset, organization, tags, topics)
        return dataset

    def organization(self) -> Organization:
        id = self._next_id()
        organization = Organization(
            id=f"organization-{id}", slug=f"organization-{id}", name=f"Organization {id}"
        )
        self._register_list_proxy(organization)
        return organization

    def tag(self) -> Tag:
        id = self._next_id()
        tag = Tag(id=f"tag-{id}")
        self._register_list_proxy(tag)
        return tag

    def topic(self, organization: Organization | None = None) -> Topic:
        id = self._next_id()
        topic = Topic(
            id=f"topic-{id}", slug=f"topic-{id}", name=f"Topic {id}", organization=organization
        )
        # Topic doesn't need a ListProxy since it stores its own elements, but using one for symmetry,
        # to avoid dealing with another variant in _objects
        self._register_list_proxy(topic)
        return topic

    def universe(self, objects: Iterable[TopicObject] | None = None) -> Topic:
        # Ignoring universe.organization since it's not used in tests so far
        topic = self.topic()
        if objects:
            topic.elements.extend(TopicElement(f"element-{obj.id}", obj) for obj in objects)
        return topic

    def universe_from(self, grist_universe: Iterable[GristEntry]) -> Topic:
        inclusions = self._leaf_objects(
            *(entry.identifier for entry in grist_universe if not entry.exclude)
        )
        exclusions = self._leaf_objects(
            *(entry.identifier for entry in grist_universe if entry.exclude)
        )
        objects = [obj for obj in inclusions if obj not in exclusions]
        return self.universe(objects)

    def mock[T: TopicObject](
        self,
        existing_universe: list[T],
        grist_universe: list[GristEntry],
        bouquets: Iterable[Topic] | None = None,
    ) -> None:
        existing = self.universe(existing_universe)
        upcoming = self.universe_from(grist_universe)

        # datagouv.delete_all_topic_elements()
        # TODO: support reset=True

        for object_class in Topic.object_classes():
            upcoming_elements = upcoming.elements_of(object_class)
            existing_elements = existing.elements_of(object_class)

            # datagouv.get_upcoming_universe_perimeter()
            for entry in grist_universe:
                self.mock_get_upcoming_universe_perimeter(entry, object_class)

            # datagouv.get_topic_elements()
            self.mock_get_topic_elements(existing_elements, object_class)

            existing_object_ids = {elem.object.id for elem in existing_elements}
            upcoming_object_ids = {elem.object.id for elem in upcoming_elements}

            # datagouv.put_topic_elements()
            if additions := sorted(
                {
                    elem.object.id
                    for elem in upcoming_elements
                    if elem.object.id not in existing_object_ids
                }
            ):
                self.mock_put_topic_elements(additions, object_class)

            # datagouv.delete_topic_elements()
            if removals := sorted(
                {elem.id for elem in existing_elements if elem.object.id not in upcoming_object_ids}
            ):
                self.mock_delete_topic_elements(removals)

        # datagouv.get_bouquets()
        self.mock_get_bouquets(bouquets or [])

    def mock_get_upcoming_universe_perimeter(
        self, entry: GristEntry, object_class: type[TopicObject]
    ) -> None:
        if entry.object_class in (Dataset, Dataservice) and entry.object_class == object_class:
            self.mock_get_upcoming_universe_perimeter_object(entry.identifier, object_class)
        elif entry.object_class is Organization:
            self.mock_get_upcoming_universe_perimeter_organization(entry.identifier, object_class)
        elif entry.object_class is Tag:
            self.mock_get_upcoming_universe_perimeter_tag(entry.identifier, object_class)
        elif entry.object_class is Topic:
            self.mock_get_upcoming_universe_perimeter_topic(entry.identifier, object_class)

    def mock_get_upcoming_universe_perimeter_object(
        self, id: str, object_class: type[TopicObject]
    ) -> None:
        url = f"{self.config.datagouv.url}/api/1/{object_class.namespace()}/{id}/"
        obj = self._get_object(id, object_class)
        if not obj:
            _ = self.responses.get(url=url, status=404)
            return
        _ = self.responses.get(
            url=url,
            json={
                **self._as_dict(obj, ["id", "name", "slug"], missing={}),
                "organization": self._as_dict(obj.organization, ["id", "name", "slug"]),
            },
        )

    def mock_get_upcoming_universe_perimeter_organization(
        self, id: str, object_class: type[TopicObject]
    ) -> None:
        url = f"{self.config.datagouv.url}/api/1/organizations/{id}/"
        org = self._get_object(id, Organization)
        if not org:
            _ = self.responses.get(url=url, status=404)
            return

        _ = self.responses.get(url=url, json=self._as_dict(org, ["id", "name", "slug"]))

        objects = self._leaf_objects_of(org.id, object_class=object_class)
        _ = self.responses.get(
            url=f"{self.config.datagouv.url}/api/2/{object_class.namespace()}/search/",
            match=[
                header_matcher(
                    {"X-Fields": f"data{{id,{','.join(INACTIVE_OBJECT_MARKERS)}}},next_page"}
                ),
                query_param_matcher({"organization": org.id}, strict_match=False),
            ],
            json={
                "data": [{"id": obj.id} for obj in objects],
                "next_page": None,
            },
        )

    def mock_get_upcoming_universe_perimeter_tag(
        self, id: str, object_class: type[TopicObject]
    ) -> None:
        objects = self._leaf_objects_of(id, object_class=object_class)
        _ = self.responses.get(
            url=f"{self.config.datagouv.url}/api/1/{object_class.namespace()}/",
            match=[
                header_matcher(
                    {
                        "X-Fields": f"data{{id,organization{{id,name,slug}},{','.join(INACTIVE_OBJECT_MARKERS)}}},next_page"
                    }
                ),
                query_param_matcher({"tag": id}, strict_match=False),
            ],
            json={
                "data": [
                    {
                        "id": obj.id,
                        "organization": self._as_dict(obj.organization, ["id", "name", "slug"]),
                    }
                    for obj in objects
                ],
                "next_page": None,
            },
        )

    def mock_get_upcoming_universe_perimeter_topic(
        self, id: str, object_class: type[TopicObject]
    ) -> None:
        version = "2" if object_class is Dataset else "1"
        objects = self._leaf_objects_of(id, object_class=object_class)
        _ = self.responses.get(
            url=f"{self.config.datagouv.url}/api/{version}/{object_class.namespace()}/",
            match=[
                header_matcher(
                    {
                        "X-Fields": f"data{{id,organization{{id,name,slug}},{','.join(INACTIVE_OBJECT_MARKERS)}}},next_page"
                    }
                ),
                query_param_matcher({"topic": id}, strict_match=False),
            ],
            json={
                "data": [
                    {
                        "id": obj.id,
                        "organization": self._as_dict(obj.organization, ["id", "name", "slug"]),
                    }
                    for obj in objects
                ],
                "next_page": None,
            },
        )

    def mock_get_topic_elements(
        self, elements: Iterable[TopicElement], object_class: type[TopicObject]
    ) -> None:
        _ = self.responses.get(
            url=f"{self.config.datagouv.url}/api/2/topics/{self.config.topic}/elements/",
            match=[
                header_matcher(
                    {
                        "X-Fields": f"data{{id,element,{','.join(INACTIVE_OBJECT_MARKERS)}}},next_page"
                    }
                ),
                query_param_matcher({"class": object_class.model_name()}, strict_match=False),
            ],
            json={
                "data": [
                    {
                        "id": elem.id,
                        "element": {"class": object_class.model_name(), "id": elem.object.id},
                    }
                    for elem in elements
                ],
                "next_page": None,
            },
        )

    def mock_put_topic_elements(
        self, additions: Iterable[str], object_class: type[TopicObject]
    ) -> None:
        # TODO: support batching
        _ = self.responses.post(
            url=f"{self.config.datagouv.url}/api/2/topics/{self.config.topic}/elements/",
            match=[
                header_matcher(
                    {"Content-Type": "application/json", "X-API-KEY": self.config.datagouv.token}
                ),
                json_params_matcher(
                    [
                        {"element": {"class": object_class.model_name(), "id": oid}}
                        for oid in additions
                    ]
                ),
            ],
        )

    def mock_delete_topic_elements(self, removals: Iterable[str]) -> None:
        for eid in removals:
            _ = self.responses.delete(
                url=f"{self.config.datagouv.url}/api/2/topics/{self.config.topic}/elements/{eid}/",
                match=[header_matcher({"X-API-KEY": self.config.datagouv.token})],
            )

    def mock_get_bouquets(self, bouquets: Iterable[Topic]) -> None:
        _ = self.responses.get(
            url=f"{self.config.datagouv.url}/api/2/topics/",
            match=[
                header_matcher(
                    {
                        "X-API-KEY": self.config.datagouv.token,
                        "X-Fields": f"data{{id,name,slug,organization{{id,name,slug}},{','.join(INACTIVE_OBJECT_MARKERS)}}},next_page",
                    }
                ),
                query_param_matcher(
                    {"tag": self.config.tag, "include_private": "yes"}, strict_match=False
                ),
            ],
            json={
                "data": [
                    {
                        **self._as_dict(bouquet, ["id", "name", "slug"], missing={}),
                        "organization": self._as_dict(bouquet.organization, ["id", "name", "slug"]),
                    }
                    for bouquet in bouquets
                ],
                "next_page": None,
            },
        )

    def _next_id(self) -> int:
        self._id_counter += 1
        return self._id_counter

    def _register_proxy(
        self,
        object: TopicObject,
        organization: Organization | None = None,
        tags: list[Tag] | None = None,
        topics: list[Topic] | None = None,
    ) -> None:
        """
        Add object to the _objects register, and register it as child of its declared organization,
        tags and topics.
        """
        self._objects[object.id] = Proxy(object)
        if organization:
            proxy = self._objects[organization.id]
            assert isinstance(proxy, ListProxy)
            proxy.children.append(object)
        for tag in tags or []:
            proxy = self._objects[tag.id]
            assert isinstance(proxy, ListProxy)
            proxy.children.append(object)
        for topic in topics or []:
            proxy = self._objects[topic.id]
            assert isinstance(proxy, ListProxy)
            proxy.children.append(object)

    def _register_list_proxy(self, object: DatagouvObject) -> None:
        self._objects[object.id] = ListProxy(object, [])

    def _get_object[T: DatagouvObject](self, id: str, _: type[T]) -> T | None:
        if proxy := self._objects.get(id):
            return proxy.object

    def _leaf_objects(self, *ids: str) -> Sequence[TopicObject]:
        return list(self._leaf_objects_inner(*ids))

    def _leaf_objects_of[T: TopicObject](self, *ids: str, object_class: type[T]) -> Sequence[T]:
        # cast shouldn't be needed, but ty complains
        return [cast(T, obj) for obj in self._leaf_objects_inner(*ids) if type(obj) is object_class]

    def _leaf_objects_inner(self, *ids: str) -> Iterable[TopicObject]:
        for id in ids:
            if proxy := self._objects.get(id):
                if isinstance(proxy, ListProxy):
                    yield from proxy.children
                else:
                    yield proxy.object

    @staticmethod
    def _as_dict[T: JSONObject | None](
        object: DatagouvObject | None, fields: Iterable[str] | None = None, missing: T = None
    ) -> JSONObject | T:
        if not object:
            return missing
        d = asdict(object)
        if fields:
            return {k: v for k, v in d.items() if k in fields}
        else:
            return d
