import inspect
import requests
import sys

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from functools import total_ordering
from itertools import batched
from typing import get_args, Any, Protocol, TypeAlias

import dacite

from universe.util import (
    JSONObject,
    elapsed,
    elapsed_and_count,
    normalize_string,
    uniquify,
    verbose_print,
)


session = requests.Session()


@dataclass(frozen=True)
class DatagouvObject:
    """Base class for datagouv objects."""

    id: str

    @staticmethod
    def class_from_name(name: str) -> type["DatagouvObject"]:
        # Warning: Subclasses should be declared in current file. Otherwise, change lookup scope.
        for clazz_name, clazz in inspect.getmembers(
            sys.modules[__name__], predicate=inspect.isclass
        ):
            if clazz_name.lower() == name.lower() and issubclass(clazz, DatagouvObject):
                return clazz
        raise TypeError(f"{name} is not a DatagouvObject")

    @classmethod
    def model_name(cls) -> str:
        """
        Name of the model class as declared in `udata.udata.core.*.models`.
        Override if different from `cls.__name__`.
        """
        return cls.__name__


class Addressable(Protocol):
    slug: str | None = None

    @classmethod
    def model_name(cls) -> str: ...

    @classmethod
    def namespace(cls) -> str:
        """
        API namespace for the model.
        Override if different from plural lowercased `model_name()`.
        """
        return f"{cls.model_name().lower()}s"


@total_ordering
@dataclass(frozen=True)
class Organization(DatagouvObject, Addressable):
    slug: str | None = None
    name: str | None = None

    def __lt__(self, other: "Organization") -> bool:
        # TODO: replace assert guards with refresh() when we move to datagouv-client
        assert self.name is not None
        assert other.name is not None
        assert self.slug is not None
        assert other.slug is not None
        self_name = normalize_string(self.name)
        other_name = normalize_string(other.name)
        return self_name < other_name or (self_name == other_name and self.slug < other.slug)


class Owned(Protocol):
    organization: Organization | None = None


class AddressableOwned(Addressable, Owned, Protocol):
    pass


@dataclass(frozen=True)
class Tag(DatagouvObject):
    pass


class Tagged(Protocol):
    tags: list[str]


class AddressableTagged(Addressable, Tagged, Protocol):
    pass


@dataclass(frozen=True)
class Dataset(DatagouvObject, AddressableOwned, AddressableTagged):
    slug: str | None = None
    title: str | None = None
    organization: Organization | None = None
    tags: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Dataservice(DatagouvObject, AddressableOwned, AddressableTagged):
    slug: str | None = None
    title: str | None = None
    organization: Organization | None = None
    tags: list[str] = field(default_factory=list)


TopicObject: TypeAlias = Dataset | Dataservice


@dataclass
class TopicElement[T: TopicObject]:
    id: str
    object: T


@dataclass(frozen=True)
class Topic(DatagouvObject, AddressableOwned):
    slug: str | None = None
    name: str | None = None
    organization: Organization | None = None
    elements: list[TopicElement] = field(default_factory=list)

    @classmethod
    def object_classes(cls) -> tuple[type[TopicObject]]:
        return get_args(TopicObject)

    @property
    def objects(self) -> Sequence[TopicObject]:
        return [elem.object for elem in self.elements]

    def elements_of[T: TopicObject](self, object_class: type[T]) -> Sequence[TopicElement[T]]:
        return [elem for elem in self.elements if type(elem.object) is object_class]

    def objects_of[T: TopicObject](self, object_class: type[T]) -> Sequence[T]:
        return [elem.object for elem in self.elements_of(object_class)]


INACTIVE_OBJECT_MARKERS = [
    "archived",  # dataset
    "archived_at",  # dataservice
    "deleted",  # dataset, organization
    "deleted_at",  # dataservice
    "private",  # dataset, dataservice
    "extras{geop:dataset_id}",  # dataset
]

DEFAULT_PAGE_SIZE = 1000


class DatagouvApi:
    def __init__(
        self, base_url: str, token: str, fail_on_errors: bool = False, dry_run: bool = False
    ):
        self.base_url = base_url
        self.token = token
        self.fail_on_errors = fail_on_errors
        self.dry_run = dry_run
        print(f"API for {self.base_url} ready.")

    def get_object[T: Addressable](self, id_or_slug: str, object_class: type[T]) -> T | None:
        url = f"{self.base_url}/api/1/{object_class.namespace()}/{id_or_slug}/"
        r = session.get(url)
        if not r.ok:
            return None
        return dacite.from_dict(object_class, r.json())

    def get_organization(self, id_or_slug: str) -> Organization | None:
        return self.get_object(id_or_slug, Organization)

    def get_organization_objects[T: AddressableTagged](
        self, org_id: str, object_class: type[T]
    ) -> Sequence[T]:
        url = f"{self.base_url}/api/2/{object_class.namespace()}/search/"
        params = {"organization": org_id}
        objs = self._get_objects(url, params=params)
        return [dacite.from_dict(object_class, o) for o in objs]

    def get_tagged_objects[T: AddressableTagged](
        self, tag_id: str, object_class: type[T]
    ) -> Sequence[T]:
        url = f"{self.base_url}/api/1/{object_class.namespace()}/"
        params = {"tag": tag_id}
        objs = self._get_objects(url, params=params, fields=["id", "organization{id,name,slug}"])
        return [dacite.from_dict(object_class, o) for o in objs]

    def get_topic_id(self, topic_id_or_slug: str) -> str:
        url = f"{self.base_url}/api/2/topics/{topic_id_or_slug}/"
        r = session.get(url)
        r.raise_for_status()
        return r.json()["id"]

    def get_topic_objects_count(
        self, topic_id: str, object_class: type[TopicObject], use_search: bool = False
    ) -> int:
        if use_search:
            url = f"{self.base_url}/api/2/{object_class.namespace()}/search/"
        else:
            version = "2" if object_class is Dataset else "1"
            url = f"{self.base_url}/api/{version}/{object_class.namespace()}/"
        params = {"topic": topic_id, "page_size": 1}
        r = session.get(url, params=params)
        r.raise_for_status()
        return int(r.json()["total"])

    @elapsed_and_count
    def get_topic_elements(
        self, topic_id_or_slug: str, object_class: type[TopicObject]
    ) -> Sequence[TopicElement]:
        url = f"{self.base_url}/api/2/topics/{topic_id_or_slug}/elements/"
        params = {"class": object_class.model_name()}
        objs = self._get_objects(url, params=params, fields=["id", "element{id}"])
        return [TopicElement(id=o["id"], object=object_class(o["element"]["id"])) for o in objs]

    def get_topic_objects[T: TopicObject](
        self, topic_id: str, object_class: type[T], use_search: bool = False
    ) -> Sequence[T]:
        if use_search:
            url = f"{self.base_url}/api/2/{object_class.namespace()}/search/"
        else:
            version = "2" if object_class is Dataset else "1"
            url = f"{self.base_url}/api/{version}/{object_class.namespace()}/"
        params = {"topic": topic_id}
        objs = self._get_objects(url, params=params, fields=["id", "organization{id,name,slug}"])
        return [dacite.from_dict(object_class, o) for o in objs]

    @elapsed_and_count
    def put_topic_elements(
        self,
        topic_id_or_slug: str,
        object_class: type[TopicObject],
        object_ids: Iterable[str],
        batch_size: int = 0,
    ) -> None:
        url = f"{self.base_url}/api/2/topics/{topic_id_or_slug}/elements/"
        headers = {"Content-Type": "application/json", "X-API-KEY": self.token}
        batches = batched(object_ids, batch_size) if batch_size else [object_ids]
        for batch in batches:
            data = [{"element": {"class": object_class.model_name(), "id": id}} for id in batch]
            if not self.dry_run:
                session.post(url, json=data, headers=headers).raise_for_status()

    @elapsed
    def delete_topic_elements(self, topic_id_or_slug: str, element_ids: Iterable[str]) -> None:
        for element_id in element_ids:
            try:
                url = f"{self.base_url}/api/2/topics/{topic_id_or_slug}/elements/{element_id}/"
                headers = {"X-API-KEY": self.token}
                if not self.dry_run:
                    session.delete(url, headers=headers).raise_for_status()
            except requests.HTTPError as e:
                if self.fail_on_errors:
                    raise
                verbose_print(e)

    @elapsed
    def delete_all_topic_elements(self, topic_id_or_slug: str) -> None:
        url = f"{self.base_url}/api/2/topics/{topic_id_or_slug}/elements/"
        if not self.dry_run:
            headers = {"Content-Type": "application/json", "X-API-KEY": self.token}
            session.delete(url, headers=headers).raise_for_status()

    @elapsed_and_count
    def get_bouquets(self, universe_tag: str, include_private: bool = True) -> Sequence[Topic]:
        """Fetch all bouquets (topics) tagged with the universe tag"""
        url = f"{self.base_url}/api/2/topics/"
        params = {"tag": universe_tag}
        headers = {}
        if include_private:
            params["include_private"] = "yes"
            headers["X-API-KEY"] = self.token
        objs = self._get_objects(
            url,
            params=params,
            headers=headers,
            fields=["id", "name", "slug", "organization{id,name,slug}"],
        )
        return [
            Topic(
                id=d["id"],
                slug=d["slug"],
                name=d["name"],
                organization=Organization(id=o["id"], name=o["name"], slug=o["slug"])
                if (o := d.get("organization"))
                else None,
            )
            for d in objs
        ]

    def _get_objects(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        fields: Iterable[str] | None = None,
        active: bool = True,
    ) -> Iterable[JSONObject]:
        _params = dict(params or {})  # local copy
        if "page_size" not in _params:
            _params["page_size"] = DEFAULT_PAGE_SIZE

        _headers = dict(headers or {})  # local copy
        if fields or active:
            _fields = uniquify(
                ["id"]
                + (list(fields) if fields else [])
                + (INACTIVE_OBJECT_MARKERS if active else [])
            )
            _headers["X-Fields"] = f"data{{{','.join(_fields)}}},next_page"

        try:
            while True:
                r = session.get(url, params=_params, headers=_headers)
                r.raise_for_status()
                data = r.json()
                for obj in data["data"]:
                    if active and any(obj.get(m) for m in INACTIVE_OBJECT_MARKERS):
                        continue
                    yield obj
                url = data.get("next_page")
                if not url:
                    return
        except requests.HTTPError as e:
            if self.fail_on_errors:
                raise
            verbose_print(e)
