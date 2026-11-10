import datetime
import json
import os
import sys
import time

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

from minicli import cli, run

from universe.config import Config
from universe.datagouv import (
    DatagouvApi,
    Dataservice,
    Dataset,
    Organization,
    Tag,
    Topic,
    TopicObject,
)
from universe.grist import GristApi, GristEntry
from universe.util import (
    uniquify,
    verbose_print,  # noqa: F401
)


ADDITIONS_BATCH_SIZE = 1000
REMOVALS_THRESHOLD = 1800


# LATER: drop along with GristEntry.category
@dataclass(frozen=True)
class CategorizedOrganization(Organization):
    category: str | None = None
    __eq__ = Organization.__eq__
    __hash__ = Organization.__hash__


class Perimeter:
    _inclusions: dict[str, Organization | None]
    _exclusions: set[str]

    def __init__(self):
        self._inclusions = {}
        self._exclusions = set()

    @property
    def ids(self) -> Sequence[str]:
        return self.objects.keys()

    @property
    def organizations(self) -> Sequence[Organization]:
        return uniquify(org for org in self.objects.values() if org is not None)

    @property
    def objects(self) -> dict[str, Organization | None]:
        return {id: org for id, org in self._inclusions.items() if id not in self._exclusions}

    def include(
        self,
        objects: TopicObject | Sequence[TopicObject],
        override_organization: Organization | None = None,
    ) -> None:
        objs = objects if isinstance(objects, Sequence) else [objects]
        if override_organization:
            self._inclusions |= {obj.id: override_organization for obj in objs}
        else:
            self._inclusions |= {obj.id: obj.organization for obj in objs}

    def exclude(self, objects: TopicObject | Sequence[TopicObject]) -> None:
        objs = objects if isinstance(objects, Sequence) else [objects]
        self._exclusions |= {obj.id for obj in objs}


def write_organizations_file(filepath: Path, organizations: list[Organization]):
    """Write organizations list to a JSON file in dist/"""
    print(f"Generating output file {filepath} with {len(organizations)} entries...")
    orgs = [
        {
            "id": org.id,
            "name": org.name,
            "slug": org.slug,
            "type": org.category if isinstance(org, CategorizedOrganization) else None,
        }
        for org in organizations
    ]
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with filepath.open("w") as f:
        json.dump(orgs, f, indent=2, ensure_ascii=False)


def get_upcoming_universe_perimeter(
    datagouv: DatagouvApi,
    grist_entries: Iterable[GristEntry],
    object_class: type[TopicObject],
) -> Perimeter:
    perimeter = Perimeter()

    for entry in grist_entries:
        verbose_print(
            f"Fetching {object_class.namespace()} for {entry.object_class.model_name()} {entry.identifier}..."
        )

        if entry.object_class in (Dataset, Dataservice) and entry.object_class is object_class:
            obj = datagouv.get_object(entry.identifier, entry.object_class)
            if not obj:
                print(
                    f"Unknown {entry.object_class.model_name()} {entry.identifier}", file=sys.stderr
                )
                continue
            if entry.exclude:
                perimeter.exclude(obj)
            else:
                perimeter.include(obj)

        elif entry.object_class is Organization:
            org = datagouv.get_organization(entry.identifier)
            if not org:
                print(
                    f"Unknown {entry.object_class.model_name()} {entry.identifier}", file=sys.stderr
                )
                continue
            objs = datagouv.get_organization_objects(org.id, object_class)
            if entry.exclude:
                perimeter.exclude(objs)
            else:
                org = CategorizedOrganization(
                    id=org.id, slug=org.slug, name=org.name, category=entry.category
                )
                perimeter.include(objs, override_organization=org)

        elif entry.object_class is Tag:
            objs = datagouv.get_tagged_objects(entry.identifier, object_class)
            if entry.exclude:
                perimeter.exclude(objs)
            else:
                perimeter.include(objs)

        elif entry.object_class is Topic:
            objs = datagouv.get_topic_objects(entry.identifier, object_class)
            if entry.exclude:
                perimeter.exclude(objs)
            else:
                perimeter.include(objs)

        else:
            continue

    return perimeter


@cli
def feed_universe(
    universe: Path,
    *extra_configs: Path,
    fail_on_errors: bool = False,
    dry_run: bool = False,
    reset: bool = False,
    verbose: bool = False,
):
    """Feed the universe with datasets and dataservices from organizations.

    :universe: Universe yaml config file
    :extra_configs: Additional config files (optional overrides)
    :fail_on_errors: Fail the run on http errors
    :dry_run: Perform a trial run without actual feeding
    :reset: Empty topic before refeeding it
    :verbose: Enable verbose mode
    """
    global verbose_print
    if verbose:
        verbose_print = print

    conf = Config.from_files(universe, *extra_configs)

    conf.datagouv.token = os.getenv("DATAGOUV_API_KEY", conf.datagouv.token)
    if not conf.datagouv.token:
        print(
            "Error: missing datagouv token. Set `datagouv.token` in config or `DATAGOUV_API_KEY` in env."
        )
        sys.exit(1)
    conf.grist.token = os.getenv("GRIST_API_KEY", conf.grist.token)
    if not conf.grist.token:
        print("Error: missing grist token. Set `grist.token` in config or `GRIST_API_KEY` in env.")
        sys.exit(1)

    feed(
        conf=conf,
        fail_on_errors=fail_on_errors,
        dry_run=dry_run,
        reset=reset,
    )


def feed(
    conf: Config,
    fail_on_errors: bool = False,
    dry_run: bool = False,
    reset: bool = False,
) -> None:
    datagouv = DatagouvApi(
        base_url=conf.datagouv.url,
        token=conf.datagouv.token,
        fail_on_errors=fail_on_errors,
        dry_run=dry_run,
    )

    grist = GristApi(
        base_url=conf.grist.url,
        token=conf.grist.token,
        table=conf.grist.table,
        filter=conf.grist.filter,
    )

    print(f"Starting at {datetime.datetime.now():%c}.")
    if dry_run:
        print("*** DRY RUN ***")

    t_all = time.time()
    try:
        print(f"Processing universe topic '{conf.topic}'.")

        verbose_print("Fetching grist universe definition...")
        grist_entries = grist.get_entries()
        print(f"Found {len(grist_entries)} entries in grist.")

        if reset:
            print("Removing ALL elements from topic...")
            datagouv.delete_all_topic_elements(conf.topic)

        for object_class in Topic.object_classes():
            verbose_print(f"Fetching upcoming {object_class.namespace()}...")
            upcoming_perimeter = get_upcoming_universe_perimeter(
                datagouv, grist_entries, object_class
            )
            print(
                f"Found {len(upcoming_perimeter.ids)} {object_class.namespace()} matching the upcoming universe."
            )

            verbose_print(f"Fetching existing {object_class.namespace()}...")
            existing_elements = datagouv.get_topic_elements(conf.topic, object_class)
            existing_object_ids = uniquify(e.object.id for e in existing_elements)
            print(
                f"Found {len(existing_object_ids)} {object_class.namespace()} currently in the universe."
            )

            verbose_print("Computing topic updates...")
            additions = sorted(set(upcoming_perimeter.ids) - set(existing_object_ids))
            removals = sorted(set(existing_object_ids) - set(upcoming_perimeter.ids))
            if (n := len(removals)) > REMOVALS_THRESHOLD:
                raise Exception(f"Too many removals ({n} > {REMOVALS_THRESHOLD}), aborting.")

            print("Updating topic:")
            print(f"- Adding {len(additions)} {object_class.namespace()}...")
            datagouv.put_topic_elements(conf.topic, object_class, additions, ADDITIONS_BATCH_SIZE)

            print(f"- Deleting {len(removals)} {object_class.namespace()}...")
            element_ids = [e.id for e in existing_elements if e.object.id in removals]
            datagouv.delete_topic_elements(conf.topic, element_ids)

            write_organizations_file(
                conf.output_dir / f"organizations-{object_class.namespace()}.json",
                sorted(upcoming_perimeter.organizations),
            )

        if conf.tag:
            # TODO: can this be handled by the main update loop? datasets/services in bouquets should also be in universe?
            verbose_print("Fetching additional organizations from bouquets...")
            bouquets = datagouv.get_bouquets(conf.tag)
            print(f"Found {len(bouquets)} bouquets with the universe tag.")
            bouquet_orgs = uniquify(org for b in bouquets if (org := b.organization))
            write_organizations_file(
                conf.output_dir / "organizations-bouquets.json",
                sorted(bouquet_orgs),
            )

    finally:
        print(f"Done at {datetime.datetime.now():%c} in {time.time() - t_all:.0f} seconds.")


if __name__ == "__main__":
    run(feed_universe)
