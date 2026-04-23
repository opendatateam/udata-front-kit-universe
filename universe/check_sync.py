import sys

from pathlib import Path

from minicli import cli, run

from universe.config import Config
from universe.datagouv import DatagouvApi, Topic


@cli
def check_sync(universe: Path, *extra_configs: Path):
    """Check universe sync.

    :universe: Universe yaml config file
    :extra_configs: Additional config files (optional)
    """
    print("Running check of universe sync...")

    conf = Config.from_files(universe, *extra_configs)

    datagouv = DatagouvApi(
        base_url=conf.datagouv.url,
        token="no-token-needed",
        fail_on_errors=False,
        dry_run=True,
    )

    topic_id = datagouv.get_topic_id(conf.topic)

    nb_errors = 0
    for object_class in Topic.object_classes():
        mongo_ids = {o.id for o in datagouv.get_topic_objects(topic_id, object_class, use_search=False)}
        es_ids = {o.id for o in datagouv.get_topic_objects(topic_id, object_class, use_search=True)}

        missing_from_es = mongo_ids - es_ids
        stale_in_es = es_ids - mongo_ids

        if not missing_from_es and not stale_in_es:
            print(f"✅ {object_class.__name__}: {len(mongo_ids)}")
        else:
            nb_errors += 1
            print(f"❌ {object_class.__name__}: Mongo={len(mongo_ids)} / ES={len(es_ids)}", file=sys.stderr)
            for id in missing_from_es:
                print(f"  missing from ES: {id}", file=sys.stderr)
            for id in stale_in_es:
                print(f"  stale in ES:     {id}", file=sys.stderr)

    if nb_errors:
        print(f"\n{nb_errors} object type(s) are NOT in sync.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    run(check_sync)
