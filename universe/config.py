from dataclasses import dataclass
from deepmerge import always_merger
from pathlib import Path
from typing import Any

import dacite
import yaml


@dataclass
class DatagouvConfig:
    url: str
    token: str | None = None


@dataclass
class GristConfig:
    url: str
    table: str
    filter: dict[str, list[Any]] | None = None
    token: str | None = None


@dataclass
class Config:
    topic: str
    tag: str | None
    datagouv: DatagouvConfig
    grist: GristConfig
    output_dir: Path = Path("dist")

    @staticmethod
    def from_files(*paths: Path) -> "Config":
        assert len(paths) > 0
        dicts = [yaml.safe_load(path.read_text()) for path in paths]
        conf = dicts[0] if len(dicts) == 1 else always_merger.merge(*dicts)
        return dacite.from_dict(Config, conf, config=dacite.Config(cast=[Path]))
