"""Configuration: defaults < config.toml < command-line flags."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from .metadata import DEFAULT_METADATA_URL, DEFAULT_THEME
from .schema import DEFAULT_NULL_MARKERS


@dataclass(frozen=True)
class Config:
    out_dir: str = "output"
    manifest_path: str = "state/manifest.json"
    db_path: Optional[str] = "state/hospitals.db"  # None disables the load step
    metadata_url: str = DEFAULT_METADATA_URL
    theme: Optional[str] = DEFAULT_THEME
    workers: int = 8
    null_markers: Tuple[str, ...] = DEFAULT_NULL_MARKERS  # placeholder text loaded as NULL
    max_row_drop: float = 0.3  # block a file whose rows fall by more than this fraction

    def with_overrides(self, **overrides: Any) -> "Config":
        """Apply non-None overrides (e.g. from CLI flags)."""
        return replace(self, **{k: v for k, v in overrides.items() if v is not None})


def load_config(path: str | Path | None) -> Config:
    """Read a TOML file into a Config. A missing path means pure defaults."""
    if path is None:
        return Config()
    with open(path, "rb") as f:
        data: Dict[str, Any] = tomllib.load(f)
    known = {f.name for f in fields(Config)}
    unknown = set(data) - known
    if unknown:
        raise ValueError(f"unknown config keys: {sorted(unknown)}")
    if data.get("db_path") == "":  # TOML has no null; empty string means "no database"
        data["db_path"] = None
    if "null_markers" in data:
        data["null_markers"] = tuple(data["null_markers"])
    return Config(**data)
