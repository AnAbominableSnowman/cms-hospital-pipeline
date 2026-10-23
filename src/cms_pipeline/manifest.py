"""Incremental-run state.

The manifest records, per processed file, the `modified` timestamp we last saw
plus the row count and column list of what we loaded (used for drift checks).
A dataset is reprocessed only when its timestamp changes (or on --force), which
is what turns a full re-download into an incremental run.

Writes are atomic (temp file + os.replace) so an interrupted run never leaves a
half-written manifest that would corrupt the next run's view of what is current.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


class Manifest:
    def __init__(self, path: str | os.PathLike) -> None:
        self.path = Path(path)
        self._data: Dict[str, Dict[str, Any]] = {}

    def load(self) -> "Manifest":
        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    for k, v in loaded.items():
                        # Older manifests stored a bare timestamp string.
                        self._data[str(k)] = v if isinstance(v, dict) else {"modified": str(v)}
            except (json.JSONDecodeError, OSError):
                # A corrupt manifest should not wedge the pipeline; treat it as
                # empty and let this run rebuild it from scratch.
                self._data = {}
        return self

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        return self._data.get(key)

    def is_modified(self, key: str, modified: str) -> bool:
        """True if `key` is new or its timestamp differs from the recorded one."""
        rec = self._data.get(key)
        return rec is None or rec.get("modified") != modified

    def update(
        self,
        key: str,
        modified: str,
        rows: Optional[int] = None,
        columns: Optional[List[str]] = None,
        types: Optional[List[str]] = None,
    ) -> None:
        self._data[key] = {"modified": modified, "rows": rows, "columns": columns, "types": types}

    def prune(self, valid_keys: Iterable[str]) -> List[str]:
        """Drop entries for datasets no longer in the catalog; return what was dropped."""
        keep = set(valid_keys)
        gone = [k for k in self._data if k not in keep]
        for k in gone:
            del self._data[k]
        return gone

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(self.path.parent), suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2, sort_keys=True)
            os.replace(tmp, self.path)  # atomic on same filesystem
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def as_dict(self) -> Dict[str, Dict[str, Any]]:
        return {k: dict(v) for k, v in self._data.items()}
