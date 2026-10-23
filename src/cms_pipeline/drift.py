"""Drift detection: compare a freshly built file with what the last run loaded.

Two different policies, because the two signals mean different things:
  * Schema change (columns added/removed/renamed) is WARNED about, not blocked.
    CMS legitimately evolves schemas; blocking would stall good data, but a human
    should still see it because downstream SQL may depend on the columns.
  * A large row-count drop is BLOCKED (unless --force). It is the typical
    signature of a truncated or partial download that still parses cleanly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .quality import CsvStats


@dataclass(frozen=True)
class Drift:
    warnings: List[str]
    row_drop: Optional[str] = None  # set only when the drop exceeds the threshold


def compare(prev: Optional[Dict[str, Any]], now: CsvStats, max_row_drop: float) -> Drift:
    """`prev` is the manifest record from the last successful run (None if first time)."""
    if not prev:
        return Drift([])
    warnings: List[str] = []

    old_cols = prev.get("columns")
    if old_cols is not None and list(old_cols) != now.columns:
        added = sorted(set(now.columns) - set(old_cols))
        removed = sorted(set(old_cols) - set(now.columns))
        warnings.append(f"schema changed: +{added} -{removed}")
    elif old_cols is not None and prev.get("types") and now.types:
        # Same columns, so compare inferred types: a numeric column turning TEXT (or the
        # reverse) usually means a new placeholder value or a real change in the source.
        flips = [
            f"{c} {a}->{b}"
            for c, a, b in zip(now.columns, prev["types"], now.types)
            if a != b
        ]
        if flips:
            warnings.append(f"column types changed: {flips}")

    row_drop = None
    old_rows = prev.get("rows")
    if old_rows and now.rows < old_rows * (1 - max_row_drop):
        pct = 100 * (old_rows - now.rows) / old_rows
        row_drop = f"row count fell {old_rows} -> {now.rows} ({pct:.0f}% drop, limit {max_row_drop:.0%})"
    return Drift(warnings, row_drop)
