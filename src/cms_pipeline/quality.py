"""Data-quality gate run on every transformed CSV before it is loaded.

A file that fails is reported as a failed dataset and is NOT recorded in the
manifest, so it is retried on the next run instead of being silently accepted.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List

from .schema import DEFAULT_NULL_MARKERS, ColumnInferer

# Lowercase words joined by single underscores ("30_day_rate" included).
SNAKE_CASE = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")


class DataQualityError(ValueError):
    pass


@dataclass(frozen=True)
class CsvStats:
    rows: int
    columns: List[str]
    types: List[str] = field(default_factory=list)  # SQL type per column (see schema.py)
    nulls: int = 0  # cells that will load as NULL
    suspect: List[str] = field(default_factory=list)  # near-numeric columns kept TEXT


def check_csv(path: str | Path, null_markers: Iterable[str] = DEFAULT_NULL_MARKERS) -> CsvStats:
    """Validate one output CSV; return its row count, columns and inferred column types.

    Checks: header present, every name snake_case and unique, at least one data
    row, and every row has exactly as many fields as the header. Type inference
    (schema.py) rides along on the same pass over the file.
    """
    path = Path(path)
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            raise DataQualityError(f"{path.name}: file is empty") from None

        bad = [h for h in header if not SNAKE_CASE.match(h)]
        if bad:
            raise DataQualityError(f"{path.name}: non-snake_case headers {bad[:5]}")
        if len(set(header)) != len(header):
            raise DataQualityError(f"{path.name}: duplicate headers")

        inferer = ColumnInferer(header, null_markers)
        rows = 0
        for row in reader:
            rows += 1
            if len(row) != len(header):
                raise DataQualityError(
                    f"{path.name}: row {rows} has {len(row)} fields, expected {len(header)}"
                )
            inferer.feed(row)
    if rows == 0:
        raise DataQualityError(f"{path.name}: no data rows")
    return CsvStats(
        rows=rows,
        columns=header,
        types=inferer.types,
        nulls=inferer.nulls,
        suspect=inferer.suspect,
    )
