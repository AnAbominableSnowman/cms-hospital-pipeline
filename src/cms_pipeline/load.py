"""Load step: write each transformed CSV into a SQLite table (stdlib only).

Each load is one transaction that drops and recreates the table, so it is
idempotent: a re-run after a modified dataset replaces the old rows rather than
appending duplicates, and a failure mid-load rolls back to the previous table.

Column types and null handling come from schema.py: placeholder markers become
NULL and a column is INTEGER/REAL only when every non-null value is a plain number.
Without `types` every column is TEXT and values load verbatim.
"""

from __future__ import annotations

import csv
import re
import sqlite3
from itertools import islice
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence

from . import schema

BATCH_SIZE = 5000


def connect(db_path: str | Path) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    # isolation_level=None: we issue BEGIN/COMMIT ourselves so DDL is transactional too.
    return sqlite3.connect(str(db_path), isolation_level=None)


def table_name(csv_path: str | Path) -> str:
    """Table name from the output filename: lowercase, [a-z0-9_] only."""
    stem = Path(csv_path).stem.lower()
    name = re.sub(r"[^0-9a-z]+", "_", stem).strip("_")
    return name if not name[0].isdigit() else f"t_{name}"


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _batches(rows: Iterable[List[str]], size: int) -> Iterator[List[List[str]]]:
    it = iter(rows)
    while batch := list(islice(it, size)):
        yield batch


def load_csv(
    conn: sqlite3.Connection,
    csv_path: str | Path,
    table: str,
    types: Optional[Sequence[str]] = None,
    null_markers: Optional[Iterable[str]] = None,
) -> int:
    """Replace `table` with the CSV's contents. Returns the loaded row count.

    `types` gives a SQL type per column (default: all TEXT). `null_markers`, when
    given, turns blanks and those placeholder strings into NULL in every column;
    when None the values load exactly as they appear in the file.
    """
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)
        col_types = list(types) if types is not None else ["TEXT"] * len(header)
        if len(col_types) != len(header):
            raise ValueError(f"{len(col_types)} types for {len(header)} columns")
        cols = ", ".join(f"{_quote(c)} {t}" for c, t in zip(header, col_types))
        marks = ", ".join("?" for _ in header)
        t = _quote(table)
        convert = [schema.converter(ty) for ty in col_types]
        is_null = schema.null_checker(null_markers) if null_markers is not None else None

        def prepare(row: List[str]) -> List[object]:
            if len(row) != len(convert):
                raise ValueError(f"row has {len(row)} fields, expected {len(convert)}")
            return [
                None if (is_null is not None and is_null(v)) else conv(v)
                for v, conv in zip(row, convert)
            ]

        conn.execute("BEGIN")
        try:
            conn.execute(f"DROP TABLE IF EXISTS {t}")
            conn.execute(f"CREATE TABLE {t} ({cols})")
            for batch in _batches(map(prepare, reader), BATCH_SIZE):
                conn.executemany(f"INSERT INTO {t} VALUES ({marks})", batch)
            (count,) = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    return count
