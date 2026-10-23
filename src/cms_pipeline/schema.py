"""Null-marker normalisation and column type inference.

CMS files mix real numbers with placeholder text ("Not Available", "N/A", ...).
Loaded as-is, that forces every column to TEXT and makes a bare CAST('Not
Available' AS REAL) silently equal 0.0. So at load time:

  * placeholder markers (and blanks) become SQL NULL, not the string "NA": a "NA"
    string is just another placeholder and would still stop the column being
    numeric, while NULL is skipped by AVG()/COUNT(col) and friends;
  * a column is typed INTEGER or REAL only if EVERY non-null value in it is a
    plain number, otherwise it stays TEXT. Inference reads the whole file, never
    a sample, so one late stray value can't break the load.

Guards against corrupting data:
  * identifier-like columns (ids, zip, phone, footnotes) are always TEXT;
  * a value with a leading zero ("010001") is not a number, so the column stays TEXT;
  * a column that is almost entirely numeric but kept TEXT is reported, so an
    unrecognised placeholder shows up instead of silently blocking typing.

The raw CSVs in output/ are never modified by any of this.
"""

from __future__ import annotations

import re
from typing import Callable, Iterable, List, Sequence

DEFAULT_NULL_MARKERS = ("Not Available", "N/A", "Not Applicable", "Too Few to Report")

# Column names that are identifiers/codes, never quantities, even when the values
# look numeric (facility_id, zip_code, telephone_number, *_footnote, ...).
FORCED_TEXT = re.compile(r"(^|_)(id|code|zip|telephone|phone|npi|ccn|footnote)(_|$)")

# Plain decimal numbers only. No leading zeros ("007" is an identifier, not 7),
# no thousands separators, no exponents.
_NUMBER = re.compile(r"^-?(0|[1-9]\d*)(\.\d+)?$")
_MAX_INT_DIGITS = 18  # stay inside SQLite's signed 64-bit INTEGER
_SUSPECT_MIN_NUMERIC_SHARE = 0.9


_FOOTNOTE_SUFFIX = re.compile(r"\s*\(\d+(?:\s*,\s*\d+)*\)$")  # e.g. "N/A (5)", "Not Available (1, 3)"


def null_checker(markers: Iterable[str]) -> Callable[[str], bool]:
    """Return is_null(value): true for blanks and any marker (case/space-insensitive).

    A marker followed by a footnote reference, like "N/A (5)", also counts. The suffix
    is only stripped to test for a marker, so "5 (3)" is still not null.
    """
    folded = {m.strip().casefold() for m in markers}

    def is_null(value: str) -> bool:
        s = value.strip()
        if s == "" or s.casefold() in folded:
            return True
        return _FOOTNOTE_SUFFIX.sub("", s).casefold() in folded

    return is_null


class ColumnInferer:
    """Feed it every data row; then read `types`, `nulls` and `suspect`."""

    def __init__(self, columns: Sequence[str], null_markers: Iterable[str]) -> None:
        self.columns = list(columns)
        self._is_null = null_checker(null_markers)
        n = len(self.columns)
        self._forced = [bool(FORCED_TEXT.search(c)) for c in self.columns]
        self._all_int = [True] * n
        self._numeric = [0] * n
        self._other = [0] * n
        self._examples: List[List[str]] = [[] for _ in range(n)]
        self.nulls = 0

    def feed(self, row: Sequence[str]) -> None:
        for j, value in enumerate(row):
            if self._is_null(value):
                self.nulls += 1
                continue
            s = value.strip()
            if _NUMBER.match(s):
                self._numeric[j] += 1
                if "." in s or len(s.lstrip("-")) > _MAX_INT_DIGITS:
                    self._all_int[j] = False
            else:
                self._other[j] += 1
                ex = self._examples[j]
                if len(ex) < 3 and s not in ex:
                    ex.append(s)

    @property
    def types(self) -> List[str]:
        out = []
        for j in range(len(self.columns)):
            numeric_only = self._numeric[j] > 0 and self._other[j] == 0
            if self._forced[j] or not numeric_only:
                out.append("TEXT")  # includes all-null columns: nothing to infer from
            else:
                out.append("INTEGER" if self._all_int[j] else "REAL")
        return out

    @property
    def suspect(self) -> List[str]:
        """Non-forced TEXT columns that are >=90% numeric: probably an unknown placeholder."""
        found = []
        for j, name in enumerate(self.columns):
            total = self._numeric[j] + self._other[j]
            if (
                not self._forced[j]
                and self._numeric[j] > 0
                and self._other[j] > 0
                and self._numeric[j] / total >= _SUSPECT_MIN_NUMERIC_SHARE
            ):
                pct = 100 * self._numeric[j] / total
                found.append(f"{name}: {pct:.0f}% numeric but kept TEXT because of {self._examples[j]}")
        return found


def converter(sql_type: str) -> Callable[[str], object]:
    """Value converter for a column type; only called on non-null values."""
    if sql_type == "INTEGER":
        return lambda v: int(v.strip())
    if sql_type == "REAL":
        return lambda v: float(v.strip())
    return lambda v: v
