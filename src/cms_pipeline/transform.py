"""Header normalization: convert arbitrary column names to snake_case.

Written to handle the messy real-world cases, not just "Title Case" -> "title_case":

  * CamelCase / PascalCase boundaries        -> "FacilityID"      -> facility_id
  * acronym runs                             -> "HCAHPS Score"    -> hcahps_score
  * apostrophes (straight or curly) dropped  -> "Hospital’s"      -> hospitals
  * punctuation, %, /, parentheses           -> "Payment (per %)" -> payment_per
  * accented letters folded to ASCII         -> "Résumé Score"    -> resume_score
  * leading digits kept as-is                -> "30 Day Rate"     -> 30_day_rate
    (the SQLite loader quotes every identifier, so no "_" prefix is needed)
  * duplicate results within one header row  -> foo, foo          -> foo, foo_1
"""

from __future__ import annotations

import re
import unicodedata
from typing import Iterable, List

# Insert a boundary between an acronym/char run and a following TitleCase word:
#   "HTTPServer" -> "HTTP_Server", "aB" handled by _CAMEL_TAIL below.
_CAMEL_HEAD = re.compile(r"(.)([A-Z][a-z]+)")
# Apostrophes join a word rather than split it: "Hospital’s" -> "hospitals", not "hospital_s".
_APOSTROPHES = re.compile(r"['‘’ʼ`]")
# Insert a boundary between a lower/digit and a following uppercase: "idValue" -> "id_Value".
_CAMEL_TAIL = re.compile(r"([a-z0-9])([A-Z])")
# Any run of characters that cannot appear in a snake_case identifier.
_NON_ALNUM = re.compile(r"[^0-9a-zA-Z]+")
_MULTI_UNDERSCORE = re.compile(r"_+")


def to_snake_case(name: str) -> str:
    """Normalize a single column name to snake_case.

    Always returns a non-empty, lowercase ASCII token. A name that reduces to
    nothing (e.g. "???") becomes "column".
    """
    s = _APOSTROPHES.sub("", name.strip())
    # "é" -> "e" + combining accent; drop the accent so the letter survives.
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = _CAMEL_HEAD.sub(r"\1_\2", s)
    s = _CAMEL_TAIL.sub(r"\1_\2", s)
    s = _NON_ALNUM.sub("_", s)
    s = _MULTI_UNDERSCORE.sub("_", s).strip("_").lower()
    return s or "column"


def snake_case_headers(headers: Iterable[str]) -> List[str]:
    """Normalize a full header row, guaranteeing uniqueness.

    Collisions are disambiguated with a numeric suffix (foo, foo_1, foo_2 ...).
    The suffix itself is checked against names already emitted, so a pre-existing
    "foo_1" in the input cannot be silently clobbered.
    """
    out: List[str] = []
    used: set[str] = set()
    for raw in headers:
        base = to_snake_case(raw)
        candidate = base
        n = 0
        while candidate in used:
            n += 1
            candidate = f"{base}_{n}"
        used.add(candidate)
        out.append(candidate)
    return out
