"""CMS Hospital provider-data ETL pipeline.

A small, incremental, concurrent ETL that:
  1. reads the CMS Provider Data Catalog metadata API,
  2. selects datasets under a given theme (default: "Hospitals"),
  3. downloads only the ones changed since the last run,
  4. rewrites every CSV header to snake_case while streaming,
  5. records what it processed so the next run is incremental.
"""

from .metadata import Dataset, fetch_datasets
from .transform import to_snake_case, snake_case_headers
from .manifest import Manifest
from .pipeline import run, build_session

__all__ = [
    "Dataset",
    "fetch_datasets",
    "to_snake_case",
    "snake_case_headers",
    "Manifest",
    "run",
    "build_session",
]

__version__ = "1.1.0"
