"""Checks against the real CMS API and the real loaded database.

Skipped by default (they need the network or a prior full run). Run them with:

    python -m pytest -m live
"""

import sqlite3
import sys
from pathlib import Path

import pytest

from cms_pipeline.metadata import fetch_datasets
from cms_pipeline.pipeline import build_session
from cms_pipeline.transform import snake_case_headers

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "state" / "hospitals.db"

pytestmark = pytest.mark.live


def test_catalog_lists_hospital_csvs_with_dates():
    with build_session() as session:
        datasets = fetch_datasets(session)
    assert len(datasets) >= 50  # 75 as of Oct 2026
    assert all(d.download_url.lower().split("?")[0].endswith(".csv") for d in datasets)
    assert all(d.modified for d in datasets)
    assert len({d.key for d in datasets}) == len(datasets)


def test_a_real_header_converts_cleanly():
    with build_session() as session:
        ds = fetch_datasets(session)[0]
        with session.get(ds.download_url, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            line = next(resp.iter_lines()).decode("utf-8-sig")
    names = snake_case_headers(line.split(","))
    assert len(set(names)) == len(names)
    assert all(n == n.lower() and " " not in n for n in names)


@pytest.mark.skipif(not DB.exists(), reason="needs state/hospitals.db from a full run")
def test_every_example_query_runs_and_returns_rows():
    sys.path.insert(0, str(ROOT))
    from run_queries import statements

    conn = sqlite3.connect(DB)
    try:
        for name, stmt in statements((ROOT / "queries.sql").read_text(encoding="utf-8")):
            cur = conn.execute(stmt)
            if cur.description is not None:
                assert cur.fetchall(), f"query {name!r} returned no rows"
        conn.commit()
    finally:
        conn.close()
