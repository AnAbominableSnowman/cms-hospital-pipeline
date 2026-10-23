"""Tests for load, quality, config, CLI, retry setup, parallelism, and crash recovery."""

import sqlite3
import threading
import time

import pytest

from cms_pipeline import __main__ as cli
from cms_pipeline.config import Config, load_config
from cms_pipeline.load import connect, load_csv, table_name
from cms_pipeline.manifest import Manifest
from cms_pipeline.metadata import Dataset
from cms_pipeline.pipeline import build_session, run, safe_filename
from cms_pipeline.quality import DataQualityError, check_csv

from test_pipeline import META, FakeResponse, FakeSession, make_routes


def write(path, text):
    path.write_bytes(text.encode("utf-8"))
    return path


# ---------- quality ----------

def test_quality_accepts_good_file(tmp_path):
    stats = check_csv(write(tmp_path / "a.csv", "facility_id,30_day\n1,2\n3,4\n"))
    assert (stats.rows, stats.columns) == (2, ["facility_id", "30_day"])


@pytest.mark.parametrize(
    "content,msg",
    [
        ("", "empty"),
        ("a,b\n", "no data rows"),
        ("Facility ID,b\n1,2\n", "non-snake_case"),
        ("a,a\n1,2\n", "duplicate"),
        ("a,b\n1,2\n3\n", "row 2 has 1 fields"),
    ],
)
def test_quality_rejects_bad_files(tmp_path, content, msg):
    with pytest.raises(DataQualityError, match=msg):
        check_csv(write(tmp_path / "a.csv", content))


# ---------- load ----------

def test_load_replaces_table_idempotently(tmp_path):
    conn = connect(tmp_path / "db.sqlite")
    f = write(tmp_path / "t.csv", 'a,b\n1,"x, y"\n2,z\n')
    assert load_csv(conn, f, "t") == 2
    assert load_csv(conn, f, "t") == 2  # re-load must not append duplicates
    assert conn.execute("SELECT b FROM t ORDER BY a").fetchall() == [("x, y",), ("z",)]


def test_load_rolls_back_on_failure(tmp_path):
    conn = connect(tmp_path / "db.sqlite")
    load_csv(conn, write(tmp_path / "ok.csv", "a\n1\n"), "t")
    ragged = write(tmp_path / "bad.csv", "a,b\n1,2\n3\n")
    with pytest.raises(ValueError, match="1 fields, expected 2"):
        load_csv(conn, ragged, "t")
    assert conn.execute("SELECT a FROM t").fetchall() == [("1",)]  # old table intact


def test_table_name_is_sql_safe():
    assert table_name("output/Hospital Info__xubh-q36u.csv") == "hospital_info_xubh_q36u"
    assert table_name("9lives.csv") == "t_9lives"


# ---------- config ----------

def test_config_defaults_toml_and_overrides(tmp_path):
    assert load_config(None) == Config()
    p = tmp_path / "c.toml"
    p.write_text('workers = 3\ndb_path = ""\ntheme = "Hospitals"\n')
    cfg = load_config(p)
    assert (cfg.workers, cfg.db_path) == (3, None)
    assert cfg.with_overrides(workers=5, theme=None).workers == 5
    assert cfg.with_overrides(theme=None).theme == "Hospitals"  # None never overrides


def test_config_rejects_unknown_keys(tmp_path):
    p = tmp_path / "c.toml"
    p.write_text("wrkers = 3\n")
    with pytest.raises(ValueError, match="unknown config keys"):
        load_config(p)


# ---------- end to end with load ----------

def test_run_loads_sqlite_and_logs_rows(tmp_path):
    routes = make_routes([("h1", "Hospital Info", "1", "http://h1.csv")],
                         {"http://h1.csv": b"Facility ID,Score\n1,90\n2,80\n"})
    res = run(out_dir=tmp_path / "o", manifest_path=tmp_path / "s" / "m.json", metadata_url=META,
              db_path=tmp_path / "s" / "db.sqlite", session_factory=lambda: FakeSession(routes))
    assert res.ok and sum(res.rows.values()) == 2
    conn = sqlite3.connect(tmp_path / "s" / "db.sqlite")
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master")]
    assert tables == ["hospital_info_h1"]
    assert conn.execute("SELECT facility_id, score FROM hospital_info_h1 ORDER BY 1").fetchall() \
        == [("1", 90), ("2", 80)]  # id stays TEXT, score is INTEGER


def test_quality_failure_is_not_recorded_and_retried(tmp_path):
    routes = make_routes([("h1", "T", "1", "http://h1.csv")], {"http://h1.csv": b"A,B\n"})  # no rows
    kw = dict(out_dir=tmp_path / "o", manifest_path=tmp_path / "m.json", metadata_url=META)
    r1 = run(**kw, session_factory=lambda: FakeSession(routes))
    assert len(r1.failed) == 1 and Manifest(tmp_path / "m.json").load().as_dict() == {}
    routes["http://h1.csv"] = FakeResponse(body=b"A,B\n1,2\n")
    assert len(run(**kw, session_factory=lambda: FakeSession(routes)).processed) == 1


def test_manifest_saved_per_file_survives_a_crash(tmp_path, monkeypatch):
    """If the process dies after file 1 loads, file 1 must already be in the manifest."""
    items = [("a", "A", "1", "http://a.csv"), ("b", "B", "1", "http://b.csv")]
    routes = make_routes(items, {"http://a.csv": b"X\n1\n", "http://b.csv": b"X\n1\n"})
    saves = []
    real_save = Manifest.save

    def spy(self):
        saves.append(len(self.as_dict()))
        real_save(self)

    monkeypatch.setattr(Manifest, "save", spy)
    run(out_dir=tmp_path / "o", manifest_path=tmp_path / "m.json", metadata_url=META,
        workers=1, session_factory=lambda: FakeSession(routes))
    assert saves[:2] == [1, 2]  # saved after each file, not only once at the end


# ---------- concurrency ----------

def test_downloads_run_in_parallel(tmp_path):
    # Every download waits at a 4-party barrier. That only completes if 4 downloads
    # are in flight at once, so a serial pipeline fails instead of merely running slow.
    barrier = threading.Barrier(4, timeout=10)

    class GatedSession(FakeSession):
        def get(self, url, **kw):
            if url != META:
                barrier.wait()
            return super().get(url, **kw)

    items = [(f"d{i}", f"T{i}", "1", f"http://d{i}.csv") for i in range(8)]
    routes = make_routes(items, {f"http://d{i}.csv": b"A\n1\n" for i in range(8)})
    res = run(out_dir=tmp_path / "o", manifest_path=tmp_path / "m.json", metadata_url=META,
              workers=8, session_factory=lambda: GatedSession(routes))
    assert res.ok and len(res.processed) == 8


def test_workers_are_bounded(tmp_path):
    active, peak, guard = 0, 0, threading.Lock()

    class CountingSession(FakeSession):
        def get(self, url, **kw):
            nonlocal active, peak
            if url == META:
                return super().get(url, **kw)
            with guard:
                active += 1
                peak = max(peak, active)
            time.sleep(0.05)
            with guard:
                active -= 1
            return super().get(url, **kw)

    items = [(f"d{i}", f"T{i}", "1", f"http://d{i}.csv") for i in range(12)]
    routes = make_routes(items, {f"http://d{i}.csv": b"A\n1\n" for i in range(12)})
    res = run(out_dir=tmp_path / "o", manifest_path=tmp_path / "m.json", metadata_url=META,
              workers=3, session_factory=lambda: CountingSession(routes))
    assert res.ok and peak <= 3


def test_every_session_is_closed(tmp_path):
    sessions = []

    def factory():
        sessions.append(FakeSession(routes))
        return sessions[-1]

    items = [(f"d{i}", f"T{i}", "1", f"http://d{i}.csv") for i in range(3)]
    routes = make_routes(items, {f"http://d{i}.csv": b"A\n1\n" for i in range(3)})
    run(out_dir=tmp_path / "o", manifest_path=tmp_path / "m.json", metadata_url=META,
        session_factory=factory)
    assert len(sessions) == 4 and all(getattr(s, "closed", False) for s in sessions)


# ---------- resilience config / helpers ----------

def test_session_retries_transient_errors_with_backoff():
    session = build_session(retries=4, backoff=1.0)
    retry = session.get_adapter("https://data.cms.gov").max_retries
    assert retry.total == 4 and retry.backoff_factor == 1.0
    assert {429, 500, 502, 503, 504} <= set(retry.status_forcelist)
    assert retry.respect_retry_after_header


def test_safe_filename_is_deterministic_and_clean():
    ds = Dataset("abc-123", "Patients' Rating (HCAHPS) / Survey", "1", "http://x.csv")
    assert safe_filename(ds) == "patients_rating_hcahps_survey__abc-123.csv"


# ---------- CLI ----------

def test_cli_exit_codes(tmp_path, monkeypatch):
    class Result:
        processed, skipped, rows = ["a"], [], {"a": 3}
        failed = {}
        ok = True

    seen = {}

    def fake_run(**kw):
        seen.update(kw)
        return Result()

    monkeypatch.setattr(cli, "run", fake_run)
    cfg = tmp_path / "c.toml"
    cfg.write_text("workers = 2\n")
    assert cli.main(["--config", str(cfg), "--workers", "6", "--db", ""]) == 0
    assert seen["workers"] == 6 and seen["db_path"] is None  # flag beats file; "" skips load

    Result.failed, Result.ok = {"x": "boom"}, False
    assert cli.main([]) == 1

    def boom(**kw):
        raise RuntimeError("catalog down")

    monkeypatch.setattr(cli, "run", boom)
    assert cli.main([]) == 2
