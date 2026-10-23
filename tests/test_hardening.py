"""Stable manifest keys, manifest records/pruning, run lock, and drift detection."""

import json
import subprocess
import sys
import threading
import time

import pytest

from cms_pipeline import __main__ as cli
from cms_pipeline.drift import compare
from cms_pipeline.lock import LockHeldError, RunLock
from cms_pipeline.manifest import Manifest
from cms_pipeline.metadata import Dataset
from cms_pipeline.pipeline import run
from cms_pipeline.quality import CsvStats

from test_pipeline import META, FakeResponse, FakeSession, catalog, make_routes


# ---------- stable key ----------

def test_key_survives_a_republish_that_changes_the_url_directory():
    old = Dataset("d1", "T", "1", "https://x/resources/aaa_1785189972/Name.csv")
    new = Dataset("d1", "T", "2", "https://x/resources/bbb_1790000000/Name.csv")
    assert old.key == new.key == "d1::Name.csv"


def test_key_ignores_query_string_and_distinguishes_files():
    a = Dataset("d1", "T", "1", "https://x/r/A.csv?v=1")
    b = Dataset("d1", "T", "1", "https://x/r/B.csv")
    assert a.key == "d1::A.csv" and a.key != b.key


def test_republished_dataset_is_reprocessed_not_duplicated(tmp_path):
    """Same dataset, new URL directory + new `modified`: one manifest entry, reprocessed."""
    def routes(directory, modified):
        url = f"http://x/resources/{directory}/Name.csv"
        return make_routes([("d1", "T", modified, url)], {url: b"A\n1\n"})

    kw = dict(out_dir=tmp_path / "o", manifest_path=tmp_path / "m.json", metadata_url=META)
    run(**kw, session_factory=lambda: FakeSession(routes("aaa_1", "v1")))
    r2 = run(**kw, session_factory=lambda: FakeSession(routes("bbb_2", "v2")))
    assert len(r2.processed) == 1 and not r2.skipped
    assert list(json.loads((tmp_path / "m.json").read_text())) == ["d1::Name.csv"]


# ---------- manifest ----------

def test_manifest_stores_rows_and_columns_and_reads_legacy_format(tmp_path):
    m = Manifest(tmp_path / "m.json")
    m.update("k", "v1", rows=5, columns=["a", "b"])
    m.save()
    assert Manifest(tmp_path / "m.json").load().get("k") == {"modified": "v1", "rows": 5, "columns": ["a", "b"], "types": None}
    (tmp_path / "old.json").write_text('{"k": "2024-01-01"}')  # pre-records format
    legacy = Manifest(tmp_path / "old.json").load()
    assert not legacy.is_modified("k", "2024-01-01") and legacy.get("k")["modified"] == "2024-01-01"


def test_manifest_prune(tmp_path):
    m = Manifest(tmp_path / "m.json")
    for k in ("a", "b", "c"):
        m.update(k, "1")
    assert sorted(m.prune(["a", "c"])) == ["b"] and sorted(m.as_dict()) == ["a", "c"]


def test_run_prunes_datasets_that_left_the_catalog(tmp_path):
    kw = dict(out_dir=tmp_path / "o", manifest_path=tmp_path / "m.json", metadata_url=META)
    two = make_routes([("a", "A", "1", "http://a.csv"), ("b", "B", "1", "http://b.csv")],
                      {"http://a.csv": b"X\n1\n", "http://b.csv": b"X\n1\n"})
    one = make_routes([("a", "A", "1", "http://a.csv")], {"http://a.csv": b"X\n1\n"})
    run(**kw, session_factory=lambda: FakeSession(two))
    res = run(**kw, session_factory=lambda: FakeSession(one))
    assert res.pruned == ["b::b.csv"]
    assert list(json.loads((tmp_path / "m.json").read_text())) == ["a::a.csv"]


# ---------- lock ----------

def test_lock_blocks_a_second_holder_and_releases(tmp_path):
    path = tmp_path / "run.lock"
    with RunLock(path):
        with pytest.raises(LockHeldError):
            RunLock(path).acquire()
    with RunLock(path):  # reusable after release
        pass


def test_lock_released_when_the_run_raises(tmp_path):
    path = tmp_path / "run.lock"
    with pytest.raises(RuntimeError):
        with RunLock(path):
            raise RuntimeError("boom")
    with RunLock(path):
        pass


def test_leftover_lock_file_from_a_crashed_run_does_not_block(tmp_path):
    # A killed run leaves the file behind, but the OS dropped its lock with the process.
    path = tmp_path / "run.lock"
    path.write_text("pid=1")
    with RunLock(path):
        pass


def test_lock_released_when_the_holding_process_dies(tmp_path):
    path = tmp_path / "run.lock"
    code = ("import sys; from cms_pipeline.lock import RunLock; "
            "RunLock(sys.argv[1]).acquire(); print('held', flush=True); sys.stdin.read()")
    child = subprocess.Popen([sys.executable, "-c", code, str(path)],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == "held"
        with pytest.raises(LockHeldError):
            RunLock(path).acquire()
    finally:
        child.kill()  # no clean release: the lock must go with the process
        child.wait()
    # Windows frees a dead process's locks shortly after exit, not synchronously.
    deadline = time.monotonic() + 10
    while True:
        try:
            with RunLock(path):
                break
        except LockHeldError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.05)


def test_racing_lock_attempts_have_exactly_one_winner(tmp_path):
    path = tmp_path / "run.lock"
    start, hold = threading.Barrier(8), threading.Event()
    won, lost = [], []

    def attempt():
        lock = RunLock(path)
        start.wait()
        try:
            lock.acquire()
        except LockHeldError:
            lost.append(1)
            return
        won.append(1)
        hold.wait(5)
        lock.release()

    threads = [threading.Thread(target=attempt) for _ in range(8)]
    for t in threads:
        t.start()
    deadline = time.monotonic() + 5
    while len(won) + len(lost) < 8 and time.monotonic() < deadline:
        time.sleep(0.01)
    hold.set()
    for t in threads:
        t.join()
    assert (len(won), len(lost)) == (1, 7)


def test_run_refuses_to_start_while_locked_and_cli_exits_3(tmp_path):
    routes = make_routes([("a", "A", "1", "http://a.csv")], {"http://a.csv": b"X\n1\n"})
    with RunLock(tmp_path / "run.lock"):
        with pytest.raises(LockHeldError):
            run(out_dir=tmp_path / "o", manifest_path=tmp_path / "m.json", metadata_url=META,
                session_factory=lambda: FakeSession(routes))
        assert cli.main(["--manifest", str(tmp_path / "m.json"), "--out-dir",
                         str(tmp_path / "o"), "--db", ""]) == 3


# ---------- drift ----------

def test_compare_first_run_and_stable_file_have_no_findings():
    now = CsvStats(rows=100, columns=["a", "b"])
    assert compare(None, now, 0.3).warnings == [] and compare(None, now, 0.3).row_drop is None
    same = {"rows": 100, "columns": ["a", "b"]}
    d = compare(same, now, 0.3)
    assert d.warnings == [] and d.row_drop is None


def test_compare_flags_schema_change_as_warning_only():
    d = compare({"rows": 10, "columns": ["a", "b"]}, CsvStats(10, ["a", "c"]), 0.3)
    assert "+['c'] -['b']" in d.warnings[0] and d.row_drop is None


@pytest.mark.parametrize("new_rows,blocked", [(70, False), (69, True), (0, True), (500, False)])
def test_compare_row_drop_threshold(new_rows, blocked):
    d = compare({"rows": 100, "columns": ["a"]}, CsvStats(new_rows, ["a"]), 0.3)
    assert (d.row_drop is not None) is blocked


def _two_versions(tmp_path, first, second, **extra):
    def routes(body, modified):
        return make_routes([("d", "T", modified, "http://d.csv")], {"http://d.csv": body})

    kw = dict(out_dir=tmp_path / "o", manifest_path=tmp_path / "m.json", metadata_url=META)
    run(**kw, session_factory=lambda: FakeSession(routes(first, "v1")))
    return run(**kw, **extra, session_factory=lambda: FakeSession(routes(second, "v2")))


BIG = b"A\n" + b"1\n" * 100
SMALL = b"A\n" + b"1\n" * 10


def test_truncated_file_is_blocked_and_old_state_kept(tmp_path):
    res = _two_versions(tmp_path, BIG, SMALL)
    assert list(res.failed) == ["d::d.csv"] and "row count fell 100 -> 10" in res.failed["d::d.csv"]
    rec = Manifest(tmp_path / "m.json").load().get("d::d.csv")
    assert rec["modified"] == "v1" and rec["rows"] == 100  # unchanged, so it retries next run
    # The rejected file never replaced the last good CSV, and left no .part behind.
    assert (tmp_path / "o" / "t__d.csv").read_bytes() == BIG.replace(b"A\n", b"a\n", 1)
    assert not list((tmp_path / "o").glob("*.part"))


def test_file_failing_quality_check_keeps_previous_csv(tmp_path):
    res = _two_versions(tmp_path, b"A\n1\n", b"A,B\n1\n")  # v2 has a ragged row
    assert list(res.failed) == ["d::d.csv"]
    assert (tmp_path / "o" / "t__d.csv").read_bytes() == b"a\n1\n"


def test_crash_after_csv_replaced_but_before_manifest_saved_is_redone(tmp_path, monkeypatch):
    routes = make_routes([("d", "T", "v1", "http://d.csv")], {"http://d.csv": b"A\n1\n"})
    kw = dict(out_dir=tmp_path / "o", manifest_path=tmp_path / "m.json", metadata_url=META,
              session_factory=lambda: FakeSession(routes))

    def killed(self):
        raise KeyboardInterrupt("killed")

    with monkeypatch.context() as m:
        m.setattr(Manifest, "save", killed)
        with pytest.raises(KeyboardInterrupt):
            run(**kw)
    assert (tmp_path / "o" / "t__d.csv").exists() and not (tmp_path / "m.json").exists()
    res = run(**kw)  # the lock was released, and the unrecorded file is processed again
    assert res.processed == ["d::d.csv"]
    assert Manifest(tmp_path / "m.json").load().get("d::d.csv")["modified"] == "v1"


def test_force_overrides_the_row_drop_block_but_records_a_warning(tmp_path):
    res = _two_versions(tmp_path, BIG, SMALL, force=True)
    assert res.ok and "forced past" in res.warnings["d::d.csv"][0]


def test_schema_change_loads_with_a_warning_in_the_run_log(tmp_path):
    res = _two_versions(tmp_path, b"A,B\n1,2\n", b"A,C\n1,2\n")
    assert res.ok and "schema changed" in res.warnings["d::d.csv"][0]
    last = json.loads((tmp_path / "m.json").with_name("runs.jsonl").read_text().splitlines()[-1])
    assert "d::d.csv" in last["warnings"]
