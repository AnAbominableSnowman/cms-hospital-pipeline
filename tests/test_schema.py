"""Null markers, type inference, typed loading, and type-drift warnings."""

import sqlite3

import pytest

from cms_pipeline.drift import compare
from cms_pipeline.load import connect, load_csv
from cms_pipeline.pipeline import run
from cms_pipeline.quality import CsvStats, check_csv
from cms_pipeline.schema import DEFAULT_NULL_MARKERS, ColumnInferer, null_checker

from test_pipeline import META, FakeSession, make_routes


def infer(columns, rows, markers=DEFAULT_NULL_MARKERS):
    inf = ColumnInferer(columns, markers)
    for r in rows:
        inf.feed(r)
    return inf


# ---------- null markers ----------

@pytest.mark.parametrize("v", ["Not Available", "not available", "  N/A ", "Not Applicable",
                               "Too Few to Report", "", "   "])
def test_markers_and_blanks_are_null(v):
    assert null_checker(DEFAULT_NULL_MARKERS)(v)


@pytest.mark.parametrize("v", ["N/A (5)", "Not Available (1)", "not available (1, 3)", "N/A(5)"])
def test_marker_with_footnote_suffix_is_null(v):
    assert null_checker(DEFAULT_NULL_MARKERS)(v)


@pytest.mark.parametrize("v", ["5 (3)", "Low (2)", "(5)"])
def test_footnote_suffix_alone_does_not_make_a_value_null(v):
    assert not null_checker(DEFAULT_NULL_MARKERS)(v)


@pytest.mark.parametrize("v", ["0", "Low", "3,6", "NA", "None"])
def test_real_values_are_not_null(v):
    assert not null_checker(DEFAULT_NULL_MARKERS)(v)  # "NA" only counts if you configure it


# ---------- inference ----------

def test_integer_real_and_text_columns():
    inf = infer(["i", "r", "t"], [["1", "1.5", "x"], ["-2", "0.25", "y"], ["Not Available", "N/A", "z"]])
    assert inf.types == ["INTEGER", "REAL", "TEXT"]
    assert inf.nulls == 2


def test_one_decimal_makes_the_whole_column_real():
    assert infer(["v"], [["1"], ["2"], ["3.5"]]).types == ["REAL"]


def test_one_stray_text_value_keeps_the_column_text():
    assert infer(["v"], [["1"], ["2"], ["low"]]).types == ["TEXT"]


def test_all_null_column_is_text_not_integer():
    assert infer(["v"], [["Not Available"], [""]]).types == ["TEXT"]


@pytest.mark.parametrize("col", ["facility_id", "measure_id", "zip_code", "telephone_number",
                                 "score_footnote", "provider_ccn"])
def test_identifier_columns_are_always_text(col):
    assert infer([col], [["36301"], ["10001"]]).types == ["TEXT"]


def test_leading_zero_values_are_not_numbers():
    assert infer(["rank"], [["010001"], ["5"]]).types == ["TEXT"]
    assert infer(["x"], [["0.5"], ["0"], ["-0.1"]]).types == ["REAL"]  # 0.5 is fine


def test_thousands_separators_and_huge_ints():
    assert infer(["v"], [["1,234"]]).types == ["TEXT"]
    assert infer(["v"], [["99999999999999999999"]]).types == ["REAL"]  # beyond 64-bit INTEGER


def test_suspect_reports_near_numeric_text_columns():
    rows = [[str(i)] for i in range(19)] + [["3,6"]]
    inf = infer(["denominator"], rows)
    assert inf.types == ["TEXT"] and "denominator: 95% numeric" in inf.suspect[0]
    assert "3,6" in inf.suspect[0]


def test_suspect_ignores_forced_text_and_mostly_text_columns():
    assert infer(["x_footnote"], [["1"]] * 19 + [["3,6"]]).suspect == []
    assert infer(["name"], [["a"], ["b"], ["1"]]).suspect == []


def test_custom_markers_are_honoured():
    assert infer(["v"], [["1"], ["NA"]], markers=("NA",)).types == ["INTEGER"]


# ---------- quality + loading ----------

def write(path, text):
    path.write_bytes(text.encode("utf-8"))
    return path


def test_check_csv_reports_types_and_nulls(tmp_path):
    f = write(tmp_path / "a.csv", "facility_id,score,flag\n010001,4.5,x\n010002,Not Available,y\n")
    stats = check_csv(f)
    assert stats.types == ["TEXT", "REAL", "TEXT"] and stats.nulls == 1


def test_load_converts_types_and_nulls_and_averages_correctly(tmp_path):
    f = write(tmp_path / "a.csv",
              "facility_id,score,note\n010001,10,hi\n010002,Not Available,N/A\n010003,20,\n")
    conn = connect(tmp_path / "db.sqlite")
    stats = check_csv(f)
    load_csv(conn, f, "t", stats.types, DEFAULT_NULL_MARKERS)
    assert [r[1] for r in conn.execute("PRAGMA table_info(t)")] == ["facility_id", "score", "note"]
    assert [r[2] for r in conn.execute("PRAGMA table_info(t)")] == ["TEXT", "INTEGER", "TEXT"]
    rows = conn.execute("SELECT facility_id, score, note FROM t ORDER BY 1").fetchall()
    assert rows == [("010001", 10, "hi"), ("010002", None, None), ("010003", 20, None)]  # ids keep zeros
    assert conn.execute("SELECT AVG(score) FROM t").fetchone()[0] == 15.0  # NOT 10.0: no phantom zero


def test_load_without_markers_keeps_values_verbatim(tmp_path):
    f = write(tmp_path / "a.csv", "a,b\nNot Available,\n")
    conn = connect(tmp_path / "db.sqlite")
    load_csv(conn, f, "t")
    assert conn.execute("SELECT a, b FROM t").fetchone() == ("Not Available", "")


def test_run_loads_typed_tables_and_logs_nulls(tmp_path):
    routes = make_routes([("h1", "T", "1", "http://h1.csv")],
                         {"http://h1.csv": b"Facility ID,Score,Rate\n01,90,N/A\n02,Not Available,1.5\n"})
    res = run(out_dir=tmp_path / "o", manifest_path=tmp_path / "s" / "m.json", metadata_url=META,
              db_path=tmp_path / "s" / "db.sqlite", session_factory=lambda: FakeSession(routes))
    assert res.ok and sum(res.nulls.values()) == 2
    conn = sqlite3.connect(tmp_path / "s" / "db.sqlite")
    (table,) = [r[0] for r in conn.execute("SELECT name FROM sqlite_master")]
    assert [r[2] for r in conn.execute(f'PRAGMA table_info("{table}")')] == ["TEXT", "INTEGER", "REAL"]
    assert conn.execute(f'SELECT AVG(score), SUM(rate) FROM "{table}"').fetchone() == (90.0, 1.5)


def test_custom_null_markers_flow_through_run(tmp_path):
    routes = make_routes([("h1", "T", "1", "http://h1.csv")], {"http://h1.csv": b"Score\n5\nMISSING\n"})
    res = run(out_dir=tmp_path / "o", manifest_path=tmp_path / "m.json", metadata_url=META,
              db_path=tmp_path / "db.sqlite", null_markers=("MISSING",),
              session_factory=lambda: FakeSession(routes))
    assert res.ok and sum(res.nulls.values()) == 1


# ---------- type drift ----------

def test_type_flip_with_same_columns_is_a_warning():
    prev = {"rows": 10, "columns": ["a", "b"], "types": ["INTEGER", "TEXT"]}
    d = compare(prev, CsvStats(10, ["a", "b"], ["TEXT", "TEXT"]), 0.3)
    assert d.row_drop is None and "a INTEGER->TEXT" in d.warnings[0]


def test_same_types_and_missing_old_types_do_not_warn():
    now = CsvStats(10, ["a"], ["INTEGER"])
    assert compare({"rows": 10, "columns": ["a"], "types": ["INTEGER"]}, now, 0.3).warnings == []
    assert compare({"rows": 10, "columns": ["a"]}, now, 0.3).warnings == []  # pre-typing manifest
