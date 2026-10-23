import json

import pytest

from cms_pipeline.manifest import Manifest
from cms_pipeline.metadata import Dataset, fetch_datasets
from cms_pipeline.pipeline import run
from cms_pipeline.pipeline import safe_filename
from cms_pipeline.transform import snake_case_headers, to_snake_case


# ---------- transform ----------

@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Facility ID", "facility_id"),
        ("FacilityID", "facility_id"),
        ("HCAHPS Score", "hcahps_score"),
        ("Payment (per %)", "payment_per"),
        ("Hospital Overall Rating", "hospital_overall_rating"),
        ("30 Day Rate", "30_day_rate"),
        ("Résumé Score", "resume_score"),
        ("  padded  ", "padded"),
        ("a/b-c", "a_b_c"),
        ("???", "column"),
        ("", "column"),
        ("already_snake", "already_snake"),
        # Example from the specification (note the curly apostrophe).
        ("Patients’ rating of the facility linear mean score",
         "patients_rating_of_the_facility_linear_mean_score"),
        # Apostrophes inside a word are dropped, not turned into "_".
        ("Hospital’s overall rating", "hospitals_overall_rating"),
        ("Doctor's Name", "doctors_name"),
    ],
)
def test_to_snake_case(raw, expected):
    assert to_snake_case(raw) == expected


def test_duplicate_headers_are_disambiguated():
    assert snake_case_headers(["Score", "score", "SCORE"]) == ["score", "score_1", "score_2"]


def test_suffix_does_not_clobber_existing_name():
    assert snake_case_headers(["foo", "foo_1", "foo"]) == ["foo", "foo_1", "foo_2"]


# ---------- manifest ----------

def test_manifest_roundtrip_and_modified(tmp_path):
    m = Manifest(tmp_path / "m.json").load()
    assert m.is_modified("k", "2024-01-01")
    m.update("k", "2024-01-01")
    m.save()
    m2 = Manifest(tmp_path / "m.json").load()
    assert not m2.is_modified("k", "2024-01-01")
    assert m2.is_modified("k", "2024-02-01")


def test_corrupt_manifest_is_treated_as_empty(tmp_path):
    p = tmp_path / "m.json"
    p.write_text("{not json")
    assert Manifest(p).load().as_dict() == {}


# ---------- fakes ----------

class FakeResponse:
    def __init__(self, payload=None, body=b"", status=200):
        self._payload, self._body, self.status = payload, body, status

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    def json(self):
        return self._payload

    def iter_content(self, chunk_size=1):
        # Tiny chunks prove the header can straddle chunk boundaries.
        for i in range(0, len(self._body), 7):
            yield self._body[i : i + 7]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeSession:
    def __init__(self, routes):
        self.routes = routes
        self.calls = []

    def get(self, url, **kw):
        self.calls.append(url)
        r = self.routes[url]
        if isinstance(r, Exception):
            raise r
        return r

    # requests.Session is a context manager; the pipeline closes every session it opens.
    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.closed = True
        return False


def catalog(*items):
    return [
        {
            "identifier": i,
            "title": t,
            "modified": m,
            "theme": ["Hospitals"],
            "distribution": [{"downloadURL": u, "mediaType": "text/csv"}],
        }
        for i, t, m, u in items
    ]


META = "http://meta"


def make_routes(items, bodies):
    routes = {META: FakeResponse(payload=catalog(*items))}
    for url, body in bodies.items():
        routes[url] = FakeResponse(body=body)
    return routes


# ---------- metadata ----------

def test_fetch_datasets_filters_theme_and_non_csv():
    payload = catalog(("a", "A", "1", "http://a.csv"))
    payload.append(
        {"identifier": "x", "title": "X", "modified": "1", "theme": ["Other"],
         "distribution": [{"downloadURL": "http://x.csv", "mediaType": "text/csv"}]}
    )
    payload.append(
        {"identifier": "j", "title": "J", "modified": "1", "theme": ["Hospitals"],
         "distribution": [{"downloadURL": "http://j.json", "mediaType": "application/json"}]}
    )
    ds = fetch_datasets(FakeSession({META: FakeResponse(payload=payload)}), url=META)
    assert [d.identifier for d in ds] == ["a"]


def test_theme_match_ignores_case_and_checks_every_theme():
    payload = [
        {"identifier": i, "title": i, "modified": "1", "theme": themes,
         "distribution": [{"downloadURL": f"http://{i}.csv", "mediaType": "text/csv"}]}
        for i, themes in [("a", [" hospitals "]), ("b", ["Other", "Hospitals"]),
                          ("c", "HOSPITALS"), ("d", ["Long-term care hospitals"])]
    ]
    ds = fetch_datasets(FakeSession({META: FakeResponse(payload=payload)}), url=META)
    assert [d.identifier for d in ds] == ["a", "b", "c"]


def test_same_titled_distributions_get_distinct_output_files():
    payload = [{
        "identifier": "m", "title": "Multi", "modified": "1", "theme": ["Hospitals"],
        "distribution": [{"downloadURL": "http://x/r1/Part_A.csv", "mediaType": "text/csv"},
                         {"downloadURL": "http://x/r2/Part_B.csv", "mediaType": "text/csv"}],
    }]
    ds = fetch_datasets(FakeSession({META: FakeResponse(payload=payload)}), url=META)
    assert [safe_filename(d) for d in ds] == ["multi_part_a__m.csv", "multi_part_b__m.csv"]


# ---------- end to end ----------

def test_run_transforms_headers_and_preserves_rows(tmp_path):
    body = b"Facility ID,Hospital Name,Score (%)\r\n1,\"Smith, Mary\",90\r\n2,B,80\r\n"
    routes = make_routes([("h1", "Hospital Info", "2024-01-01", "http://h1.csv")],
                         {"http://h1.csv": body})
    res = run(out_dir=tmp_path / "out", manifest_path=tmp_path / "m.json",
              metadata_url=META, workers=2, session_factory=lambda: FakeSession(routes))
    assert res.ok and len(res.processed) == 1
    out = (tmp_path / "out").glob("*.csv")
    text = next(out).read_bytes()
    assert text.startswith(b"facility_id,hospital_name,score\r\n")
    assert b"1,\"Smith, Mary\",90\r\n2,B,80\r\n" in text


def _single(tmp_path, body):
    routes = make_routes([("h1", "T", "1", "http://h1.csv")], {"http://h1.csv": body})
    res = run(out_dir=tmp_path / "out", manifest_path=tmp_path / "m.json",
              metadata_url=META, session_factory=lambda: FakeSession(routes))
    return res, tmp_path / "out" / "t__h1.csv"


def test_utf8_bom_is_kept_and_header_still_converted(tmp_path):
    res, out = _single(tmp_path, b"\xef\xbb\xbfFacility ID,Patients\xe2\x80\x99 Rating\r\n1,2\r\n")
    assert res.ok
    assert out.read_bytes() == b"\xef\xbb\xbffacility_id,patients_rating\r\n1,2\r\n"


def test_cp1252_file_is_transcoded_to_utf8(tmp_path):
    # 0x92 is a curly apostrophe in cp1252 and is invalid as UTF-8.
    res, out = _single(tmp_path, b"Hospital\x92s Rating,City\n5,Bayam\xf3n\n")
    assert res.ok
    assert out.read_bytes() == "hospitals_rating,city\n5,Bayamón\n".encode("utf-8")


def test_header_split_across_many_chunks(tmp_path):
    # FakeResponse yields 7-byte chunks, so this header arrives in dozens of pieces.
    header = ",".join(f"Column Number {i}" for i in range(20))
    res, out = _single(tmp_path, (header + "\n" + ",".join("1" * 20) + "\n").encode())
    assert res.ok
    first = out.read_text().splitlines()[0]
    assert first == ",".join(f"column_number_{i}" for i in range(20))


def test_second_run_skips_unchanged_and_reprocesses_modified(tmp_path):
    def routes(modified):
        return make_routes([("h1", "T", modified, "http://h1.csv")],
                           {"http://h1.csv": b"A B,C\n1,2\n"})

    kw = dict(out_dir=tmp_path / "out", manifest_path=tmp_path / "m.json",
              metadata_url=META)
    r1 = run(**kw, session_factory=lambda: FakeSession(routes("v1")))
    r2 = run(**kw, session_factory=lambda: FakeSession(routes("v1")))
    r3 = run(**kw, session_factory=lambda: FakeSession(routes("v2")))
    r4 = run(**kw, force=True, session_factory=lambda: FakeSession(routes("v2")))
    assert (len(r1.processed), len(r2.processed), len(r2.skipped)) == (1, 0, 1)
    assert len(r3.processed) == 1
    assert len(r4.processed) == 1


def test_failure_is_isolated_and_retried_next_run(tmp_path):
    items = [("ok", "Good", "1", "http://ok.csv"), ("bad", "Bad", "1", "http://bad.csv")]
    routes = make_routes(items, {"http://ok.csv": b"A,B\n1,2\n"})
    routes["http://bad.csv"] = FakeResponse(status=500)
    kw = dict(out_dir=tmp_path / "out", manifest_path=tmp_path / "m.json", metadata_url=META)

    r1 = run(**kw, session_factory=lambda: FakeSession(routes))
    assert len(r1.processed) == 1 and len(r1.failed) == 1
    assert not list((tmp_path / "out").glob("*.part"))  # no partial files left

    routes["http://bad.csv"] = FakeResponse(body=b"X,Y\n1,2\n")
    r2 = run(**kw, session_factory=lambda: FakeSession(routes))
    assert len(r2.processed) == 1 and r2.ok  # only the failed one reran
    saved = json.loads((tmp_path / "m.json").read_text())
    assert len(saved) == 2


def test_run_appends_run_history(tmp_path):
    routes = make_routes([("h1", "T", "1", "http://h1.csv")], {"http://h1.csv": b"A,B\n1,2\n"})
    kw = dict(out_dir=tmp_path / "out", manifest_path=tmp_path / "state" / "m.json",
              metadata_url=META, session_factory=lambda: FakeSession(routes))
    run(**kw)
    run(**kw)
    lines = (tmp_path / "state" / "runs.jsonl").read_text().splitlines()
    recs = [json.loads(l) for l in lines]
    assert [(r["processed"], r["skipped"]) for r in recs] == [(1, 0), (0, 1)]


def test_catalog_outage_is_logged_and_raised(tmp_path):
    routes = {META: RuntimeError("catalog down")}
    with pytest.raises(RuntimeError):
        run(out_dir=tmp_path / "out", manifest_path=tmp_path / "state" / "m.json",
            metadata_url=META, session_factory=lambda: FakeSession(routes))
    rec = json.loads((tmp_path / "state" / "runs.jsonl").read_text().splitlines()[0])
    assert "catalog" in rec["failed"]


def test_empty_body_is_a_failure(tmp_path):
    routes = make_routes([("e", "Empty", "1", "http://e.csv")], {"http://e.csv": b""})
    res = run(out_dir=tmp_path / "out", manifest_path=tmp_path / "m.json",
              metadata_url=META, session_factory=lambda: FakeSession(routes))
    assert len(res.failed) == 1
