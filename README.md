# CMS Hospital Data Pipeline

A Python job that downloads every dataset in the CMS Provider Data Catalog tagged
**Hospitals**, converts each CSV's column names to `snake_case`, and loads the result
into SQLite. It is meant to run daily: each run downloads only the files that changed
since the last successful run. It is plain Python with one third-party dependency
(`requests`) and runs on Windows, Linux or macOS.

| Requirement | How it's met |
|---|---|
| Find all "Hospitals" datasets | Reads the [metastore API](https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items) and keeps every dataset whose themes include `Hospitals` (case-insensitive), one entry per CSV file (`metadata.py`) |
| snake_case column names | Only the header row is rewritten; data rows pass through byte for byte (`transform.py`) |
| Parallel download and processing | A bounded thread pool downloads and validates files concurrently; one file failing doesn't affect the others (`pipeline.py`) |
| Daily, changed files only | Each file's `modified` date is recorded after it succeeds and compared on the next run (`manifest.py`); see [Scheduling](#scheduling) |
| Runs on a regular machine | Python 3.11+, standard library plus `requests` (`requirements.txt`); no cloud services |

## Quick start
Needs Python 3.11 or newer and internet access. Open a terminal in this folder (the one
containing `pyproject.toml`) and paste the block for your system.

**Windows** (PowerShell or Command Prompt):
```bat
py -m venv .venv
.venv\Scripts\python -m pip install -e ".[dev]"
.venv\Scripts\python -m cms_pipeline -v
.venv\Scripts\python -m cms_pipeline
```

**Linux / macOS:**
```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e ".[dev]"
.venv/bin/python -m cms_pipeline -v
.venv/bin/python -m cms_pipeline
```
The first run downloads everything (about a minute); the second finds nothing changed
and finishes in about a second. If `py` isn't found on Windows, use `python` instead.
The commands in the rest of this README use the Linux/macOS path; on Windows replace
`.venv/bin/python` with `.venv\Scripts\python`.

The first run creates `output/` (the CSVs, about 280 MB), `state/hospitals.db` (SQLite)
and `state/manifest.json`. `sample_output/` holds the header and first 6 rows of all 75
output files (national summary files with fewer rows are included whole), plus the run
log from a first and a second run.

## How a run works
1. **Lock.** Takes an OS-level lock on `state/run.lock`. If another run holds it, this
   run exits with code 3 without touching anything.
2. **Catalog.** Fetches the catalog and selects the CSV files of every `Hospitals` dataset
   (75 as of October 2026).
3. **Select.** Skips each file whose catalog `modified` date matches the one recorded in
   `state/manifest.json`. Files never seen before, or whose date changed, are queued.
4. **Download and transform, in parallel.** Each file is streamed to `<name>.csv.part`.
   Only the header line is buffered and rewritten to `snake_case`; everything after it
   is copied through unchanged.
5. **Validate.** The `.part` file must have unique snake_case headers, at least one row,
   and the same number of fields on every row. It is also compared with the previous
   version: a row count that falls by more than 30% blocks the file (the usual sign of
   a truncated download), and a schema or column-type change is logged as a warning.
   Only a file that passes replaces the previous CSV, so `output/` always holds the
   last good version.
6. **Load.** Each file is loaded into its own SQLite table in a single transaction
   (drop and recreate), so a failed load rolls back to the previous table.
7. **Record.** The file's `modified` date, row count, columns and types are written to
   the manifest immediately, so an interrupted run keeps the work it finished. A failed
   file is not recorded, so the next run retries it. One line per run is appended to
   `state/runs.jsonl`.

## Column names
| Source header | Output |
|---|---|
| `Patients’ rating of the facility linear mean score` | `patients_rating_of_the_facility_linear_mean_score` |
| `Facility ID` / `FacilityID` | `facility_id` |
| `Payment (per %)` | `payment_per` |
| `30 Day Rate` | `30_day_rate` |
| `Score`, `score` in the same file | `score`, `score_1` |

Apostrophes (straight or curly) are dropped and accented letters lose their accents.
CamelCase is split into words, and any other run of spaces or punctuation becomes a
single `_`. A UTF-8 byte-order mark and the file's line endings are preserved. A file
that isn't UTF-8 is converted from Windows-1252; CMS doesn't currently publish any.

## Scheduling
The job is safe to run as often as you like: unchanged files are skipped, and
overlapping runs are refused. [schedule/README.md](schedule/README.md) has ready-to-use
cron (Linux/macOS) and Task Scheduler (Windows) setups. Exit codes:

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Some files failed; they are retried on the next run |
| 2 | Run aborted: catalog unreachable or invalid config |
| 3 | Not started: another run is in progress |

## Configuration
Defaults work out of the box. To change them, pass `--config config.toml` (every key
is optional); command-line flags override the file.

| Key / flag | Default | Purpose |
|---|---|---|
| `out_dir` / `--out-dir` | `output` | Where the CSVs go |
| `manifest_path` / `--manifest` | `state/manifest.json` | Run state; the lock and run log sit next to it |
| `db_path` / `--db` | `state/hospitals.db` | SQLite file; `""` skips loading |
| `theme` / `--theme` | `Hospitals` | Catalog theme to download |
| `workers` / `--workers` | `8` | Parallel downloads |
| `max_row_drop` | `0.3` | Block a file whose row count falls by more than this fraction |
| `null_markers` | see `config.toml` | Placeholder text loaded as SQL NULL |
| `--force` | | Re-download everything and override the row-drop block |
| `-v` | | Debug logging |

## Loading into SQLite
Each CSV becomes a table named after its file (for example
`hospital_general_information_xubh_q36u`). Placeholder values such as
`Not Available`, `N/A`, `Not Applicable`, `Too Few to Report`, blanks, and forms like
`N/A (5)` are loaded as NULL. A column is typed INTEGER or REAL only if every remaining
value is a plain number. Identifier-like columns (ids, ZIP codes, phone numbers,
footnotes) and values with leading zeros stay TEXT. Dates stay TEXT as published
(`MM/DD/YYYY`). The CSVs in `output/` are never altered by this step.

A column that is mostly numeric but had to stay TEXT is reported as a `typing:`
warning in `state/runs.jsonl`, so unexpected placeholder values are visible rather
than silently blocking a numeric type. In the current data this flags a
`$14,126,447.33` in a count column and `very high`/`high`/`low` in a `score` column.

`python run_queries.py` runs the example analysis in `queries.sql`: average star rating
by state, the highest 30-day pneumonia readmission rates, readmissions by ownership
type, and how well each measure is populated.

## Reliability
- **Retries:** HTTP 429 and 5xx responses are retried with exponential backoff, and
  every request has a timeout. The catalog endpoint can be slow (90 s has been seen),
  so it gets 180 s.
- **Failure isolation:** a failed file is logged and skipped. Its siblings still finish
  and are recorded.
- **Atomic writes:** CSVs and the manifest are written to a temporary file and moved
  into place, so a crash never leaves a half-written file that looks complete.
- **Locking:** the run lock is held by the operating system (`flock` on Linux/macOS,
  `msvcrt.locking` on Windows) and released when the process exits, even if it is
  killed, so a crashed run never blocks the next one.
- **Change detection:** files are matched across runs by dataset identifier plus file
  name, because CMS download URLs contain a hash directory that changes when a file is
  republished.

## Tests
```bash
.venv/bin/python -m pytest            # 115 offline tests, about 2 s
.venv/bin/python -m pytest -m live    # 3 tests against the real API and state/hospitals.db
```
The offline suite replaces HTTP with an in-memory fake (`session_factory`). It covers:
- header conversion and encodings
- incremental and forced runs, retries after failure, and crash recovery
- parallelism and the worker limit
- the run lock, including a killed holder and a race between threads
- the quality and drift checks
- SQLite loading and type inference
- configuration and the CLI

## Project layout
```
src/cms_pipeline/
  __main__.py   command line and exit codes
  config.py     config.toml + flag handling
  metadata.py   catalog fetch and theme filter
  pipeline.py   run orchestration, streaming download, header rewrite
  transform.py  snake_case conversion
  quality.py    per-file validation
  drift.py      comparison with the previous run
  schema.py     null markers and type inference
  load.py       SQLite loading
  manifest.py   per-file run state
  lock.py       single-run lock
tests/          offline tests (+ test_live.py, opt-in)
schedule/       cron and Task Scheduler setup
sample_output/  first 6 rows of every output CSV, and the run log
queries.sql, run_queries.py   example analysis
make_sample.py  rebuilds sample_output/ from output/
```

## Known limitations
- **Change detection is by date.** The catalog's `modified` field is a date, so two
  republishes on the same day count as one.
- **Removed datasets leave files behind.** When a dataset leaves the catalog, its
  manifest entry is dropped, but its CSV and SQLite table stay on disk. A retitled
  dataset also leaves its old file, because the filename includes the title.
- **Multi-line headers aren't supported.** A header containing a quoted line break
  would be split at the first line break. CMS headers don't contain any.
- **NULL hides which placeholder was used.** All placeholder values become NULL, so
  "not available" and "not applicable" can only be told apart from the footnote
  columns or the raw CSV.
- **SQLite is the only load target.** Postgres or Parquet output would be the natural
  next step.
