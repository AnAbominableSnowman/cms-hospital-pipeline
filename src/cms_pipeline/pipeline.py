"""Orchestration: fetch catalog -> filter changed -> download + transform -> record.

Design notes
  * Incremental: only datasets whose `modified` differs from the manifest run.
  * Concurrent: a thread pool downloads files in parallel (I/O bound).
  * Streaming: the body is never held in memory. We buffer only until the header
    line is complete, rewrite it, then copy the remaining bytes through as-is
    (transcoded to UTF-8 only if the source turns out to be cp1252).
  * Atomic, gated output: write to `<name>.part`, run the quality and drift checks
    on it, and only then os.replace it into place. A crash or a failed check never
    leaves a truncated or rejected CSV where the last good one was.
  * Fault isolation: one failing dataset is logged and skipped. Its manifest
    entry is NOT updated, so the next run retries it. Successful siblings are
    still recorded.
"""

from __future__ import annotations

import codecs
import csv
import io
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import load as sqlite_load
from .drift import compare as compare_drift
from .lock import RunLock
from .manifest import Manifest
from .metadata import DEFAULT_METADATA_URL, DEFAULT_THEME, Dataset, fetch_datasets
from .quality import CsvStats, DataQualityError, check_csv
from .schema import DEFAULT_NULL_MARKERS
from .transform import snake_case_headers

log = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 256
MAX_TITLE_CHARS = 60  # title part of an output filename
_UTF8_BOM = b"\xef\xbb\xbf"
_FALLBACK_ENCODING = "cp1252"  # what a non-UTF-8 CSV exported from Excel/Windows almost always is


@dataclass
class RunResult:
    processed: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    failed: Dict[str, str] = field(default_factory=dict)
    rows: Dict[str, int] = field(default_factory=dict)
    warnings: Dict[str, List[str]] = field(default_factory=dict)
    pruned: List[str] = field(default_factory=list)
    nulls: Dict[str, int] = field(default_factory=dict)  # cells loaded as NULL, per dataset

    @property
    def ok(self) -> bool:
        return not self.failed


def build_session(retries: int = 4, backoff: float = 1.0) -> requests.Session:
    """A Session that retries transient failures with exponential backoff."""
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=16)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def safe_filename(dataset: Dataset) -> str:
    """Filesystem-safe, deterministic output name for a dataset's CSV.

    The title part is capped so the full path stays well under Windows' default
    260-character limit (some CMS titles are ~170 characters). The identifier keeps
    names unique after truncation.
    """
    stem = re.sub(r"[^0-9a-zA-Z]+", "_", dataset.title).lower()
    stem = stem.strip("_")[:MAX_TITLE_CHARS].rstrip("_")
    stem = stem or "dataset"
    return f"{stem}__{dataset.identifier}.csv"


def _rewrite_header_line(raw_header: bytes) -> Tuple[bytes, str]:
    """Snake_case one raw CSV header line (newline kept).

    Returns the new header as UTF-8 bytes plus the source encoding: "utf-8", or
    "cp1252" when the header is not valid UTF-8 (a Windows-exported file), in which
    case the caller must transcode the rest of the file too.
    """
    bom = _UTF8_BOM if raw_header.startswith(_UTF8_BOM) else b""
    body = raw_header[len(bom):]
    newline = b"\r\n" if body.endswith(b"\r\n") else b"\n" if body.endswith(b"\n") else b""
    raw = body[: len(body) - len(newline)]
    try:
        text, encoding = raw.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        text, encoding = raw.decode(_FALLBACK_ENCODING), _FALLBACK_ENCODING
    row = next(csv.reader([text]))
    new_row = snake_case_headers(row)
    buf = io.StringIO()
    csv.writer(buf, lineterminator="").writerow(new_row)
    return bom + buf.getvalue().encode("utf-8") + newline, encoding


def download_and_transform(
    session: requests.Session,
    dataset: Dataset,
    out_dir: Path,
    timeout: float = 60.0,
    validate: Optional[Callable[[Path], None]] = None,
) -> Path:
    """Stream one dataset to disk with a snake_case header. Returns final path.

    `validate`, if given, is called on the finished `.part` file before it replaces
    the previous output. If it raises, the previous file is left untouched.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / safe_filename(dataset)
    part = final.with_suffix(".csv.part")

    try:
        with session.get(dataset.download_url, stream=True, timeout=timeout) as resp:
            resp.raise_for_status()
            chunks = resp.iter_content(chunk_size=CHUNK_SIZE)
            buffer = b""
            decoder = None  # set when the source is not UTF-8 and must be transcoded
            with open(part, "wb") as out:

                def emit(data: bytes) -> None:
                    out.write(decoder.decode(data).encode("utf-8") if decoder else data)

                for chunk in chunks:
                    if not chunk:
                        continue
                    if buffer is None:
                        emit(chunk)
                        continue
                    buffer += chunk
                    nl = buffer.find(b"\n")
                    if nl == -1:
                        continue
                    header, encoding = _rewrite_header_line(buffer[: nl + 1])
                    if encoding != "utf-8":
                        log.warning("%s is %s, not UTF-8; transcoding", dataset.key, encoding)
                        decoder = codecs.getincrementaldecoder(encoding)()
                    out.write(header)
                    emit(buffer[nl + 1 :])
                    buffer = None
                if buffer is not None:
                    if not buffer:
                        raise ValueError("empty response body")
                    # Single-line file with no trailing newline: header only.
                    out.write(_rewrite_header_line(buffer)[0])
                if decoder:
                    out.write(decoder.decode(b"", final=True).encode("utf-8"))
        if validate is not None:
            validate(part)
        os.replace(part, final)
    except BaseException:
        if part.exists():
            part.unlink()
        raise
    return final


def run(
    out_dir: str | os.PathLike = "output",
    manifest_path: str | os.PathLike = "state/manifest.json",
    metadata_url: str = DEFAULT_METADATA_URL,
    theme: Optional[str] = DEFAULT_THEME,
    workers: int = 8,
    force: bool = False,
    db_path: Optional[str | os.PathLike] = None,
    session_factory: Callable[[], requests.Session] = build_session,
    max_row_drop: float = 0.3,
    null_markers: Sequence[str] = DEFAULT_NULL_MARKERS,
) -> RunResult:
    """Run the pipeline once and return a summary.

    Holds a single-run lock next to the manifest for the whole run; raises
    LockHeldError if another run is active (see lock.py).
    """
    with RunLock(Path(manifest_path).with_name("run.lock")):
        return _run(out_dir, manifest_path, metadata_url, theme, workers, force,
                    db_path, session_factory, max_row_drop, null_markers)


def _run(
    out_dir: str | os.PathLike,
    manifest_path: str | os.PathLike,
    metadata_url: str,
    theme: Optional[str],
    workers: int,
    force: bool,
    db_path: Optional[str | os.PathLike],
    session_factory: Callable[[], requests.Session],
    max_row_drop: float,
    null_markers: Sequence[str],
) -> RunResult:
    started = datetime.now(timezone.utc)
    out_path = Path(out_dir)
    manifest = Manifest(manifest_path).load()
    result = RunResult()

    try:
        with session_factory() as catalog_session:
            datasets = fetch_datasets(catalog_session, url=metadata_url, theme=theme)
    except Exception as exc:
        # Record the failed run so history shows the outage, then let the caller decide.
        result.failed["catalog"] = str(exc)
        _append_run_log(Path(manifest_path).with_name("runs.jsonl"), started, result)
        raise
    log.info("catalog lists %d CSV datasets for theme=%r", len(datasets), theme)

    # Forget datasets the catalog no longer lists so the manifest can't grow forever.
    result.pruned = manifest.prune(ds.key for ds in datasets)
    if result.pruned:
        log.warning("pruned %d manifest entries no longer in the catalog", len(result.pruned))

    todo: List[Dataset] = []
    for ds in datasets:
        if force or manifest.is_modified(ds.key, ds.modified):
            todo.append(ds)
        else:
            result.skipped.append(ds.key)
    log.info("%d to process, %d unchanged", len(todo), len(result.skipped))

    def work(ds: Dataset, prev: Optional[dict]) -> tuple[Path, CsvStats, List[str]]:
        checked: List[tuple[CsvStats, List[str]]] = []

        def gate(part: Path) -> None:
            # Quality and drift run on the .part file, in the worker, before it can
            # replace the last good CSV in out_dir.
            stats = check_csv(part, null_markers)
            drift = compare_drift(prev, stats, max_row_drop)
            warnings = list(drift.warnings) + [f"typing: {m}" for m in stats.suspect]
            if drift.row_drop:
                if not force:
                    raise DataQualityError(drift.row_drop)
                warnings.append(f"forced past: {drift.row_drop}")
            checked.append((stats, warnings))

        # One session per task keeps this safe without sharing connection state.
        with session_factory() as session:
            path = download_and_transform(session, ds, out_path, validate=gate)
        stats, warnings = checked[0]
        return path, stats, warnings

    # Loading happens on this (the main) thread only: SQLite allows one writer anyway,
    # and it keeps the connection off the worker threads.
    conn = sqlite_load.connect(db_path) if db_path is not None else None
    try:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            # The previous record is read here, on the main thread, before any worker runs.
            futures = {pool.submit(work, ds, manifest.get(ds.key)): ds for ds in todo}
            for fut in as_completed(futures):
                ds = futures[fut]
                try:
                    path, stats, warnings = fut.result()
                    if conn is not None:
                        loaded = sqlite_load.load_csv(
                            conn, path, sqlite_load.table_name(path), stats.types, null_markers
                        )
                        if loaded != stats.rows:
                            raise ValueError(f"loaded {loaded} rows, CSV has {stats.rows}")
                except Exception as exc:  # noqa: BLE001 - isolate per-dataset failure
                    log.error("failed %s: %s", ds.key, exc)
                    result.failed[ds.key] = str(exc)
                else:
                    manifest.update(ds.key, ds.modified, stats.rows, stats.columns, stats.types)
                    result.nulls[ds.key] = stats.nulls
                    manifest.save()  # per file: a killed run keeps its finished work
                    result.processed.append(ds.key)
                    result.rows[ds.key] = stats.rows
                    if warnings:
                        result.warnings[ds.key] = warnings
                        for w in warnings:
                            log.warning("%s: %s", ds.key, w)
                    log.info("done %s (%d rows)", ds.title, stats.rows)
    finally:
        if conn is not None:
            conn.close()

    manifest.save()
    _append_run_log(Path(manifest_path).with_name("runs.jsonl"), started, result)
    return result


def _append_run_log(path: Path, started: datetime, result: RunResult) -> None:
    """Append one JSON line per run: when it ran and what it did."""
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "started_at": started.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "processed": len(result.processed),
        "skipped": len(result.skipped),
        "rows_loaded": sum(result.rows.values()),
        "cells_nulled": sum(result.nulls.values()),
        "warnings": result.warnings,
        "pruned": result.pruned,
        "failed": result.failed,
    }
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
