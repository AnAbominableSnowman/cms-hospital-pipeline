"""CLI entrypoint:  python -m cms_pipeline [--config config.toml] [options]

Exit codes: 0 success, 1 some datasets failed (retried next run), 2 aborted
(catalog unreachable or bad config), 3 not started (another run holds the lock).
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import replace

from .config import load_config
from .lock import LockHeldError
from .pipeline import run


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="cms_pipeline", description=__doc__)
    p.add_argument("--config", help="TOML file; flags below override it")
    p.add_argument("--out-dir")
    p.add_argument("--manifest")
    p.add_argument("--db", help="SQLite path; pass an empty string to skip loading")
    p.add_argument("--metadata-url")
    p.add_argument("--theme", help="catalog theme filter")
    p.add_argument("--workers", type=int)
    p.add_argument("--force", action="store_true", help="ignore the manifest")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )
    try:
        cfg = load_config(args.config).with_overrides(
            out_dir=args.out_dir,
            manifest_path=args.manifest,
            db_path=args.db,
            metadata_url=args.metadata_url,
            theme=args.theme,
            workers=args.workers,
        )
        if cfg.db_path == "":  # "--db ''" means skip the load step
            cfg = replace(cfg, db_path=None)
        result = run(
            out_dir=cfg.out_dir,
            manifest_path=cfg.manifest_path,
            metadata_url=cfg.metadata_url,
            theme=cfg.theme,
            workers=cfg.workers,
            force=args.force,
            db_path=cfg.db_path,
            max_row_drop=cfg.max_row_drop,
            null_markers=cfg.null_markers,
        )
    except LockHeldError as exc:
        logging.error("not started: %s", exc)
        return 3
    except Exception as exc:  # catalog unreachable / malformed / bad config
        logging.error("run aborted: %s", exc)
        return 2

    print(
        f"processed={len(result.processed)} skipped={len(result.skipped)} "
        f"failed={len(result.failed)} rows_loaded={sum(result.rows.values())}"
    )
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
