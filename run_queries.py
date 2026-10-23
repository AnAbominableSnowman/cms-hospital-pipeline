"""Run every statement in queries.sql against the loaded database and print results.

    python run_queries.py [state/hospitals.db] [queries.sql]
"""

import sqlite3
import sys
from pathlib import Path


def statements(sql: str):
    """Yield (name, statement) pairs; a '-- name:' comment labels the statement after it."""
    name, buf = None, ""
    for line in sql.splitlines(keepends=True):
        if line.startswith("-- name:"):
            name = line[len("-- name:"):].strip()
        elif line.startswith("--"):
            continue
        else:
            buf += line
            if sqlite3.complete_statement(buf):
                yield name, buf.strip()
                name, buf = None, ""


def main() -> int:
    db = sys.argv[1] if len(sys.argv) > 1 else "state/hospitals.db"
    sql_file = sys.argv[2] if len(sys.argv) > 2 else "queries.sql"
    conn = sqlite3.connect(db)
    for name, stmt in statements(Path(sql_file).read_text(encoding="utf-8")):
        cur = conn.execute(stmt)
        if cur.description is None:  # DDL such as CREATE VIEW
            continue
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        print(f"\n== {name} ({len(rows)} rows)")
        print(" | ".join(cols))
        for r in rows[:10]:
            print(" | ".join("" if v is None else str(v) for v in r))
    conn.commit()
    return 0


if __name__ == "__main__":
    sys.exit(main())
