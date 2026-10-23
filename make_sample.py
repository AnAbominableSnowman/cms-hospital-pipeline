"""Copy the header and first rows of every output file into sample_output/, plus the run log."""
import shutil
from pathlib import Path

OUT, SAMPLE, ROWS = Path("output"), Path("sample_output"), 6  # data rows per file


def head(src, records):
    """Yield the raw bytes of the first `records` CSV records (header included).

    A quoted field can contain a line break, so a record ends only at a newline
    reached with an even number of quote characters ("" escapes count as two).
    """
    quotes = 0
    for line in src:
        yield line
        quotes += line.count(b'"')
        if quotes % 2 == 0:
            records -= 1
            quotes = 0
            if records == 0:
                return


shutil.rmtree(SAMPLE, ignore_errors=True)
SAMPLE.mkdir()
files = sorted(OUT.glob("*.csv"))
for f in files:
    with open(f, "rb") as src, open(SAMPLE / f.name, "wb") as dst:
        dst.writelines(head(src, ROWS + 1))
shutil.copy("state/runs.jsonl", SAMPLE / "runs.jsonl")
print(f"sampled {len(files)} files ({ROWS} rows each) into {SAMPLE}/")
