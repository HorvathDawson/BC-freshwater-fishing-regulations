"""FORMAT PAIRS (T483 fast-fail, AGENTS 20a): every format string Python WRITES that the app / worker READS has ONE value
on both sides.  .venv/bin/python scripts/check-format-pairs.py   (stdlib only, reads the source text, < 0.1 s)

caught (2026-11-09): T517 bumped `inseason-links/3` in pipeline/regs/inseason/publish.py (FORMAT) and core inseason.ts
(INSEASON_LINKS_FORMAT), but the feeds worker still required /2 (fixed in 53b90e7: the worker imports the core constant).

THE LIVE FEEDS' FORMATS ARE NOT PAIRS ANY MORE (T556): they are typed once, in pipeline/common/feed_formats.py, and the
TypeScript is generated from it (`python -m pipeline.tools.emit.feed_formats --check`). What this script holds for them
is `GENERATED` below: each named constant still reads the table, and none of the table's strings is typed as a literal
in product code anywhere else (a literal beside a reader is how the worker fell behind).

Only plain pairs are listed: a Python constant that names the format it writes, and the TypeScript constant (or the
literal its reader compares) that names what is read. A reader that takes several versions (PAINT_READS) must include
the one written. A new pair joins the list when a writer and its reader are added; a moved name fails here, loudly.
"""
from __future__ import annotations

import io
import re
import sys
import tokenize
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# (python file, regex whose group 1 is the written format) -> (ts file, regex whose group 1 is the read format)
PY_CONST = r'^{name}\s*=\s*"([^"]+)"'
TS_CONST = r'\b{name}\s*=\s*"([^"]+)"'
PAIRS = [
    ("release salmon file",
     "pipeline/deliver/salmon.py", PY_CONST.format(name="FORMAT"),
     "app/packages/core/src/salmon.ts", r'P\.format\s*!==\s*"([^"]+)"'),
    ("app manifest",
     "pipeline/deliver/release/manifest.py", PY_CONST.format(name="FORMAT"),
     "app/packages/data/src/pack.generated.ts", TS_CONST.format(name="MANIFEST_FORMAT")),
    ("regs water file",
     "pipeline/deliver/release/contract.py", r'"water":\s*\{"format":\s*"([^"]+)"',
     "app/packages/core/src/regs/versions.ts", r'\bwater:\s*"([^"]+)"'),
    ("regs region files",
     "pipeline/deliver/release/contract.py", r'"regs-\{r\}":\s*\{"format":\s*"([^"]+)"',
     "app/packages/core/src/regs/versions.ts", r'\bregion:\s*"([^"]+)"'),
    ("regs sid file",
     "pipeline/deliver/release/contract.py", r'"regs-sid":\s*\{"format":\s*"([^"]+)"',
     "app/packages/core/src/regs/versions.ts", r'\bsid:\s*"([^"]+)"'),
]
#: a reader that takes a LIST of versions: the written one must be in it
MEMBER = [
    ("release paint file",
     "pipeline/deliver/release/paint.py", PY_CONST.format(name="FORMAT"),
     "app/packages/core/src/paint.ts", r'\bPAINT_READS\b[^=]*=\s*\[([^\]]*)\]'),
]


#: (file, regex that must match once): each feed format constant READS the table / the generated file (T556)
GENERATED = [
    ("pipeline/regs/dfo_salmon/live.py", r'^FORMAT = FEED_FORMATS\["dfo_feed"\]'),
    ("pipeline/regs/inseason/publish.py", r'^FORMAT = FEED_FORMATS\["inseason_links"\]'),
    ("pipeline/gauges/feed/publish.py", r'^FEED_FORMAT = FEED_FORMATS\["gauge_feed"\]'),
    ("pipeline/runtiming/feed/build.py", r'^INDEX_FORMAT = FEED_FORMATS\["runtiming_index"\]'),
    ("pipeline/runtiming/feed/build.py", r'^RUNS_FORMAT = FEED_FORMATS\["runtiming_runs"\]'),
    ("pipeline/runtiming/feed/build.py", r'^PAINT_FORMAT = FEED_FORMATS\["runtiming_paint"\]'),
    ("app/packages/core/src/salmonFeed.ts", r'^export const SALMON_FEED_FORMAT = FEED_FORMATS\.dfoFeed;'),
    ("app/packages/core/src/inseason.ts", r'^export const INSEASON_FEED_FORMAT = FEED_FORMATS\.inseasonFeed;'),
    ("app/packages/core/src/inseason.ts", r'^export const INSEASON_LINKS_FORMAT = FEED_FORMATS\.inseasonLinks;'),
    ("app/packages/core/src/gaugeFeed.ts", r'^export const GAUGE_FEED_FORMAT = FEED_FORMATS\.gaugeFeed;'),
    ("app/packages/core/src/runTiming.ts", r'index: FEED_FORMATS\.runtimingIndex, runs: FEED_FORMATS\.runtimingRuns,\s+paint: FEED_FORMATS\.runtimingPaint'),
    ("app/workers/feeds/src/dfo/live.ts", r'^export const FORMAT = FEED_FORMATS\.dfoFeed;'),
    ("app/workers/feeds/src/inseason.ts", r'^export const FEED_FORMAT = INSEASON_FEED_FORMAT;'),
]
#: where a feed format may be typed: the table, and the file generated from it
TABLE = "pipeline/common/feed_formats.py"
TABLE_TS = "app/packages/core/src/feedFormats.generated.ts"
#: product code scanned for a feed format typed as a literal (tests, fixtures, generated files and the generated
#: goldens — captured feeds — are not product code)
SCAN = [("pipeline", ".py"), ("app/packages", ".ts"), ("app/packages", ".tsx"), ("app/workers/feeds/src", ".ts"),
        ("app/apps/mobile/src", ".ts"), ("app/apps/mobile/src", ".tsx")]
SKIP_DIRS = {"node_modules", "__pycache__", "tests", "dist", "fixtures", "__snapshots__"}


def feed_formats() -> dict:
    """The table, read without importing the pipeline package (this script is stdlib only)."""
    import runpy
    return runpy.run_path(str(ROOT / TABLE))["FEED_FORMATS"]


def typed_twice(formats: dict) -> list[str]:
    """Every place a feed format is typed as a quoted literal outside the table and its generated file."""
    import os
    rx = re.compile("[\"'`](" + "|".join(re.escape(v) for v in sorted(set(formats.values()))) + ")[\"'`]")
    bad = []
    for top, ext in SCAN:
        for folder, dirs, files in os.walk(ROOT / top):
            dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS and not d.startswith("."))
            for name in sorted(files):
                rel = (Path(folder) / name).relative_to(ROOT).as_posix()
                if (not name.endswith(ext) or rel in (TABLE, TABLE_TS) or ".test." in name or ".generated." in name
                        or ".golden." in name or name.startswith("test_") or name.endswith(".d.ts")):
                    continue
                text = (Path(folder) / name).read_text(encoding="utf-8")
                if ext == ".py":      # a string TOKEN that is exactly a format (a docstring showing the JSON is not one)
                    hits = [(t.start[0], t.string[1:-1]) for t in tokenize.generate_tokens(io.StringIO(text).readline)
                            if t.type == tokenize.STRING and rx.fullmatch(t.string)]
                else:
                    hits = [(n, m.group(1)) for n, line in enumerate(text.splitlines(), 1)
                            for m in rx.finditer(re.sub(r"(^|\s)//.*$|^\s*\*.*$|/\*.*?\*/", "", line))]
                for n, fmt in hits:
                    bad.append(f"{rel}:{n}: the feed format {fmt!r} is typed as a literal — read it from "
                               f"{TABLE} (Python) or FEED_FORMATS in {TABLE_TS} (TypeScript)")
    return bad


def one(path: str, rx: str) -> str | list[str]:
    """The single value `rx` finds in `path`, or the problem as a list of one message."""
    text = (ROOT / path).read_text(encoding="utf-8")
    got = sorted(set(re.findall(rx, text, flags=re.M)))
    if len(got) != 1:
        return [f"{path}: {'no' if not got else len(got)} match(es) of /{rx}/ {got or ''} — the name moved? fix "
                "scripts/check-format-pairs.py with it"]
    return got[0]


def main() -> int:
    bad = []
    for what, py, pr, ts, tr in PAIRS:
        w, r = one(py, pr), one(ts, tr)
        for x in (w, r):
            if isinstance(x, list):
                bad += x
        if isinstance(w, str) and isinstance(r, str) and w != r:
            bad.append(f"{what}: Python writes {w!r} ({py}) but the app reads {r!r} ({ts}) — bump both together")
    for what, py, pr, ts, tr in MEMBER:
        w, r = one(py, pr), one(ts, tr)
        for x in (w, r):
            if isinstance(x, list):
                bad += x
        if isinstance(w, str) and isinstance(r, str):
            reads = re.findall(r'"([^"]+)"', r)
            if w not in reads:
                bad.append(f"{what}: Python writes {w!r} ({py}) but the app reads only {reads} ({ts})")
    for path, rx in GENERATED:
        if len(re.findall(rx, (ROOT / path).read_text(encoding="utf-8"), flags=re.M)) != 1:
            bad.append(f"{path}: no line matches /{rx}/ — a feed format constant must read the one table ({TABLE}; "
                       "TypeScript through the generated FEED_FORMATS), never a literal")
    bad += typed_twice(feed_formats())
    for b in bad:
        print(f"fast-fail: {b}", file=sys.stderr)
    if not bad:
        print(f"   {len(PAIRS) + len(MEMBER)} format pair(s) agree; {len(GENERATED)} feed format constant(s) read the one table")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
