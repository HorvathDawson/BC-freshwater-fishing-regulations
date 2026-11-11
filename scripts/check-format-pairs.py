"""FORMAT PAIRS (T483 fast-fail, AGENTS 20a): every format string Python WRITES that the app / worker READS has ONE value
on both sides.  .venv/bin/python scripts/check-format-pairs.py   (stdlib only, reads the source text, < 0.1 s)

caught (2026-11-09): T517 bumped `inseason-links/3` in pipeline/regs/inseason/publish.py (FORMAT) and core inseason.ts
(INSEASON_LINKS_FORMAT), but the feeds worker still required /2 (fixed in 53b90e7: the worker imports the core constant).

Only plain pairs are listed: a Python constant that names the format it writes, and the TypeScript constant (or the
literal its reader compares) that names what is read. A reader that takes several versions (PAINT_READS) must include
the one written. A new pair joins the list when a writer and its reader are added; a moved name fails here, loudly.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# (python file, regex whose group 1 is the written format) -> (ts file, regex whose group 1 is the read format)
PY_CONST = r'^{name}\s*=\s*"([^"]+)"'
TS_CONST = r'\b{name}\s*=\s*"([^"]+)"'
PAIRS = [
    ("inseason links (matches.json)",
     "pipeline/regs/inseason/publish.py", PY_CONST.format(name="FORMAT"),
     "app/packages/core/src/inseason.ts", TS_CONST.format(name="INSEASON_LINKS_FORMAT")),
    ("DFO salmon feed index",
     "pipeline/regs/dfo_salmon/live.py", PY_CONST.format(name="FORMAT"),
     "app/packages/core/src/salmonFeed.ts", TS_CONST.format(name="SALMON_FEED_FORMAT")),
    ("DFO salmon feed index (the feeds worker writes it too)",
     "pipeline/regs/dfo_salmon/live.py", PY_CONST.format(name="FORMAT"),
     "app/workers/feeds/src/dfo/live.ts", TS_CONST.format(name="FORMAT")),
    ("run timing index",
     "pipeline/runtiming/feed/build.py", PY_CONST.format(name="INDEX_FORMAT"),
     "app/packages/core/src/runTiming.ts", r'\bindex:\s*"([^"]+)"'),
    ("run timing runs",
     "pipeline/runtiming/feed/build.py", PY_CONST.format(name="RUNS_FORMAT"),
     "app/packages/core/src/runTiming.ts", r'\bruns:\s*"([^"]+)"'),
    ("run timing paint",
     "pipeline/runtiming/feed/build.py", PY_CONST.format(name="PAINT_FORMAT"),
     "app/packages/core/src/runTiming.ts", r'\bpaint:\s*"([^"]+)"'),
    ("gauge feed (index, station files, obs)",
     "pipeline/gauges/feed/publish.py", PY_CONST.format(name="FEED_FORMAT"),
     "app/packages/core/src/gaugeFeed.ts", TS_CONST.format(name="GAUGE_FEED_FORMAT")),
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
    for b in bad:
        print(f"fast-fail: {b}", file=sys.stderr)
    if not bad:
        print(f"   {len(PAIRS) + len(MEMBER)} format pair(s) agree")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
