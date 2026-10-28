"""T225 DFO parity harness: run the PYTHON live feed (`pipeline.regs.dfo_salmon.live.run`) over a recorded scenario so
the TypeScript port (`src/dfo/`) can be diffed against it byte for byte.

    .venv/bin/python app/workers/feeds/parity/dfo/harness.py <scenario.json> <out_dir>

scenario.json (paths relative to the scenario file):
    {"entries": "entries",                      # the curated entry files the run reads
     "initial": {"index": "prev/index.json", "unplaced": "prev/unplaced.json"},   # optional previous feed state
     "steps": [{"name": "fresh", "now": "2026-10-28T12:00:00Z", "regions": null,
                "fetched": "2026-10-28T12:00:00+00:00",
                "pages": {"1": "pages/region1-eng.html", "2": null, ...}}]}   # null / missing: the fetch fails

Each step runs in the same feed dir (state carries), and its index.json, unplaced.json and notices.json are copied to
<out_dir>/<n>-<name>/. Pages are read as the live fetcher reads them (`Path.read_text`: universal newlines).
No network, no credits, no notify (FEED_NOTIFY_URL is cleared).
"""
from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT))
os.environ.pop("FEED_NOTIFY_URL", None)

from pipeline.regs.dfo_salmon import live  # noqa: E402

FAIL_MSG = "fixture: the page could not be fetched"


def main(scenario: str, out: str) -> int:
    sc_path = Path(scenario).resolve()
    base = sc_path.parent
    sc = json.loads(sc_path.read_text(encoding="utf-8"))
    out_dir = Path(out)
    if out_dir.exists():
        shutil.rmtree(out_dir)
    feed = out_dir / "_feed"
    feed.mkdir(parents=True)
    init = sc.get("initial") or {}
    for name in ("index", "unplaced"):
        if init.get(name):
            shutil.copy(base / init[name], feed / f"{name}.json")
    entries = base / sc["entries"]
    for n, step in enumerate(sc["steps"]):
        pages = step.get("pages") or {}

        def fetcher(slug: str, pages=pages, step=step):
            p = pages.get(slug)
            if p is None:
                raise RuntimeError(FAIL_MSG)
            return (base / p).read_text(encoding="utf-8"), step.get("fetched")

        got = live.run(feed, fetcher=fetcher, regions=step.get("regions"), entries_dir=entries, now=step["now"],
                       notify_url=None)
        dest = out_dir / f"{n}-{step['name']}"
        dest.mkdir(parents=True)
        shutil.copy(feed / "index.json", dest / "index.json")
        shutil.copy(feed / "unplaced.json", dest / "unplaced.json")
        (dest / "notices.json").write_text(json.dumps(got["notices"], ensure_ascii=False, indent=1) + "\n",
                                           encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
