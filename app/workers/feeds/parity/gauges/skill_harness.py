"""T178 parity harness: the PYTHON scorer (pipeline/gauges/feed/skill.py, the spec) on a worker-layout fixture.

    .venv/bin/python app/workers/feeds/parity/gauges/skill_harness.py --dir DIR --month YYYY-MM --before YYYY-MM-DD

DIR holds `feed/` (the published prefix: <station>.json, clim.json) and `priv/runs/` (the worker's archive: the month
file, day files, parts). The worker's layout is concatenated in key order into Python's `feed.runs/<month>.jsonl`
(T178 ticket: exactly how a copy of the R2 archive is scored by hand), then `read_runs`, `feed_observed`, `cases`,
`score` and `verdict` run as the CLI runs them. Prints the CLI's JSON.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from pipeline.gauges.feed.skill import (archive_dir, cases, clim_ranks, feed_observed, read_runs, score,  # noqa: E402
                                        verdict)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True, type=Path)
    ap.add_argument("--month", required=True)
    ap.add_argument("--before", required=True)
    a = ap.parse_args()
    feed = a.dir / "feed"
    runs = a.dir / "priv"
    keys = sorted(str(p.relative_to(runs)) for p in (runs / "runs").rglob("*.jsonl")
                  if str(p.relative_to(runs)) == f"runs/{a.month}.jsonl"
                  or str(p.relative_to(runs)).startswith(f"runs/{a.month}/"))
    out = archive_dir(feed)
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{a.month}.jsonl").write_text("".join((runs / k).read_text(encoding="utf-8") for k in keys), encoding="utf-8")
    clim_path = feed / "clim.json"
    clim = json.loads(clim_path.read_text(encoding="utf-8")) if clim_path.exists() else None
    records = read_runs(out)
    found = cases(records, feed_observed(feed), date.fromisoformat(a.before))
    table = score(found, clim_ranks(clim))
    print(json.dumps({"runs": len(records), "cases": len(found), "table": table,
                      "verdict": {lead: verdict(row) for lead, row in table.items()}}))


if __name__ == "__main__":
    main()
