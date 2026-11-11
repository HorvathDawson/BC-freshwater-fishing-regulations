"""(T412) WHAT A NEW RUN-TIMING FEED CHANGES against the one on R2 — the workflow publishes only a real change.

    python -m pipeline.runtiming.feed.diff --new <out>/runtiming --live https://data-staging.canifishthis.ca/runtiming

`changed` is true when the live index is missing or any file (runs, paint, waters: by sha256) or the tiles it is bound
to differ; `built_at` alone is not a change. The report lists the runs added, dropped and changed (by run id). Written
as JSON to stdout (and `--out FILE`).

THE FLOOR (T552 item 1): an upstream column or page change can parse to a collapsed feed that still passes `build`'s
internal checks. So the new feed's PSF runs, DFO runs and painted sections are compared with the live feed's, and a fall
of more than `FLOOR_SHARE` in any of them EXITS 1 (the report is written first, with `floor` naming each fall) — the
workflow stops before it keeps fresh sources or uploads. `--allow-shrink REASON` lets a shrink the owner expects through;
the reason is in the report. Exit 0 otherwise.
"""
from __future__ import annotations

import argparse
import gzip
import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

UA = {"User-Agent": "bc-fishing-runtiming-diff"}
#: the most any of the floor's counts may fall against the live feed before the run is refused
FLOOR_SHARE = 0.10


def _get(url: str) -> Optional[bytes]:
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60) as r:
            return r.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def _doc(raw: bytes) -> dict:
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def _runs(raw: bytes) -> dict:
    return {r["id"]: r for r in _doc(raw)["runs"]}


def counts(runs: dict, painted: int) -> dict:
    """What the floor compares: runs per source and the painted sections."""
    return {"psf_runs": sum(1 for r in runs.values() if r.get("src") == "psf"),
            "dfo_runs": sum(1 for r in runs.values() if r.get("src") == "dfo"), "painted_sections": painted}


def floor(new: dict, live: Optional[dict], share: float = FLOOR_SHARE) -> list:
    """Each count that fell more than `share` against the live feed's, as words; [] when none (or nothing is live)."""
    if not live:
        return []
    return [f"{k}: {live[k]} live -> {new[k]} new ({(live[k] - new[k]) / live[k]:.0%} fewer)"
            for k in sorted(new) if live.get(k) and new[k] < live[k] * (1 - share)]


def diff(new_index: dict, new_runs: dict, live_index: Optional[dict], live_runs: Optional[dict]) -> dict:
    if live_index is None:
        return {"changed": True, "why": "nothing live", "runs": {"added": len(new_runs), "dropped": 0, "changed": 0}}
    why = [n for n in ("runs", "paint", "waters")
           if new_index["files"][n]["sha256"] != live_index.get("files", {}).get(n, {}).get("sha256")]
    if new_index.get("tile_version") != live_index.get("tile_version"):
        why.append("tiles")
    live_runs = live_runs or {}
    added = sorted(set(new_runs) - set(live_runs))
    dropped = sorted(set(live_runs) - set(new_runs))
    changed = sorted(k for k in set(new_runs) & set(live_runs) if new_runs[k] != live_runs[k])
    return {"changed": bool(why), "why": ", ".join(why) or "same files",
            "tiles": {"live": live_index.get("tile_version"), "new": new_index.get("tile_version")},
            "runs": {"added": len(added), "dropped": len(dropped), "changed": len(changed)},
            "examples": {"added": added[:10], "dropped": dropped[:10], "changed": changed[:10]}}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline.runtiming.feed.diff", description=__doc__.split("\n\n")[0])
    ap.add_argument("--new", type=Path, required=True, help="the built runtiming/ directory")
    ap.add_argument("--live", required=True, help="the live runtiming/ base URL")
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--allow-shrink", default="", metavar="REASON",
                    help="let a fall past the floor through (the owner expects it); the reason is reported")
    a = ap.parse_args(argv)
    new_index = json.loads((a.new / "index.json").read_text())
    new_runs = _runs((a.new / new_index["files"]["runs"]["url"]).read_bytes())
    new_paint = _doc((a.new / new_index["files"]["paint"]["url"]).read_bytes())
    base = a.live.rstrip("/")
    raw = _get(f"{base}/index.json")
    live_index = json.loads(raw) if raw else None
    live_runs, live_counts = None, None
    if live_index:
        r = _get(f"{base}/{live_index['files']['runs']['url']}")
        p = _get(f"{base}/{live_index['files']['paint']['url']}")
        if r is None or p is None:
            raise SystemExit(f"diff: the live index at {base} names a runs or paint file that is not there")
        live_runs = _runs(r)
        live_counts = counts(live_runs, len(_doc(p)["sid"]))
    rep = diff(new_index, new_runs, live_index, live_runs)
    new_counts = counts(new_runs, len(new_paint["sid"]))
    fell = floor(new_counts, live_counts)
    rep["counts"] = {"new": new_counts, "live": live_counts}
    rep["floor"] = {"share": FLOOR_SHARE, "fell": fell, "allowed": a.allow_shrink or None}
    text = json.dumps(rep, indent=1, sort_keys=True)
    print(text)
    if a.out:
        a.out.write_text(text + "\n")
    if fell and not a.allow_shrink:
        print(f"diff: REFUSED — the new feed fell past the floor ({FLOOR_SHARE:.0%}) against the live one: "
              f"{'; '.join(fell)}. An upstream format change? If the owner expects it, rerun with --allow-shrink REASON.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
