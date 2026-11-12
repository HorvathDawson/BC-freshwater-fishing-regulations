"""T331: THE ONE LIST of what goes to R2 beside a release for the feeds Worker and the app — read by BOTH uploaders.

    python -m pipeline.deliver.sync_inputs --out DIR [--only NAME ...]

stages each input at `DIR/<its R2 key>` and prints one line per object, `<key>\t<cache-control or ->`. The uploaders
only put what is staged — all on the Mac (T520, owner 2026-11-08: the GitHub workflow is deleted; it had no atlas):

    deploy/staging/sync-inputs.sh   (rclone --checksum; by hand, from upload.sh, and by the review app after a saved
                                     input: curation-review/backend/inputs_sync.py)
    deploy/prod/sync-inputs.sh      (the owner's CONFIRM_PROD step)

THE LIST (`INPUTS`), every source read through `CURATED` / `SOURCE`:
    dfo-entries       dfo_salmon/places/region-*.json               -> private/feeds/dfo/entries/region-*.json
    wording           dfo_salmon/wording.json                       -> private/feeds/dfo/wording.json (T321; the
                                                                       worker's ONLY copy, T324)
    dfo-pairs         `pipeline.deliver.salmon_pairs` (the live release's two-region chains over the current typed
                      DFO feed: the Python's answer for two regions' openings, T668)
                                                                    -> private/feeds/dfo/pairs.json
    roster            source bc_hydrometric_stations.json, slim [{station, realtime}] -> private/feeds/gauges/roster.json
    inseason-matches  `pipeline.regs.inseason.publish` (inseason/notice_waters.json, already resolved to FWA ranges
                      where the atlas is — T520: no atlas here)     -> feeds/inseason/matches.json (PUBLIC)

`private/` keys are never served (the data worker 404s them); the Worker reads them on its next run, no redeploy.
Needs only pydantic + pyyaml (no atlas: the in-season matches are resolved before, `pipeline.regs.inseason.resolve`).
"""
from __future__ import annotations

import argparse
import contextlib
import json
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from pipeline.common.curated import CURATED, SOURCE

#: the in-season links are public and re-read often (the app polls beside the worker's index.json)
INSEASON_CACHE = "public, max-age=300, stale-while-revalidate=3600"


ROSTER_FIELDS = ("station", "realtime")


def _dfo_entries(out: Path) -> list[str]:
    keys = []
    for f in sorted(Path(CURATED.dfo_salmon.places).glob("region-*.json")):
        key = f"private/feeds/dfo/entries/{f.name}"
        _put(out, key).write_bytes(f.read_bytes())
        keys.append(key)
    return keys


def _wording(out: Path) -> list[str]:
    key = "private/feeds/dfo/wording.json"
    _put(out, key).write_bytes(Path(CURATED.dfo_salmon.wording).read_bytes())
    return [key]


def _dfo_pairs(out: Path) -> list[str]:
    """(T668 option 2, owner 2026-11-12) the straddler pair table for the live release (`salmon_pairs.table`); a
    release, a typed feed or a curated place missing STOPS this input (said, the others still stage)."""
    from pipeline.deliver import salmon_pairs
    from pipeline.regs.inseason.resolve import release_dir
    key = "private/feeds/dfo/pairs.json"
    t = salmon_pairs.table(release_dir())
    _put(out, key).write_text(json.dumps(t, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
                              encoding="utf-8")
    return [key]


def _roster(out: Path) -> list[str]:
    """The station roster cut to what the Worker reads — the bytes `jq -c '[.[] | {station, realtime}]'` wrote."""
    rows = json.loads((SOURCE / "bc_hydrometric_stations.json").read_text(encoding="utf-8"))
    slim = [{k: r.get(k) for k in ROSTER_FIELDS} for r in rows]   # a copy for the Worker, not a liveness read (test_liveness)
    key = "private/feeds/gauges/roster.json"
    _put(out, key).write_text(json.dumps(slim, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    return [key]


def _inseason(out: Path) -> list[str]:
    from pipeline.regs.inseason import publish
    key = "feeds/inseason/matches.json"
    with tempfile.TemporaryDirectory() as tmp:
        with contextlib.redirect_stdout(sys.stderr):          # stdout is the manifest
            rc = publish.main(["--out", tmp])
        if rc:
            raise SystemExit(f"sync_inputs: inseason.publish exited {rc}")
        shutil.copyfile(Path(tmp) / "matches.json", _put(out, key))
    return [key]


def _put(out: Path, key: str) -> Path:
    p = out / key
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


@dataclass(frozen=True)
class Input:
    name: str
    stage: Callable[[Path], list[str]]
    cache: str | None = None          # Cache-Control for each object; None = the bucket default


INPUTS = (
    Input("dfo-entries", _dfo_entries),
    Input("wording", _wording),
    Input("dfo-pairs", _dfo_pairs),
    Input("roster", _roster),
    Input("inseason-matches", _inseason, INSEASON_CACHE),
)
NAMES = tuple(i.name for i in INPUTS)


def stage(out: Path, only: tuple[str, ...] = (), failed: list[tuple[str, str]] | None = None
          ) -> list[tuple[str, str | None]]:
    """Stage the inputs (all, or `only`) under `out`; -> [(key, cache-control)] in list order. (T520) EACH INPUT ON
    ITS OWN: one that fails (an in-season refusal) is appended to `failed` as (name, why) and the others still stage —
    a refused match must not hold back the DFO entries. Without `failed`, the first failure raises."""
    bad = sorted(set(only) - set(NAMES))
    if bad:
        raise SystemExit(f"sync_inputs: unknown input(s) {bad}; the list is {', '.join(NAMES)}")
    got = []
    for i in INPUTS:
        if only and i.name not in only:
            continue
        try:
            got += [(k, i.cache) for k in i.stage(out)]
        except (SystemExit, Exception) as ex:                 # noqa: BLE001 — said, per input
            if failed is None:
                raise
            failed.append((i.name, str(ex) or type(ex).__name__))
    return got


def main(argv: list[str] | None = None) -> int:
    """The manifest of what staged on stdout; every input that did not, one `FAILED <name>: <why>` line on stderr and
    exit 1 (the uploader puts what staged, then fails)."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, required=True, help="the staging directory (objects land at <out>/<key>)")
    ap.add_argument("--only", nargs="*", default=[], choices=NAMES, help="stage just these (default: all)")
    a = ap.parse_args(argv)
    a.out.mkdir(parents=True, exist_ok=True)
    failed: list[tuple[str, str]] = []
    for key, cache in stage(a.out, tuple(a.only), failed):
        sys.stdout.write(f"{key}\t{cache or '-'}\n")
    for name, why in failed:
        sys.stderr.write(f"FAILED {name}: {why}\n")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
