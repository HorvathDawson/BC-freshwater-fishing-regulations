"""T331: THE ONE LIST of what goes to R2 beside a release for the feeds Worker and the app — read by BOTH uploaders.

    python -m pipeline.deliver.sync_inputs --out DIR [--only NAME ...]

stages each input at `DIR/<its R2 key>` and prints one line per object, `<key>\\t<cache-control or ->`. The two
uploaders only put what is staged, so they cannot drift apart again (the workflow had no `wording.json`):

    .github/workflows/feeds-sync-inputs.yml   (GitHub; compares MD5 = the R2 ETag, puts what differs)
    deploy/staging/sync-inputs.sh             (the Mac; rclone --checksum)

THE LIST (`INPUTS`), every source read through `CURATED` / `SOURCE`:
    dfo-entries       dfo_salmon/places/region-*.json               -> private/feeds/dfo/entries/region-*.json
    wording           dfo_salmon/wording.json                       -> private/feeds/dfo/wording.json (T321; the
                                                                       worker's ONLY copy, T324)
    roster            source bc_hydrometric_stations.json, slim [{station, realtime}] -> private/feeds/gauges/roster.json
    inseason-matches  `pipeline.regs.inseason.publish` (inseason/notice_waters.json + split labels)
                                                                    -> feeds/inseason/matches.json (PUBLIC)

`private/` keys are never served (the data worker 404s them); the Worker reads them on its next run, no redeploy.
Needs only pydantic + pyyaml (the GitHub runner installs nothing else).
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
    Input("roster", _roster),
    Input("inseason-matches", _inseason, INSEASON_CACHE),
)
NAMES = tuple(i.name for i in INPUTS)


def stage(out: Path, only: tuple[str, ...] = ()) -> list[tuple[str, str | None]]:
    """Stage the inputs (all, or `only`) under `out`; -> [(key, cache-control)] in list order."""
    bad = sorted(set(only) - set(NAMES))
    if bad:
        raise SystemExit(f"sync_inputs: unknown input(s) {bad}; the list is {', '.join(NAMES)}")
    got = []
    for i in INPUTS:
        if only and i.name not in only:
            continue
        got += [(k, i.cache) for k in i.stage(out)]
    return got


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, required=True, help="the staging directory (objects land at <out>/<key>)")
    ap.add_argument("--only", nargs="*", default=[], choices=NAMES, help="stage just these (default: all)")
    a = ap.parse_args(argv)
    a.out.mkdir(parents=True, exist_ok=True)
    for key, cache in stage(a.out, tuple(a.only)):
        sys.stdout.write(f"{key}\t{cache or '-'}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
