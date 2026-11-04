"""(T373, owner 2026-11-03: "we need the run timing to always be synced") RUN TIMING BOUND FOR ONE RELEASE. `deliver all`
runs this after the release step, so every release has a run-timing feed of ITS map: the paint index names the
release's `section_handles` and its tiles' `tile_version`; the vintage check (`common.vintage.report_all`) refuses a
delivery whose feed names another. A new atlas no longer waits on a hand-run base (the 2026-11-03 failure: the feed
was bound to 10cbcb72…, the release carried 822db448…, so Run timing painted nothing).

    python -m pipeline.runtiming.feed.release --release DIR [--tiles DIR] [--build DIR] --out DIR

1. THE BASE for the tiles' handles (`base-<handles>.json.gz` in `GENERATED.runtiming/feed`): read when it is there,
   else built (`base.build`: the atlas graph + the tiles' layers, ~3 GB streamed — heavy, as `deliver all` already is).
2. THE BINDING (`build.build`) of the parsed timings (`TIMING`, source data since T331) and the CU polygons (`POLYGONS`)
   — `python -m pipeline.runtiming.parse`'s own default outputs, after a fetch. Missing inputs stop the delivery (never an
   old feed in their place). Checked (`build.check`), written to `--out`.
The scheduled workflow keeps refreshing the runs against the same base (`private/runtiming/base/<handles>`); the
staging upload sends this feed with its release (`deploy/staging/upload.sh`).
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

from pipeline.common.curated import GENERATED, SOURCE

TIMING = SOURCE / "runtiming" / "timing.json"                  # `runtiming.parse --out`'s default
POLYGONS = GENERATED.runtiming / "cu_polygons.geojson"          # `runtiming.parse --polygons`'s default


def base_for(handles: str, tiles: Path, release: Path, build_dir: Optional[Path] = None) -> Path:
    """The base of `handles`: the one on disk, else built now from the atlas, the tiles and `release`."""
    from pipeline.runtiming.feed import base as B
    home = GENERATED.runtiming / "feed"
    p = home / f"base-{handles}.json.gz"
    if p.is_file():
        return p
    base, report = B.build(build_dir or GENERATED.require_build(), tiles, release)
    p = B.write(base, home)
    print(f"runtiming: built the base for {handles}: {json.dumps(report, sort_keys=True)} -> {p}")
    return p


def bind(release: Path, tiles: Path, out: Path, build_dir: Optional[Path] = None,
         timing: Path = TIMING, polygons: Path = POLYGONS) -> dict:
    """Bind the run-timing feed for `release` (cut against `tiles`) into `out`; returns its index."""
    from pipeline.runtiming.feed import build as F
    meta = json.loads((tiles / "atlas.meta.json").read_text(encoding="utf-8"))
    handles, tv = meta.get("section_handles"), meta.get("tile_version")
    if not handles or not tv:
        raise SystemExit(f"runtiming: the tiles at {tiles} carry no section_handles / tile_version")
    missing = [str(p) for p in (timing, polygons) if not p.is_file()]
    if missing:
        raise SystemExit(f"runtiming: no {', '.join(missing)} — fetch and parse first:\n"
                         "    python -m pipeline.runtiming.fetch.explorer && python -m pipeline.runtiming.fetch.boundaries "
                         "&& python -m pipeline.runtiming.fetch.nuseds && python -m pipeline.runtiming.parse")
    files, index, report = F.build(timing, polygons, base_for(handles, tiles, release, build_dir), tile_version=tv)
    F.check(files, index)
    if index["section_handles"] != handles:
        raise SystemExit(f"runtiming: bound to {index['section_handles']}, the tiles carry {handles}")
    d = F.write(out, files, index, report)
    print(f"runtiming: {json.dumps(report, sort_keys=True)} -> {d} ({handles}, {tv})")
    return index


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline.runtiming.feed.release", description=__doc__.split("\n\n")[0])
    ap.add_argument("--release", type=Path, required=True, help="the release just built (its core names the items)")
    ap.add_argument("--tiles", type=Path, default=None, help="the tiles it is cut against (default: GENERATED.tiles)")
    ap.add_argument("--build", type=Path, default=None, help="the atlas build, for a missing base (default: the live one)")
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args(argv)
    bind(a.release, a.tiles or GENERATED.tiles, a.out, a.build)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
