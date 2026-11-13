"""RUN TIMING FOR ONE RELEASE (T373 "always synced"; T412 owner 2026-11-05: ONE publisher). The release ships the BINDING
BASE of its map, not the feed: `deliver all` runs `stage_base` after the release step, so every release has the base of
its own `section_handles`; the vintage check (`common.vintage.report_all`) refuses a delivery whose base names another.
The run-timing feed itself is built and published ONLY by `.github/workflows/feeds-runtiming.yml`: the staging upload
puts this base in `private/runtiming/base/`, starts the workflow for the release's tiles, and flips the release pointer
last, once run timing for those tiles is on R2 (`deploy/staging/upload.sh`), so map and run timing switch together.

    python -m pipeline.runtiming.feed.release --release DIR [--tiles DIR] [--build DIR] --out DIR          # the base
    python -m pipeline.runtiming.feed.release --local [--tiles DIR] [--build DIR]   # the LOCAL test bed's feed only

`--local` binds the cached timings (`TIMING`, `POLYGONS`: `runtiming.parse`'s defaults) into
`data/generated/runtiming/feed/out`, which `app/tools/serve-tiles.mjs` serves. Never uploaded.
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


def stage_base(release: Path, tiles: Path, out: Path, build_dir: Optional[Path] = None) -> dict:
    """The base of `tiles`' handles into `out` (`base-<handles>.json.gz` + `base.json` naming it); returns base.json."""
    import hashlib
    import shutil
    meta = json.loads((tiles / "atlas.meta.json").read_text(encoding="utf-8"))
    handles, tv = meta.get("section_handles"), meta.get("tile_version")
    if not handles or not tv:
        raise SystemExit(f"runtiming: the tiles at {tiles} carry no section_handles / tile_version")
    src = base_for(handles, tiles, release, build_dir)
    out.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, out / src.name)
    doc = {"section_handles": handles, "tile_version": tv, "base": src.name,
           "sha256": hashlib.sha256((out / src.name).read_bytes()).hexdigest()}
    (out / "base.json").write_text(json.dumps(doc, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"runtiming: base {src.name} staged for {tv}")
    return doc


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
    ap.add_argument("--release", type=Path, default=None, help="the release just built (its core names the items)")
    ap.add_argument("--tiles", type=Path, default=None, help="the tiles it is cut against (default: GENERATED.tiles)")
    ap.add_argument("--build", type=Path, default=None, help="the atlas build, for a missing base (default: the live one)")
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--local", action="store_true", help="bind the cached timings for the local test bed (never uploaded)")
    a = ap.parse_args(argv)
    tiles = a.tiles or GENERATED.tiles
    if a.local:
        from pipeline.deliver.release.manifest import live
        rel = a.release or live(GENERATED.release)
        bind(rel, tiles, a.out or GENERATED.runtiming / "feed" / "out", a.build)
        return 0
    if not a.release or not a.out:
        ap.error("--release and --out (or --local)")
    stage_base(a.release, tiles, a.out, a.build)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
