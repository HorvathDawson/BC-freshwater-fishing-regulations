"""The atlas's DERIVED SIDECARS — the per-build facts `pipeline.atlas.build` writes beside
`registry.json` that nothing downstream may compute again — and a writer for an atlas that
predates them.

    python -m pipeline.atlas.sidecars --build data/generated/atlas/full --out /tmp/full_p1

Writes, from the atlas's own artifacts and the curated files the build read:
  registry.json             `registry.flowing.merge_flowing_polygons` — ONE flowing water, lines and
                            polygons: a river's polygons in its item, every flowing polygon
                            `stream`-kind, absorbed ids in `aliases` (`merge_report.json` beside it);
                            `registry.add_lake_parts` — `part_of` on every curated lake part
                            (`added_lakes.geojson`; the parts and their lakes must be items here),
                            and a lake with parts owns no section
  item_points.json          one pin per item of the registry written (an absorbed item's pin goes;
                            a new polygon-only water takes its lowest member's)
  region_home.json          `registry.regions.write_homes` — the region each straddling section
                            lies in (region polygons × outlines × lines of THIS atlas)
  splits.resolved.json      `source` and the authored `anchor_offset_m` / `anchor_offset_dir`
                            on every cut resolved from a curated split (`splits.json`)

With `--out`, a new build directory is made that holds every other artifact of `--build` as a
symlink and these written fresh — the section handle table (and so the digest every derivative is
keyed by) is untouched: the registry pass changes item MEMBERSHIP and KIND, never a section. Without
it, the files are written into `--build` in place, which is refused for a promoted atlas unless
`--force` (`vintage.promoted_atlas`).

This is a MIGRATION for an atlas built before the fields existed: a new build writes all of them
itself, by the same functions. It reads the same inputs the build does, so the two cannot disagree;
a curated file edited since the atlas was built is refused where the atlas and the file disagree
(a part that is not an item, a curated split the atlas did not resolve). Running it on an atlas
that already has them changes nothing (the registry pass is idempotent).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from pipeline.common.curated import CURATED, GENERATED
from pipeline.atlas.splits.splits import dump_resolved, order_resolved

SIDECARS = ("region_home.json", "registry.json", "item_points.json", "merge_report.json",
            "splits.resolved.json")


def enrich_resolved(rows: list[dict], curated_defs, *, strict: bool = True) -> list[dict]:
    """`source` and the authored offsets on every resolved row, from the curated split defs
    (`splits.load_split_defs`): a row whose id is a curated split is `curated` (or `gauge` by its
    anchor type), every other row by its id's family. A curated split the atlas did not resolve is
    reported, not an error (8 of 405 on the promoted build); a resolved curated id the file no longer
    has is refused under `strict` (the file changed since the build)."""
    defs = {d.id: d for d in curated_defs}
    out = []
    for r in rows:
        r = dict(r)
        sid, kind = r["split_id"], r.get("anchor_type")
        d = defs.get(sid)
        if kind == "gauge":
            r["source"] = "gauge"
        elif d is not None:
            r["source"] = "curated"
            if d.anchor.offset_m and d.anchor.offset_dir:
                r["anchor_offset_m"] = round(float(d.anchor.offset_m), 1)
                r["anchor_offset_dir"] = d.anchor.offset_dir
        elif sid.startswith("area:"):
            r["source"] = "area"
        elif sid.startswith("length:"):
            r["source"] = "length"
        elif sid.startswith("border:"):
            r["source"] = "border"
        elif strict:
            raise SystemExit(f"sidecars: resolved split {sid!r} ({kind}) is not in "
                             f"{CURATED.waters.splits} and is no atlas-made cut — the curated file "
                             f"changed since this atlas was built; rebuild the atlas")
        # the keys in the build writer's order, so the migrated file reads as a fresh one
        out.append(order_resolved(r))
    return out


def write_sidecars(build: Path, out: Path | None = None, *, log=print) -> Path:
    """Write the three sidecars for `build` into `out` (a symlink farm over `build`) or in place."""
    from pipeline.atlas.registry import add_lake_parts, load_registry, regions, write_registry
    from pipeline.atlas.splits.splits import load_split_defs
    from pipeline.atlas.waters.added_lakes.ingest import lake_parts, load as load_added_lakes

    build = Path(build).resolve()
    dest = Path(out).resolve() if out else build
    if dest != build:
        dest.mkdir(parents=True, exist_ok=True)
        for p in sorted(build.iterdir()):
            if p.name in SIDECARS:
                continue
            link = dest / p.name
            if link.is_symlink() or link.exists():
                continue
            os.symlink(p, link)
        log(f"  {dest}: every other artifact of {build.name} linked")
    from pipeline.atlas.registry.flowing import merge_flowing_polygons
    from pipeline.common.io.serialize import read_artifact

    registry = load_registry(str(build / "registry.json"))
    # ONE FLOWING WATER (`registry.flowing`): the same pass the atlas build runs, over the same
    # graph and outlines; the handles are untouched (membership and kind change, no section).
    graph = read_artifact(str(build / "graph.pkl"))
    polys = read_artifact(str(build / "waterbody_polys.pkl"))
    rep: dict = {}
    before = len(registry)
    registry = merge_flowing_polygons(registry, graph, polys, rep)
    (dest / "merge_report.json").write_text(json.dumps(rep, indent=1) + "\n", encoding="utf-8")
    log(f"  registry.json: {before:,} -> {len(registry):,} items; {rep['absorbed']} folded into "
        f"{len(rep['merged'])} stream(s) + {len(rep['own_items'])} of their own; "
        f"{sum(len(x['polygons']) for x in rep['unnamed_polygons'])} unnamed polygon(s) taken "
        f"by a slough -> merge_report.json")
    registry = add_lake_parts(registry, lake_parts(load_added_lakes()))
    n_parts = sum(1 for it in registry.values() if it.part_of)
    # A water outside an area its polygon touches (`areas.json` `outside_items`): the same pass
    # the atlas build runs, on the same file (`registry.outside_area_items`).
    from pipeline.atlas.registry import outside_area_items
    from pipeline.atlas.splits.area_splits import load_area_split_defs
    registry = outside_area_items(registry, load_area_split_defs())
    write_registry(registry, dest / "registry.json")
    log(f"  registry.json: {n_parts} lake part(s) carry part_of; their lakes own no section")
    pins_path = build / "item_points.json"
    if pins_path.exists():
        pins = json.loads(pins_path.read_text(encoding="utf-8"))
        for iid, it in registry.items():
            if iid not in pins:
                got = [pins[a] for a in sorted(it.aliases) if a in pins]
                if got:
                    pins[iid] = got[0]
        pins = {k: v for k, v in pins.items() if k in registry}
        (dest / "item_points.json").write_text(json.dumps(pins, separators=(",", ":")),
                                               encoding="utf-8")
        log(f"  item_points.json: {len(pins):,} pins")
    del graph, polys
    # geometry from the atlas, the file into `dest`: a promoted atlas is never written into
    homes = regions.write_homes(build, registry, out_dir=dest)
    log(f"  {regions.HOME_FILE}: {len(homes):,} straddling section(s)")
    rows = json.loads((build / "splits.resolved.json").read_text(encoding="utf-8"))
    rows = enrich_resolved(rows, load_split_defs(str(CURATED.waters.splits)))
    # the build's own writer (`splits.write_resolved`), so a migrated file is byte-equal to a fresh one
    (dest / "splits.resolved.json").write_text(dump_resolved(rows), encoding="utf-8")
    n_cur = sum(1 for r in rows if r.get("source") == "curated")
    n_off = sum(1 for r in rows if r.get("anchor_offset_m"))
    log(f"  splits.resolved.json: {len(rows):,} cuts, {n_cur} curated ({n_off} at an authored "
        f"offset)")
    return dest


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline.atlas.sidecars",
                                 description=__doc__.split("\n\n")[0])
    ap.add_argument("--build", type=Path, default=None, help="the atlas (default: the promoted one)")
    ap.add_argument("--out", type=Path, default=None,
                    help="write a new build dir here (symlinks + the three sidecars) instead of in "
                         "place")
    ap.add_argument("--force", action="store_true", help="write in place into a promoted atlas")
    a = ap.parse_args(argv)
    build = a.build or GENERATED.require_build()
    if a.out is None and not a.force:
        from pipeline.common.vintage import promoted_atlas
        carrier = promoted_atlas(build, GENERATED.tiles, GENERATED.bundle / "bundle.sqlite")
        if carrier:
            raise SystemExit(f"sidecars: {build} is promoted ({carrier} carries its digest) — "
                             f"write beside it with --out, or --force to write in place (adds "
                             f"files, never touches the handle table)")
    write_sidecars(build, a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
