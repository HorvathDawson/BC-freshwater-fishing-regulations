"""Trim the OSM basemap to what a fishing map in British Columbia actually draws.

`data/source/bc.pmtiles` is a full Protomaps build of OpenStreetMap for the region: 4.36 GB, nine
layers, everything from building footprints to ATMs, over a bounding box that is mostly not
British Columbia. Shipping it whole is the single largest cost in the product — larger than
every regulation, every gauge and our own 875 MB atlas combined.

Four cuts, cheapest first. None of them re-tiles: `pmtiles extract` and `tile-join` both
copy tiles they keep (the feature filter re-encodes the tiles it thins, at the same zoom).

    1. CLIP TO THE PROVINCE.  The fetch bbox spans 48.2-60.0N, 139.1-114.0W — a rectangle
       whose corners are Alberta, Alaska, Washington and the Pacific. British Columbia is
       roughly a third of it. Tiles that do not touch the province are the largest single
       category of waste and the easiest to be sure about.

    2. CAP AT z14.  A z15 basemap is the most expensive level in the archive by a wide margin, and
       the map overzooms the z14 tile past it. NOT z13 (T85, measured): Protomaps keeps some kinds
       and every minor-road name for z14 only, so a z13 cap loses features — see `audit_zoom_cap`.

    3. DROP LAYERS NOBODY LOOKS AT.  Buildings and points of interest. Not `landuse`:
       dropping that was a mistake — the Protomaps style draws ELEVEN layers from it, and
       without it the map loses most of its colour and reads as a grey wireframe.

WHAT STAYS, and why
    earth       the coastline. Without it there is no land, only labels.
    water       lakes, rivers and sea AS THE BASEMAP DRAWS THEM. Ours goes on top.
    landcover   forest and ice — what makes a valley legible at a glance.
    landuse     parks, green space, built-up areas. 11 of the style's layers.
    roads       how you get there. A spot you cannot reach is not a spot.
    places      settlement labels, the thing people navigate by.
    boundaries  the provincial and international lines.

WHAT GOES
    buildings   the largest layer in an OSM extract and invisible above z14 anyway.
                Nobody has ever picked a run by its footprint.
    pois        restaurants, ATMs, benches. A different app's data.

    4. (T85) WITHIN landuse AND water, the features the style never draws — see LANDUSE_DRAWN.

Run: python -m pipeline.deliver.tiles.basemap
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from pipeline.common.curated import CURATED, GENERATED, SOURCE

KEEP = ("earth", "water", "landcover", "landuse", "roads", "places", "boundaries")
DROP = ("buildings", "pois")
MAX_ZOOM = 14

# 4. KEEP ONLY THE FEATURES THE STYLE DRAWS (T85, decisions D32 "the basemap landuse filter"). A
#    feature filter per layer (`tile-join -j`), measured on the live basemap (918.4 MB, 2026-10-24):
#
#    landuse  345.0 MB. The Protomaps style (@protomaps/basemaps 5.7.2) draws only the kinds below;
#             the app also takes its parks and military out (`runtime-style.ts` OURS_NOW: we draw
#             those from the province's own layers). Not drawn, so dropped: park 8.9, nature_reserve
#             5.7, residential 4.0, farmland 2.7, bare_rock 2.6, national_park 0.7, pitch 0.5, ...
#             about 26 MB. `wetland` (52.0 MB) is NOT drawn either and is KEPT on purpose: it is the
#             muted stand-in for the unnamed wetlands the atlas no longer draws (T64), should the map
#             read worse without them — one style layer away, with no basemap rebuild.
#    water    362 MB, of which `stream` LINES are 241.8 MB. The style draws stream lines only from z14
#             (`water_stream`), under our own FWA streams, which the atlas draws at every zoom; the
#             OSM-only ones (culverted urban creeks) carry no regulation. Dropped. River lines (z9+,
#             9.4 MB), every polygon and every label point stay.
#
#    `app/tools/basemap-filter.test.ts` holds these lists to what the app's style draws: a Protomaps
#    upgrade that draws a new kind fails there before it ships blank ground.
LANDUSE_DRAWN = ("aerodrome", "airfield", "allotments", "beach", "cemetery", "college", "dam", "forest",
                 "glacier", "golf_course", "grass", "grassland", "hospital", "industrial", "pedestrian", "pier",
                 "playground", "runway", "sand", "school", "scrub", "taxiway", "university", "village_green",
                 "wood", "zoo")
LANDUSE_STANDIN = ("wetland",)
WATER_LINES_KEPT = ("river",)
WATER_LINES_DROPPED = ("stream",)


def feature_filter() -> dict:
    """The per-layer `tile-join -j` filter (cut 4): what the style draws, and nothing else."""
    return {
        "landuse": ["in", "kind", *LANDUSE_DRAWN, *LANDUSE_STANDIN],
        "water": ["any", ["!=", "$type", "LineString"], ["in", "kind", *WATER_LINES_KEPT]],
    }

# The clip is generous on purpose. A tile that straddles the border carries ground on both
# sides, and losing the far bank of a boundary river to save a few megabytes would be a bad
# trade — the Columbia and the Kootenay both leave and re-enter the province.
BUFFER_M = 5_000


def _boundary_4326(src: Path, out: Path) -> Path:
    """BC's outline in WGS84, buffered. `pmtiles extract` needs lon/lat; ours is BC Albers."""
    import geopandas as gpd

    gdf = gpd.read_file(src)
    gdf = gdf.to_crs(3005)                       # buffer in metres, not degrees
    gdf["geometry"] = gdf.geometry.buffer(BUFFER_M)
    gdf = gdf.to_crs(4326)
    # pmtiles wants a bare Polygon/MultiPolygon, not a FeatureCollection.
    geom = gdf.geometry.union_all()
    out.write_text(json.dumps(geom.__geo_interface__))
    return out


def build(src: Path, out: Path, boundary: Path, maxzoom: int = MAX_ZOOM) -> Path:
    _tools()
    if not src.exists():
        raise FileNotFoundError(f"{src} not found")

    before = src.stat().st_size
    out.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmp:
        region = _boundary_4326(boundary, Path(tmp) / "bc.geojson")
        print(f"  {src.name}  {before / 1e9:.2f} GB")
        print(f"  1. clipping to British Columbia (+{BUFFER_M / 1000:.0f} km) and z<={maxzoom}")
        print(f"  2. dropping {', '.join(DROP)}")
        print(f"  3. keeping only the landuse kinds and water lines the style draws "
              f"(water lines dropped: {', '.join(WATER_LINES_DROPPED)})")
        _trim(src, out, Path(tmp), [f"--region={region}"], maxzoom)

    after = out.stat().st_size
    print(f"  ✅ {after / 1e9:.2f} GB  ({100 * (1 - after / before):.0f}% smaller overall)")
    return out


def _tools() -> None:
    for tool in ("pmtiles", "tile-join"):
        if shutil.which(tool) is None:
            raise FileNotFoundError(f"{tool} is not on $PATH")


def _trim(src: Path, out: Path, tmp: Path, clip: list[str], maxzoom: int) -> None:
    """Cuts 1-4 of the build, over `clip` (`--region=<geojson>` for the province, `--bbox=...` for an audit
    extent): the ONE code path the build and the zoom-cap audit share, so the audit measures what ships."""
    clipped = tmp / f"clipped-z{maxzoom}.pmtiles"
    _run(["pmtiles", "extract", str(src), str(clipped), *clip, f"--maxzoom={maxzoom}"])
    # Keeping by name, not excluding: a future Protomaps schema that adds a layer
    # should not start shipping it without anyone deciding to.
    _run(["tile-join", "-f", "-o", str(out), "--no-tile-size-limit",
          *[a for lyr in KEEP for a in ("-l", lyr)],
          "-j", json.dumps(feature_filter(), separators=(",", ":")), str(clipped)])


# THE ZOOM-CAP AUDIT (T85; owner 2026-10-24: "basemap to z13 too, on two conditions: no feature may be lost —
# measure feature counts / kinds z14-sourced vs z13 — and the map must still overzoom past z13").
#
# A cap is free only if the capped tile holds everything the deeper one does, because the map overzooms the capped
# tile in its place. Protomaps does not promise that: it gives some kinds a minimum zoom of 14 and strips minor-road
# names below z14. So the audit BUILDS the basemap twice over small real extents — `_trim`, the build's own path, to
# `deep` and to `cap` — and compares the deep build's deepest tiles with the capped build's: every feature kind
# (layer, kind, kind_detail) and every named feature (layer, kind, name) of the deep build must still be in the
# capped one. Fragment counts are reported, not compared: a z13 tile covers four z14 tiles, so a line crossing them
# is one fragment instead of four.
#
# MEASURED 2026-10-24 (`python -m pipeline.deliver.tiles.basemap --audit-zoom 13`): NOT lossless. Squamish loses 6
# kinds (path sidewalk / crossing, other living_street / ladder, rail narrow_gauge, landuse pedestrian — drawn by the
# style) and 426 names, nearly all minor-road and path names; Lac Le Jeune loses 75 names, its forest service roads
# among them (how an angler reaches the lake). So MAX_ZOOM stays 14, and a shallower `--max-zoom` is refused unless
# its audit is clean.
AUDIT_EXTENTS = {
    "squamish (town)": (-123.18, 49.68, -123.10, 49.74),
    "lac le jeune (rural)": (-120.52, 50.44, -120.42, 50.50),
}


def census(archive: Path, zoom: int) -> tuple[dict, set]:
    """The features of `archive`'s tiles at `zoom`: fragments per (layer, kind, kind_detail), and the set of
    (layer, kind, name) named features. Read with `tippecanoe-decode`, which ships with `tile-join`."""
    from collections import Counter
    r = subprocess.run(["tippecanoe-decode", f"-Z{zoom}", f"-z{zoom}", str(archive)],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"tippecanoe-decode failed ({r.returncode})\n{r.stderr[-3000:]}")
    kinds: Counter = Counter()
    names: set = set()
    for tile in json.loads(r.stdout).get("features", []):
        for layer in tile.get("features", []):
            lyr = layer["properties"]["layer"]
            for f in layer.get("features", []):
                p = f.get("properties") or {}
                kinds[(lyr, p.get("kind"), p.get("kind_detail"))] += 1
                if p.get("name"):
                    names.add((lyr, p.get("kind"), p["name"]))
    return dict(kinds), names


def zoom_cap_loss(deep: tuple[dict, set], capped: tuple[dict, set]) -> dict:
    """What the capped census lacks: `kinds` (kind -> fragments in the deep build) and `names`. Empty: lossless."""
    (dk, dn), (ck, cn) = deep, capped
    return {"kinds": {k: n for k, n in dk.items() if k not in ck}, "names": sorted(dn - cn, key=str)}


def audit_zoom_cap(src: Path, cap: int, deep: int = MAX_ZOOM, extents: dict | None = None) -> dict:
    """Per extent: the deep build's tiles at `deep` vs the capped build's at `cap` (`zoom_cap_loss`), with both
    fragment counts per kind for the record."""
    _tools()
    if shutil.which("tippecanoe-decode") is None:
        raise FileNotFoundError("tippecanoe-decode is not on $PATH")
    out = {}
    for label, bbox in (extents or AUDIT_EXTENTS).items():
        with tempfile.TemporaryDirectory() as tmp:
            t = Path(tmp)
            clip = ["--bbox=" + ",".join(str(v) for v in bbox)]
            built = {}
            for z in (deep, cap):
                _trim(src, t / f"z{z}.pmtiles", t, clip, z)
                built[z] = census(t / f"z{z}.pmtiles", z)
        dk, ck = built[deep][0], built[cap][0]
        out[label] = {**zoom_cap_loss(built[deep], built[cap]),
                      "fragments": {k: (dk.get(k, 0), ck.get(k, 0)) for k in sorted(set(dk) | set(ck), key=str)}}
    return out


def lossless(report: dict) -> bool:
    return all(not r["kinds"] and not r["names"] for r in report.values())


def print_audit(report: dict, cap: int, deep: int = MAX_ZOOM) -> None:
    for label, r in report.items():
        print(f"  z{deep} -> z{cap}  {label}: {len(r['kinds'])} kinds lost, {len(r['names'])} names lost")
        for k, n in sorted(r["kinds"].items(), key=str):
            print(f"     kind lost  {k}  ({n} fragments at z{deep})")
        for n in r["names"][:20]:
            print(f"     name lost  {n}")
        if len(r["names"]) > 20:
            print(f"     ... and {len(r['names']) - 20} more names")


def _run(cmd: list[str]) -> None:
    # Both tools name every tile they touch; that is megabytes of progress noise.
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"{cmd[0]} failed ({r.returncode})\n{r.stderr[-3000:]}")


#: THE BASEMAP'S VERSION (T75): `basemap-<sha12 of the archive>`, content-addressed like a tile version, so a
#: rebuilt basemap is a new id and an unchanged one is the same id (the app downloads it only when the id moves).
META = "basemap.meta.json"


def version_of(archive: Path) -> str:
    from pipeline.deliver.tiles.version import sha256_file
    return f"basemap-{sha256_file(archive, 12)}"


def to_release(archive: Path, release_root: Path, meta_dir: Path) -> dict:
    """`<release_root>/basemap/<id>/basemap-<id>.pmtiles` (immutable: an existing folder with other bytes is
    refused) and `<meta_dir>/basemap.meta.json` — `{basemap_version, bytes, sha256}`, the sidecar the release step
    names the basemap from (`release.manifest.basemap_block`). Returns the sidecar."""
    from pipeline.deliver.tiles.version import sha256_file
    vid = version_of(archive)
    final = Path(release_root) / "basemap" / vid
    dst = final / f"{vid}.pmtiles"
    sha = sha256_file(archive)
    if final.exists():
        if not dst.exists() or sha256_file(dst) != sha:
            raise SystemExit(f"basemap: {final} exists with other bytes — a released basemap is immutable")
    else:
        stage = final.with_name(f".{vid}.staging")
        shutil.rmtree(stage, ignore_errors=True)
        stage.mkdir(parents=True)
        try:
            shutil.copy2(archive, stage / dst.name)
            stage.rename(final)
        finally:
            shutil.rmtree(stage, ignore_errors=True)
    meta = {"basemap_version": vid, "bytes": archive.stat().st_size, "sha256": sha}
    (Path(meta_dir) / META).write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    print(f"  released: {final}  {meta}")
    return meta


def main(argv=None) -> int:
    # DELIBERATELY ITS OWN ENTRY POINT, and nothing else calls it.
    #
    # This is the OSM half: take Protomaps' 4.36 GB planet extract, clip it to BC and drop
    # the layers we do not draw. It depends on `data/source/bc.pmtiles` and on NOTHING this
    # pipeline builds — no graph, no registry, no atlas — so it must never be a step of
    # `python -m pipeline.deliver.tiles`, which is the ATLAS build and runs on every
    # geometry change. The basemap changes when Protomaps publishes, which is rarely.
    #
    #     python -m pipeline.deliver.tiles.basemap   the OSM ground: clip + reduce, then release it
    #     python -m pipeline.deliver.tiles           the atlas: our water, from a build
    #
    # T75: the built archive is RELEASED like a tile version — `data/release/basemap/<id>/` and the sidecar
    # `basemap.meta.json` beside it — so the next `python -m pipeline.deliver release` names it in the manifest
    # (`basemap: {id, url, bytes, sha256}`) and a native install fetches it with a sha check.
    import argparse
    ap = argparse.ArgumentParser(prog="python -m pipeline.deliver.tiles.basemap")
    ap.add_argument("--out", type=Path, default=GENERATED.tiles / "basemap.pmtiles",
                    help="the basemap archive (default: the tiles dir's; its sidecar is written beside it)")
    ap.add_argument("--release-root", type=Path, default=GENERATED.release)
    ap.add_argument("--release-only", action="store_true",
                    help="do not rebuild: release the archive already at --out (stamp its version, copy it)")
    ap.add_argument("--no-release", action="store_true", help="build only (a scratch build)")
    ap.add_argument("--max-zoom", type=int, default=MAX_ZOOM,
                    help=f"cap the build here (default {MAX_ZOOM}); a shallower cap must pass the zoom-cap audit")
    ap.add_argument("--audit-zoom", type=int, metavar="CAP",
                    help=f"audit only: does a CAP build lose a feature the z{MAX_ZOOM} build has? (small extents)")
    a = ap.parse_args(argv)
    if a.audit_zoom is not None:
        report = audit_zoom_cap(SOURCE / "bc.pmtiles", a.audit_zoom)
        print_audit(report, a.audit_zoom)
        return 0 if lossless(report) else 1
    if not a.release_only:
        if a.max_zoom < MAX_ZOOM:
            report = audit_zoom_cap(SOURCE / "bc.pmtiles", a.max_zoom)
            print_audit(report, a.max_zoom)
            if not lossless(report):
                raise SystemExit(f"basemap: a z{a.max_zoom} cap loses features the z{MAX_ZOOM} build has — refused")
        build(SOURCE / "bc.pmtiles", a.out, SOURCE / "bc_boundary.geojson", a.max_zoom)
    if not a.no_release:
        to_release(a.out, a.release_root, a.out.parent)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
