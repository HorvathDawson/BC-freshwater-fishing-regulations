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

    2. CAP AT z13 (T202, owner 2026-10-27; it was z14 until then). The camera overzooms the z13 tile
       to z18. z14 was 274 of the 629 MB. Protomaps strips every minor-road, path and track NAME
       below z14, so the z13 `roads` layer is REBUILT from the z14 roads (`basemap_roads`). The other
       z14-only content is urban, and the owner accepted losing it: sidewalks, crossings, living
       streets, subway, pedestrian areas, oneway arrows, three Vancouver labels. `audit_zoom_cap`
       proves on six real extents that nothing else is lost, and the build refuses to run if it is.

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

    4. (T85, T202) WITHIN landuse, water AND earth, the features the style never draws — see
       LANDUSE_DRAWN and `feature_filter` — and `sort_rank`, a key nothing reads.

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
MAX_ZOOM = 13
# The source's own deepest zoom: the z13 roads are rebuilt from it, and the zoom-cap audit compares against it.
SOURCE_ZOOM = 14
# Keys no style layer reads (`@protomaps/basemaps` 5.7.2; checked by `app/tools/basemap-filter.test.ts`). Dropped from
# every layer by `tile-join -x`.
UNREAD_KEYS = ("sort_rank",)

# 4. KEEP ONLY THE FEATURES THE STYLE DRAWS (T85, decisions D32 "the basemap landuse filter"). A
#    feature filter per layer (`tile-join -j`), measured on the live basemap (918.4 MB, 2026-10-24):
#
#    landuse  345.0 MB. The Protomaps style (@protomaps/basemaps 5.7.2) draws only the kinds below;
#             the app also takes its parks and military out (`runtime-style.ts` OURS_NOW: we draw
#             those from the province's own layers). Not drawn, so dropped: park 8.9, nature_reserve
#             5.7, residential 4.0, farmland 2.7, bare_rock 2.6, national_park 0.7, pitch 0.5, ...
#             about 26 MB.
#             `wetland` (51.5 MB) is not drawn either. T85 kept it as a stand-in for the unnamed wetlands
#             the atlas no longer draws (T64). The owner dropped it on 2026-10-27 (T202, about −32 MB at
#             z≤13). TO BRING IT BACK: put "wetland" back in LANDUSE_STANDIN, rebuild the basemap
#             (`python -m pipeline.deliver.tiles.basemap`), and add a style layer over the `landuse`
#             source-layer filtered to `kind == "wetland"` in `runtime-style.ts`, below our water. The
#             data is still in `data/source/bc.pmtiles`.
#    water    362 MB, of which `stream` LINES are 241.8 MB. The style draws stream lines only from z14
#             (`water_stream`), under our own FWA streams, which the atlas draws at every zoom; the
#             OSM-only ones (culverted urban creeks) carry no regulation. Dropped. River lines (z9+,
#             9.4 MB) and every polygon stay.
#             Of the label points (T202), only the sea's stay (`water_label_ocean`: sea, ocean, bay,
#             strait, fjord). Lake and river points (kind `water` / `lake`) feed only `water_label_lakes`,
#             which the app removes because we name water from the FWA.
#    earth    (T202) its polygons and island label points stay; its lines (cliffs) have no style layer.
#
#    `app/tools/basemap-filter.test.ts` holds these lists to what the app's style draws: a Protomaps
#    upgrade that draws a new kind fails there before it ships blank ground.
LANDUSE_DRAWN = ("aerodrome", "airfield", "allotments", "beach", "cemetery", "college", "dam", "forest",
                 "glacier", "golf_course", "grass", "grassland", "hospital", "industrial", "pedestrian", "pier",
                 "playground", "runway", "sand", "school", "scrub", "taxiway", "university", "village_green",
                 "wood", "zoo")
LANDUSE_STANDIN = ()
WATER_LINES_KEPT = ("river",)
WATER_LINES_DROPPED = ("stream",)
WATER_POINTS_DROPPED = ("water", "lake")


def feature_filter() -> dict:
    """The per-layer `tile-join -j` filter (cut 4): what the style draws, and nothing else."""
    return {
        "landuse": ["in", "kind", *LANDUSE_DRAWN, *LANDUSE_STANDIN],
        "water": ["any", ["==", "$type", "Polygon"],
                  ["all", ["==", "$type", "LineString"], ["in", "kind", *WATER_LINES_KEPT]],
                  ["all", ["==", "$type", "Point"], ["!in", "kind", *WATER_POINTS_DROPPED]]],
        "earth": ["!=", "$type", "LineString"],
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
        print(f"  3. keeping only the landuse kinds, water lines and label points the style draws "
              f"(water lines dropped: {', '.join(WATER_LINES_DROPPED)}; keys dropped: {', '.join(UNREAD_KEYS)})")
        if maxzoom < SOURCE_ZOOM:
            print(f"  4. rebuilding the z{maxzoom} roads from the z{maxzoom + 1} roads, names and all")
        _trim(src, out, Path(tmp), [f"--region={region}"], maxzoom)

    after = out.stat().st_size
    print(f"  ✅ {after / 1e9:.2f} GB  ({100 * (1 - after / before):.0f}% smaller overall)")
    return out


def _tools() -> None:
    for tool in ("pmtiles", "tile-join", "tippecanoe"):
        if shutil.which(tool) is None:
            raise FileNotFoundError(f"{tool} is not on $PATH")


# tippecanoe's worker threads (heavy jobs run with at most 4 workers; scripts/heavy.sh).
THREADS = "4"


def _trim(src: Path, out: Path, tmp: Path, clip: list[str], maxzoom: int) -> None:
    """Cuts 1-4 of the build, over `clip` (`--region=<geojson>` for the province, `--bbox=...` for an audit
    extent): the ONE code path the build and the zoom-cap audit share, so the audit measures what ships.

    Below the source's own depth, the top zoom's `roads` are rebuilt from the next zoom's (`basemap_roads`). The
    archive is joined from three parts: z<top with every kept layer, z=top without roads, and the rebuilt roads."""
    deep = min(maxzoom + 1, SOURCE_ZOOM)
    clipped = tmp / f"clipped-z{deep}.pmtiles"
    _run(["pmtiles", "extract", str(src), str(clipped), *clip, f"--maxzoom={deep}"])
    # Keeping by name, not excluding: a future Protomaps schema that adds a layer
    # should not start shipping it without anyone deciding to.
    join = ["tile-join", "-f", "--no-tile-size-limit", *[a for k in UNREAD_KEYS for a in ("-x", k)],
            "-j", json.dumps(feature_filter(), separators=(",", ":"))]
    if maxzoom >= SOURCE_ZOOM:
        _run([*join, "-o", str(out), *[a for lyr in KEEP for a in ("-l", lyr)], str(clipped)])
        return
    from pipeline.deliver.tiles.basemap_roads import roads_geojsonl
    low, top, roads = (tmp / f"{p}-z{maxzoom}.pmtiles" for p in ("low", "top", "roads"))
    parts = [top]
    _run([*join, "-o", str(top), f"-Z{maxzoom}", f"-z{maxzoom}",
          *[a for lyr in KEEP if lyr != "roads" for a in ("-l", lyr)], str(clipped)])
    if maxzoom > 0:
        _run([*join, "-o", str(low), f"-z{maxzoom - 1}", *[a for lyr in KEEP for a in ("-l", lyr)], str(clipped)])
        parts.insert(0, low)
    lines = tmp / f"roads-z{deep}.geojsonl"
    if roads_geojsonl(clipped, lines, zoom=deep):
        _run(["tippecanoe", "-q", "-f", "-o", str(roads), f"-Z{maxzoom}", f"-z{maxzoom}", "-l", "roads",
              "--no-tile-size-limit", "--no-feature-limit", "-P", str(lines)])
        parts.append(roads)
    _run(["tile-join", "-f", "--no-tile-size-limit", "-o", str(out), *map(str, parts)])


# THE ZOOM-CAP AUDIT (T85; owner 2026-10-24: "basemap to z13 too, on two conditions: no feature may be lost —
# measure feature counts / kinds z14-sourced vs z13 — and the map must still overzoom past z13"; owner 2026-10-27,
# T202: "KEEP what anglers use outside towns — trails/paths, forest service roads and resource roads, their names;
# DROP what only matters in cities").
#
# A cap is free only if the capped tile holds everything the deeper one does, because the map overzooms the capped
# tile in its place. Protomaps does not promise that: it gives some kinds a minimum zoom of 14 and strips minor-road
# names below z14. So the audit BUILDS each extent twice through the build's own path (`_trim`): to `deep`, the
# source's z14, and to `cap`, which includes the roads rebuild. It then compares the deep build's deepest tiles with
# the capped build's:
#   - every feature kind (layer, kind, kind_detail) must still be there;
#   - every named feature (layer, kind, name) must still be there;
#   - every road kind must keep at least SHORT_RATIO of its line length inside the extent. Lengths are clipped to the
#     bbox; tile buffers make them noisy by a few percent, so only a real shortfall shows.
# Only the urban losses the owner accepted (ACCEPTED_KINDS, ACCEPTED_NAMES) are allowed. They are reported, never
# silently dropped.
#
# MEASURED 2026-10-24, plain z13 cap: Squamish lost 6 kinds and 426 names, Lac Le Jeune 75 names (its FSRs).
# MEASURED 2026-10-27 with the roads rebuild: 0 unaccepted losses on all six extents (T202-basemap-audit.md).
AUDIT_EXTENTS = {
    "squamish (town)": (-123.18, 49.68, -123.10, 49.74),
    "lac le jeune (rural)": (-120.52, 50.44, -120.42, 50.50),
    "vancouver (city)": (-123.27, 49.19, -123.02, 49.32),
    "chilcotin / farwell canyon (FSRs)": (-123.0, 51.6, -122.4, 52.0),
    "squamish backcountry (trails)": (-123.35, 49.75, -122.95, 50.05),
}
# z14-only kinds the owner accepted losing (2026-10-27): urban paths and rail, the pedestrian-area fill, drains. The
# roads among them are also left out of the rebuild (`basemap_roads.URBAN_ONLY`).
ACCEPTED_KINDS = frozenset({("roads", k) for k in ("sidewalk", "crossing", "corridor", "living_street", "platform",
                                                   "services", "rest_area", "subway", "miniature", "ladder",
                                                   "narrow_gauge")} | {("landuse", "pedestrian")}
                           # a z14-only drain polygon in Vancouver (2026-10-27); ditches carry no regulation, our
                           # water is the FWA's
                           | {("water", "drain")})
# Label points that exist at z14 only, accepted by name (owner 2026-10-27).
ACCEPTED_NAMES = frozenset({"Deadman's Island", "Richmond Island", "Coal Harbour"})
SHORT_RATIO = 0.9


def _accepted(layer: str, kind, kind_detail) -> bool:
    return (layer, kind_detail) in ACCEPTED_KINDS or (layer, kind) in ACCEPTED_KINDS


def _tile_box(z: int, x: int, y: int):
    import math
    from shapely.geometry import box
    n = 2 ** z
    lat = lambda t: math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * t / n))))
    return box(x / n * 360 - 180, lat(y + 1), (x + 1) / n * 360 - 180, lat(y))


def census(archive: Path, zoom: int, bbox: tuple | None = None) -> tuple[dict, set, dict]:
    """The features of `archive`'s tiles at `zoom`: fragments per (layer, kind, kind_detail), the set of
    (layer, kind, name, kind_detail) named features, and the km of road line per (layer, kind, kind_detail). Read
    with `tippecanoe-decode`, which ships with `tile-join`.

    With `bbox`, only what lies inside it AND inside its own tile counts. A tile's buffer holds its neighbours' edges,
    so without the tile clip a deep build (more tile edges) would count those strips twice, and a name seen only in
    the buffer of an edge tile would look lost."""
    import math
    from collections import Counter, defaultdict
    from shapely.geometry import box, shape
    r = subprocess.run(["tippecanoe-decode", f"-Z{zoom}", f"-z{zoom}", str(archive)],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"tippecanoe-decode failed ({r.returncode})\n{r.stderr[-3000:]}")
    kinds: Counter = Counter()
    names: set = set()
    km: dict = defaultdict(float)
    sx = 111.32 * math.cos(math.radians((bbox[1] + bbox[3]) / 2)) if bbox else 0
    for tile in json.loads(r.stdout).get("features", []):
        tp = tile.get("properties") or {}
        clip = box(*bbox).intersection(_tile_box(tp["zoom"], tp["x"], tp["y"])) if bbox else None
        for layer in tile.get("features", []):
            lyr = layer["properties"]["layer"]
            for f in layer.get("features", []):
                p = f.get("properties") or {}
                g = None
                if clip is not None:
                    g = shape(f["geometry"]).intersection(clip)
                    if g.is_empty:
                        continue
                k = (lyr, p.get("kind"), p.get("kind_detail"))
                kinds[k] += 1
                if p.get("name"):
                    names.add((lyr, p.get("kind"), p["name"], p.get("kind_detail")))
                if g is not None and lyr == "roads" and "Line" in g.geom_type:
                    for part in getattr(g, "geoms", [g]):
                        c = list(getattr(part, "coords", []))
                        km[k] += sum(math.hypot((x2 - x1) * sx, (y2 - y1) * 110.57)
                                     for (x1, y1), (x2, y2) in zip(c, c[1:]))
    return dict(kinds), names, dict(km)


def zoom_cap_loss(deep: tuple, capped: tuple) -> dict:
    """What the capped census lacks: `kinds` (kind -> fragments in the deep build), `names`, and `short` (road kind
    -> (deep km, capped km) under SHORT_RATIO). Accepted urban losses are split out into `accepted`. Lossless when
    `kinds`, `names` and `short` are all empty."""
    dk, dn, dkm = (*deep, {})[:3]
    ck, cn, ckm = (*capped, {})[:3]
    lost_kinds = {k: n for k, n in dk.items() if k not in ck}
    lost_names = sorted(dn - cn, key=str)
    short = {k: (round(v, 2), round(ckm.get(k, 0.0), 2)) for k, v in dkm.items()
             if v > 0.5 and ckm.get(k, 0.0) < SHORT_RATIO * v and k not in lost_kinds}
    acc_k = {k: n for k, n in lost_kinds.items() if _accepted(*k)}
    acc_n = [n for n in lost_names if _accepted(n[0], n[1], n[3] if len(n) > 3 else None) or n[2] in ACCEPTED_NAMES]
    acc_s = {k: v for k, v in short.items() if _accepted(*k)}
    return {"kinds": {k: n for k, n in lost_kinds.items() if k not in acc_k},
            "names": [n for n in lost_names if n not in acc_n],
            "short": {k: v for k, v in short.items() if k not in acc_s},
            "accepted": {"kinds": acc_k, "names": acc_n, "short": acc_s}}


def audit_zoom_cap(src: Path, cap: int, deep: int = SOURCE_ZOOM, extents: dict | None = None) -> dict:
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
                (t / str(z)).mkdir(exist_ok=True)
                _trim(src, t / f"z{z}.pmtiles", t / str(z), clip, z)
                built[z] = census(t / f"z{z}.pmtiles", z, bbox)
        dk, ck = built[deep][0], built[cap][0]
        out[label] = {**zoom_cap_loss(built[deep], built[cap]),
                      "fragments": {k: (dk.get(k, 0), ck.get(k, 0)) for k in sorted(set(dk) | set(ck), key=str)}}
    return out


def lossless(report: dict) -> bool:
    return all(not r["kinds"] and not r["names"] and not r.get("short") for r in report.values())


def print_audit(report: dict, cap: int, deep: int = SOURCE_ZOOM) -> None:
    for label, r in report.items():
        a = r.get("accepted", {})
        print(f"  z{deep} -> z{cap}  {label}: {len(r['kinds'])} kinds lost, {len(r['names'])} names lost, "
              f"{len(r.get('short', {}))} road kinds short  (accepted urban: {len(a.get('kinds', {}))} kinds, "
              f"{len(a.get('names', []))} names)")
        for k, n in sorted(r["kinds"].items(), key=str):
            print(f"     kind lost  {k}  ({n} fragments at z{deep})")
        for k, v in sorted(r.get("short", {}).items(), key=str):
            print(f"     road kind short  {k}  {v[0]} km at z{deep} -> {v[1]} km at z{cap}")
        for n in r["names"][:20]:
            print(f"     name lost  {n}")
        if len(r["names"]) > 20:
            print(f"     ... and {len(r['names']) - 20} more names")


def _run(cmd: list[str]) -> None:
    # Both tools name every tile they touch; that is megabytes of progress noise.
    import os
    r = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, "TIPPECANOE_MAX_THREADS": THREADS})
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
                    help=f"cap the build here (default {MAX_ZOOM}); any cap below the source's z{SOURCE_ZOOM} must "
                         f"pass the zoom-cap audit, which runs first")
    ap.add_argument("--audit-zoom", type=int, metavar="CAP",
                    help=f"audit only: does a CAP build lose a feature the z{SOURCE_ZOOM} build has? (small extents)")
    a = ap.parse_args(argv)
    if a.audit_zoom is not None:
        report = audit_zoom_cap(SOURCE / "bc.pmtiles", a.audit_zoom)
        print_audit(report, a.audit_zoom)
        return 0 if lossless(report) else 1
    if not a.release_only:
        if a.max_zoom < SOURCE_ZOOM:
            report = audit_zoom_cap(SOURCE / "bc.pmtiles", a.max_zoom)
            print_audit(report, a.max_zoom)
            if not lossless(report):
                raise SystemExit(f"basemap: a z{a.max_zoom} cap loses features the z{SOURCE_ZOOM} build has "
                                 f"— refused")
        build(SOURCE / "bc.pmtiles", a.out, SOURCE / "bc_boundary.geojson", a.max_zoom)
    if not a.no_release:
        to_release(a.out, a.release_root, a.out.parent)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
