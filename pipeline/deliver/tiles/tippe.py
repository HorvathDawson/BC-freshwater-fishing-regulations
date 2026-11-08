"""tippecanoe invocation — into one .pmtiles, in two zoom bands.

Two flags carry the whole policy and both are the opposite of tippecanoe's defaults:

  --no-feature-limit / --no-tile-size-limit
      Tippecanoe's instinct when a tile gets crowded is to DROP features. For a
      regulations map that is a correctness bug, not a size optimisation: a stream that
      vanishes because its tile was busy is a stream a person cannot tap, and the app
      would show them nothing rather than a closure. We control density with the zoom
      ladder instead, which is deterministic and reviewable.

  --drop-densest-as-needed on `place` only
      Labels are the one layer where dropping is right — two hamlets on top of each
      other is worse than one.

  --no-simplification-of-shared-nodes
      A stream network is a graph drawn as lines. If simplification may move the point
      where two sections meet, the river develops visible gaps and — worse — the two
      features stop sharing a node, so a trace that walks downstream through the tile
      jumps. Shared nodes are pinned; only the points BETWEEN them simplify.

WHY THE BUILD IS ONE PASS PER ZOOM (it was two bands until T419)
=================================================================

`--no-simplification-of-shared-nodes` is a good flag applied to the wrong set. Tippecanoe
computes its shared-node set ONCE, over the whole input, before tiling. The per-feature
`tippecanoe.minzoom` this pipeline writes from the magnitude ladder is applied later, when
each tile is cut. So at a low zoom, where all but a handful of streams have been dropped,
their nodes are STILL in the shared set — and every one of them pins a vertex on the river
that is drawn, holding it in place for a confluence that is not on the map.

Measured on the province, per zoom, counting interior vertices of drawn features:

    zoom   drawn      pinned by a DROPPED feature      pinned by a DRAWN one
      4      130           13,314   (wasted)                  21   (the real job)
      6    1,073           84,878                            371
      8    8,816          304,846                          4,226
     10   41,750          602,410                         21,562
     12  157,891          930,313                         95,982

At z6 the flag does its actual job 371 times and pins 84,878 vertices for confluences
nobody can see. Those vertices cannot be simplified away, so they are paid for in bytes:

    z4-z7 stream tiles      682,945 B   all features + flag   (what this used to ship)
                            323,940 B   flag off  -- and the river gaps, so not an option
                            327,160 B   PRE-FILTERED input + flag

Feeding the low band ONLY the features it will draw makes tippecanoe's global node set the
right set by construction: it can no longer see a feature it is about to drop. The flag
keeps working for the features that ARE there — the 3,220 bytes between the last two rows
above is precisely that, still being done. Roughly HALF the bytes at z4-z7, and 39% at z8,
which is the part of the archive every reader downloads before they have panned anywhere.

The band ends at 8 because the input has to stay small enough for the extra pass to be
free: 8,816 stream features against 1,749,496, which tiles in seconds. The waste does
continue above it (28x at z10, 9.7x at z12) and a finer split would recover more; a
per-zoom split would recover nearly all of it, at eleven passes. Within the low band itself
the same effect survives in miniature — at z4, features with minzoom 5-8 are in the input
and dropped — and the measurement says that costs 7% at z4 and under 2% by z6, which is not
worth another pass.

PER-ZOOM PASSES (T419, 2026-11-06). The split above is now taken all the way: every zoom below the top is its own
pass fed only the features drawn at that zoom, and the top zoom one pass of everything. Two things ride on it: the
shared-node set is right at every zoom (not only at z4-z8), and the stream lines can be SMOOTHED FOR THE ZOOM THEY ARE
DRAWN AT (`smooth.py`), which one pass across several zooms could not do. Measured on a Fraser Valley clip (4,577
stream features), stream vertices per zoom, two bands at S=5 -> per-zoom at S=5 -> per-zoom + smoothing:
    z9  1,697 -> 987 -> 648      z10  3,903 -> 2,871 -> 1,770      z12  16,636 -> 15,508 -> 11,437
Each zoom is two passes — the water (`WATER_PASS`, at `WATER_SIMPLIFICATION`) and the rest — and each reads the
layer files again (the province build took 573 s with one pass per zoom); the passes still hold disjoint (layer,
zoom) pairs.

TWO TRAPS, both of which produced a wrong measurement before producing a right one:

  * `--simplification-at-maximum-zoom` applies at the band's OWN maximum, not the
    archive's. The low band's top zoom (z8) is an ordinary zoom in the finished archive and
    must be simplified like one; leaving it at tippecanoe's near-nothing default cost more
    bytes than the shared-node flag ever saved. Both bands now pass `SIMPLIFICATION`, which
    is also why the archive's own top zoom is no longer special — see that constant.
  * tippecanoe's mbtiles exposes `tiles` as a VIEW. Deleting a zoom range from it silently
    does nothing, and `tile-join` then MERGES the overlapping band rather than replacing
    it — every feature twice. The bands are cut by `--minimum-zoom`/`--maximum-zoom` at
    build time and never overlap.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path

import orjson

from pipeline.deliver.tiles import smooth
from pipeline.deliver.tiles.layers import ALL, BY_NAME, DRAWN_INTO, LAKE_LINK, tile_attrs

#: The last zoom at which the own-pass layers (`OWN_PASS`) ride the per-zoom passes; above it they are built in their
#: own pass. (Before T419 this was the top of the pre-filtered low band — see the module docstring.)
LOW_BAND_MAX = 8

#: Douglas-Peucker tolerance, in tile units. AT EVERY ZOOM, the maximum included.
#:
#: MEASURED, not guessed. On 6,000 low-zoom stream sections at the z6/10/21 tile:
#:   -S 4  (was)   20.5 points per feature, 2,634 KB archive
#:   -S 10 (now)   10.5 points per feature, 2,026 KB  -- half the vertices, 23% smaller
#: At z6 a tile is 4,096 units across and renders ~512 px wide, so 20 points on a river
#: crossing it puts a vertex every 4 px: detail nobody can see, paid for in every fetch.
#:
#: AND THE MAXIMUM ZOOM IS NOT SPECIAL HERE, which is the whole point of this constant
#: being used twice. Tippecanoe leaves the top zoom nearly unsimplified by default because
#: the top zoom is normally what gets OVERZOOMED — magnified 2x, 4x, 8x past the last tile
#: built, where a 10-unit tolerance becomes 10, 20, 40 px of visible corner-cutting.
#:
#: That does not happen here. `MAX_ZOOM` in app/packages/core/src/ladder.ts is 14 and the
#: camera is constructed with it, so the reader cannot go past the last zoom we build:
#: z14 geometry is only ever drawn at 1:1, where 10 units of 4,096 is about 1.25 px.
#: `test_tiles.py` pins the two numbers together, because the day the camera is allowed
#: past 14 this stops being free.
#:
#: MEASURED on a Fraser Valley slice (10,246 streams, 1,158 lakes, 354 wetlands), z13-14,
#: varying ONLY the maximum-zoom tolerance -- z13 came out byte-identical every time,
#: which is the check that the flag does what the docstring below says:
#:     -S(max) 1 (was)   z14  2.864 MiB
#:     -S(max) 4         z14  2.363 MiB   -17.5%
#:     -S(max) 7         z14  2.161 MiB   -24.5%
#:     -S(max) 10 (now)  z14  2.043 MiB   -28.7%
#:     -S(max) 14        z14  1.944 MiB   -32.1%
#: z14 is 68% of the province archive, so -28.7% there is about 139 MiB off the download
#: -- for a tolerance nobody can see at the only scale it is ever drawn at.
#: (T419, 2026-11-06) 5 -> 10 below the top zoom, for every layer but the water (`WATER_SIMPLIFICATION`): measured on
#: the full build, the polygons (parcels above all) at 5 cost +37% bytes at z11 in the Fraser Valley against 10.
SIMPLIFICATION = 10

#: (T419) THE WATER'S TOLERANCE below the top zoom, and the layers built at it, in a pass of their own per zoom.
#: 10 (b40de633d, staging r-4230bc0c1dd5) took points off the meandering rivers but left every meander's apex — DP
#: keeps the furthest point — so the Salmon River stayed a sawtooth while gentle streams lost real shape (owner: "the
#: bending oxbows dont become a straight line ... other streams got over simplified"). The meanders are now smoothed
#: away per zoom BEFORE tippecanoe (`smooth.py`, `SMOOTH_STREAMS`), and at 5 units (~0.6 px) a smoothed line stays on
#: its course (95th pct ~1.5 px from the true line; at 10, ~2.2 px). The routes through lakes and the water polygons
#: the streams end on ride the same pass, so a stream's end and the lake's edge stay one shared node.
WATER_SIMPLIFICATION = 5
WATER_PASS = ("stream", LAKE_LINK, "under_lake", "lake", "wetland", "tidal_water", "not_fishing_water")

#: (T489, owner 2026-11-07: "lake polygons lose almost too much shape when zoomed out") THE STANDING WATER'S OWN PASS
#: AT THE LOW ZOOMS: up to `STILL_WATER_GENTLE_MAX_Z` the lake and wetland polygons leave the water pass for one of their
#: own at `STILL_WATER_SIMPLIFICATION` (2 units of the 2,048 grid, ~0.5 px) instead of the streams' 5 (~1.25 px, which
#: is tuned for SMOOTHED lines, `smooth.py`, and cut a lake to a few corners: Pitt Lake was 10 points at z6, Hicks Lake
#: a 4-corner box at z8, Foley Lake one at z10). MEASURED (lake-only per-zoom passes over the 18,071 lakes drawn by z10,
#: live layer files 2026-11-07), points kept and the lake layer's bytes, S = 5 -> 2:
#:     z6   Harrison 28 -> 63    Okanagan 35 -> 81     Pitt 10 -> 22    bytes  10.7 ->  16.7 KB
#:     z8   Harrison 99 -> 255   Stave 78 -> 174       Hicks 5 -> 11    bytes   112 ->   165 KB
#:     z10  Harrison 469 -> 1084 Chilliwack 27 -> 67   Foley 5 -> 11    bytes 1,112 -> 1,505 KB
#: (S = 1 roughly doubled the points again for another +30-70% bytes; 2 is the knee.) Above z10 a lake is drawn big
#: enough that 5 units keeps its shape and the polygons stay in the water pass, so a stream's mouth on a lake's shore is
#: a pinned shared node again; below it the shore vertex at a mouth may move by the 0.5 px tolerance, never more.
#: To go back: STILL_WATER_GENTLE_MAX_Z = 0.
STILL_WATER_SIMPLIFICATION = 2
STILL_WATER_GENTLE_MAX_Z = 10
STILL_WATER_PASS = ("lake", "wetland")

#: (T419) Stream lines below the top zoom are smoothed for the zoom they are drawn at (`smooth.py`): a meander a few
#: pixels across collapses onto the river's course instead of drawing as a sawtooth. Ids, properties and every shared
#: node are unchanged. False builds the unsmoothed lines (the per-zoom passes stay).
SMOOTH_STREAMS = True

#: THE GRID BELOW THE TOP ZOOM (T202, owner 2026-10-27): 2^11 = 2,048 units across a tile, not tippecanoe's 4,096.
#: A z<=12 tile is drawn at most 2x (from z.0 to z.99), so a 2,048 grid snaps by at most half a pixel there, and
#: `SIMPLIFICATION` was halved with it (10 -> 5 units), which keeps the GROUND tolerance exactly where it was: the
#: tolerance is counted in grid units. Measured -10% at z11-12 on the Fraser Valley bbox, about -7 MB province-wide.
LOW_DETAIL = 11

#: THE ARCHIVE'S TOP ZOOM IS OVERZOOMED NOW (T85, D32: the archive stops at z13 and the camera goes to
#: z18), so the paragraph above no longer holds at the top: there the tolerance is magnified 2x-32x.
#: 5 units of a z13 tile (4,096 across, ~3.2 km at 49 N) is ~4 m on the ground — the same ground
#: error the old z14 top zoom had at 10 units — so overzoomed z13 draws no coarser than z14 did.
#: Every OTHER zoom keeps `SIMPLIFICATION`. NOT FREE, AND NOT -315 MB: the z13 tile now holds every
#: reach (magnitude 0-1 drew from z14 only), at the old z14's ground tolerance, so it grows toward the
#: old z14's content; ESTIMATED (unmeasured until the build) at 0.8-0.9x of it — net about -140 MB on
#: top of T64's wetland cut. The -S curve above says tolerance moves bytes little (10 -> 4: +16%).
SIMPLIFICATION_AT_MAX = 1.25

#: THE TOP ZOOM'S GRID (T202, owner 2026-10-27: "the ~5.5 m line"): 2^10 = 1,024 units across a z13 tile (3.1 m at
#: 49 N), with the tolerance cut to 1.25 units to match. The worst-case ground error is tolerance + half a grid unit:
#: 1.25 x 3.13 + 1.57 = ~5.5 m, against ~4.3 m at the old 4,096 / 5. At any given error a coarser grid beats a larger
#: tolerance (handoff/PLAN/tiles/T202-regulation-audit.md round 2 §6: detail 12 at the same 5.5 m saves ~6%, this
#: ~16% of z13, ~27 MB). Every shared node is still pinned, so sections still meet. TO GO BACK: TOP_DETAIL = 12 and
#: SIMPLIFICATION_AT_MAX = 5 (the T85 line, ~4.3 m).
TOP_DETAIL = 10


def check() -> None:
    for tool in ("tippecanoe", "tile-join"):
        if shutil.which(tool) is None:
            raise FileNotFoundError(
                f"{tool} is not on $PATH. brew install tippecanoe, or see "
                "https://github.com/felt/tippecanoe")


def _common(minzoom: int, maxzoom: int, simplify_at_max: float, verbose: bool, *,
            full_detail: int = LOW_DETAIL, low_detail: int = LOW_DETAIL,
            simplification: float = SIMPLIFICATION) -> list[str]:
    """The flags every pass shares. `full_detail` is the grid at the pass's OWN top zoom (tippecanoe's
    `--full-detail`); every zoom below it is drawn on `LOW_DETAIL`."""
    cmd = [
        f"--minimum-zoom={minzoom}", f"--maximum-zoom={maxzoom}",
        f"--full-detail={full_detail}", f"--low-detail={low_detail}",
        # per-feature tippecanoe.minzoom from the ladder is the ONLY thinning we accept
        "--no-feature-limit", "--no-tile-size-limit",
        "--no-simplification-of-shared-nodes",
        "--detect-shared-borders",
        f"--simplification={simplification}",
        f"--simplification-at-maximum-zoom={simplify_at_max}",
        # a line whose every point coincides at low zoom is still a real river
        "--no-tiny-polygon-reduction",
    ]
    if not verbose:
        cmd.append("--quiet")
    return cmd


#: AN UNNAMED LAKE KEEPS ITS AREA ONLY WHERE THE OUTLINE CAN SHOW IT (T202). `area_m2` has two readers: the label's sort
#: key (a lake with no name has no label) and the outline width, `base + k*sqrt(area)` with a missing area read as
#: 10,000 m2 (app/packages/map/src/paint.ts). Below this area the drawn width differs from that fallback by at most
#: 0.0003 * (sqrt(100,000) - 100) = 0.065 px (z16, the steepest stop), which no one can see; above it the width is real
#: information and the value stays. Unique per feature, so it never compressed: most unnamed lakes are under it.
UNNAMED_AREA_KEPT_FROM = 100_000


def _tile_props(spec, props: dict) -> dict:
    """The properties a tile feature carries: the layer's tile attributes (`layers.tile_attrs`, which leaves out an
    MVT-id layer's `section_id`: the id IS the feature's own) and, on the water polygons, nothing that says only
    what absence already says (T202):

    * `water` equal to the layer's own name (`"lake"` on `lake`, `"wetland"` on `wetland`): the style asks only
      `water == "stream"` / `!= "stream"`, and an absent key is `!= "stream"`;
    * `area_m2` on an unnamed lake under `UNNAMED_AREA_KEPT_FROM`."""
    keep = set(tile_attrs(spec))
    out = {k: v for k, v in props.items() if k in keep}
    if out.get("water") == spec.name:
        del out["water"]
    if "area_m2" in out and not out.get("name"):
        a = out["area_m2"]
        if isinstance(a, (int, float)) and a < UNNAMED_AREA_KEPT_FROM:
            del out["area_m2"]
    return out


def _copy_layer(src: Path, dst: Path, *, max_minzoom: int | None, id_attr: str | None,
                spec=None) -> int:
    """Copy a layer file's features into `dst`. Returns how many were written.

    `max_minzoom`: keep only the features DRAWN at or below it (None keeps all). A feature
    with no `tippecanoe.minzoom` is drawn at every zoom, so it is kept. Nothing in this
    pipeline emits one today — the ladder covers every layer — but reading the absence as
    "z14 only" would silently drop a whole layer out of the low band.

    `id_attr`: write that property's value as the GeoJSON feature `id`, which tippecanoe
    carries through as the MVT feature id. This is the section layers' `section_id` (see
    `LayerSpec.mvt_id`). It is done here, per feature, rather than with `--use-attribute-for-id`,
    because the value is checked: a section feature with no id is a bug upstream and stops the
    build — it would be a reach nobody can colour or tap.

    `spec`: the properties are cut to what the TILE carries (`_tile_props`). The layer FILE keeps everything the
    export wrote — `section_id` above all, which the bundle's section access reads (`deliver/bundle/access.py`) —
    and the tile carries the id once, as the MVT id (T202: the property copy was 18 MB of the archive).
    """
    kept = 0
    with src.open("rb") as fin, dst.open("wb") as fout:
        for line in fin:
            if not line.strip():
                continue
            f = orjson.loads(line)
            if max_minzoom is not None and f.get("tippecanoe", {}).get("minzoom", 0) > max_minzoom:
                continue
            # (T493) nor one that STOPS before it: a pass is fed only what its zoom draws. A feature left in the input
            # is in the pass's shared-node set even where tippecanoe drops it, and a lake's link (the route, drawn up
            # to the zoom before its lake) would pin every vertex of the same route in `under_lake` from the lake's zoom.
            if max_minzoom is not None and f.get("tippecanoe", {}).get("maxzoom", max_minzoom) < max_minzoom:
                continue
            if id_attr is not None:
                v = f.get("properties", {}).get(id_attr)
                if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                    raise SystemExit(f"{src.name}: a feature's {id_attr} is {v!r}; an MVT id "
                                     f"must be a non-negative integer")
                f["id"] = v
            if spec is not None:
                f["properties"] = _tile_props(spec, f.get("properties") or {})
            fout.write(orjson.dumps(f) + b"\n")
            kept += 1
    return kept


def _inputs(files: list[Path], tmp: Path, max_minzoom: int | None) -> tuple[list[Path], int]:
    """The files one tippecanoe pass reads: EVERY layer copied into `tmp`, cut to its tile properties
    (`_tile_props`), filtered when the pass needs it (`max_minzoom`) and id-stamped (`LayerSpec.mvt_id`).
    Returns the non-empty inputs and the feature count."""
    out, total = [], 0
    for f in files:
        spec = BY_NAME[DRAWN_INTO.get(f.stem, f.stem)]      # (T493) a file drawn into another layer takes its spec
        id_attr = spec.attrs[0] if spec.mvt_id else None
        dst = tmp / f.name
        n = _copy_layer(f, dst, max_minzoom=max_minzoom, id_attr=id_attr, spec=spec)
        total += n
        if n:
            out.append(dst)
    return out, total


def _run(cmd: list[str]) -> None:
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"{cmd[0]} failed ({r.returncode})\n{r.stderr[-3000:]}")


#: THE LAYERS BUILT IN A PASS OF THEIR OWN, stopping at their own `maxzoom` (T202, owner 2026-10-27). The management
#: units stop at z12 and the app overzooms that tile to z18 (`sources.mus` in app/packages/map/style/layers.source.json),
#: because in the atlas every z13 tile also holds streams, so mu's z13 squares never deduped: -7 MB. Their own pass makes
#: z12 that pass's TOP zoom, so it takes `OWN_PASS_SIMPLIFICATION_AT_MAX` on the full 4,096 grid: the unit edge is at
#: most ~8 m off at z13-18 (in the high band z12 is an ordinary zoom, ~16 m). TO REVERSE: in layers.py set `mu`'s
#: maxzoom back to `ladder.MAX_ZOOM` and drop its `own_pass` (it then rides the high band again: this tuple is read
#: from the specs), and point the style's `mu` layer back at `atlas` (dropping `sources.mus`).
OWN_PASS = tuple(s.name for s in ALL if s.own_pass)
#: (T299) THE LAYERS BUILT ALONE: one pass each over the layer's own zoom range, in neither band (`LayerSpec.alone`).
#: `stream_name` repeats the named streams' geometry; in their pass its vertices would count as shared nodes and pin
#: theirs.
ALONE = tuple(s.name for s in ALL if s.alone)
#: AND ON A FINE GRID, SMOOTHED. A name is placed only where the line bends less than `text-max-angle` along it, and on
#: the bands' coarse grids (2,048 below the top, 1,024 at it) every diagonal is a staircase of right angles: measured
#: on the Lower Mainland at z11/z13, one stream of ten named with the label's 25 degrees, all ten at 180. So the name
#: lines are cut on the full 4,096 grid (`ALONE_DETAIL`) and simplified enough to take the stairs out
#: (`ALONE_SIMPLIFICATION`, 8 units = 1 px of a 512 px tile; 4 at the overzoomed top): a label runs within a pixel of
#: the water at its own zoom.
ALONE_DETAIL = 12
ALONE_SIMPLIFICATION = 8
ALONE_SIMPLIFICATION_AT_MAX = 4
OWN_PASS_SIMPLIFICATION_AT_MAX = 5
OWN_PASS_DETAIL = 12


def build(layer_dir: Path, out: Path, *, minzoom: int = 4, maxzoom: int = 13,
          name: str = "atlas", verbose: bool = False) -> Path:
    check()
    files = [layer_dir / f"{n}.geojsonl" for n in [s.name for s in ALL] + list(DRAWN_INTO)]
    files = [f for f in files if f.exists() and f.stat().st_size > 0]
    if not files:
        raise RuntimeError(f"no layer files in {layer_dir}")

    attribution = "FWA + BC Data Catalogue; OSM contributors"
    out.parent.mkdir(parents=True, exist_ok=True)

    # A band needs at least one zoom on each side of the split to be worth the join.
    # Every pass reads COPIES of the layers, cut to their tile properties, with `section_id`
    # stamped as the MVT feature id (see `_copy_layer`). tile-join keeps feature ids as they
    # are, so the ids written by every pass survive the join.
    if not (minzoom <= LOW_BAND_MAX < maxzoom):
        with tempfile.TemporaryDirectory(prefix="tiles-ids-") as tmp:
            inputs, _ = _inputs(files, Path(tmp), None)
            print(f"  tippecanoe -> {out.name}  ({len(inputs)} layers, single pass)")
            _run(["tippecanoe", "-o", str(out), "--force", f"--name={name}",
                  f"--attribution={attribution}",
                  *_common(minzoom, maxzoom, SIMPLIFICATION_AT_MAX, verbose, full_detail=TOP_DETAIL),
                  *[str(f) for f in inputs]])
        return out

    with tempfile.TemporaryDirectory(prefix="tiles-lowband-") as tmp:
        # (T419) ONE PASS PER ZOOM below the top, each fed only what that zoom draws, with the stream lines smoothed
        # for that zoom (`smooth.py`); then the top zoom from everything, unsmoothed. See "PER-ZOOM PASSES" above.
        banded = [f for f in files if f.stem not in ALONE]
        passes = []
        for z in range(minzoom, maxzoom):
            zdir = Path(tmp) / f"z{z}"
            zdir.mkdir()
            layers = [f for f in banded if z <= LOW_BAND_MAX or f.stem not in OWN_PASS]
            zfiles, kept = _inputs(layers, zdir, z)
            stream = zdir / "stream.geojsonl"
            if SMOOTH_STREAMS and stream in zfiles:
                smooth.smooth_file(stream, z)
            # (T419) two passes per zoom: the WATER (smoothed streams, their routes, the water polygons they end on, so
            # those nodes are still shared) at `WATER_SIMPLIFICATION`, everything else at `SIMPLIFICATION`.
            # (T489) up to `STILL_WATER_GENTLE_MAX_Z` the lake and wetland polygons are a third pass, at a gentler tolerance.
            still = STILL_WATER_PASS if z <= STILL_WATER_GENTLE_MAX_Z else ()
            groups = (("water", [f for f in zfiles if f.stem in WATER_PASS and f.stem not in still], WATER_SIMPLIFICATION),
                      ("still", [f for f in zfiles if f.stem in still], STILL_WATER_SIMPLIFICATION),
                      ("other", [f for f in zfiles if f.stem not in WATER_PASS], SIMPLIFICATION))
            for tag, gfiles, tol in groups:
                if not gfiles:
                    continue
                mb = Path(tmp) / f"z{z}-{tag}.mbtiles"
                print(f"  tippecanoe -> z{z} {tag}  ({len(gfiles)} layers)")
                # `--simplification-at-maximum-zoom` = the same tolerance, NOT 1: this pass's maximum is an
                # ordinary zoom of the finished archive and is simplified like one, on the same grid. See the docstring.
                _run(["tippecanoe", "-o", str(mb), "--force", f"--name={name}",
                      f"--attribution={attribution}",
                      *_common(z, z, tol, verbose, full_detail=LOW_DETAIL, simplification=tol),
                      *[str(f) for f in gfiles]])
                passes.append(mb)
            print(f"     z{z}: {kept:,} features drawn")

        top_dir = Path(tmp) / "top"
        top_dir.mkdir()
        # (T493) a lake's link stops the zoom before its lake, so never at the top (no lake starts after it): left in,
        # its vertices would pin the same route's in `under_lake` (see `_copy_layer`)
        top_all, _ = _inputs([f for f in banded if f.stem not in DRAWN_INTO], top_dir, None)
        own = [f for f in top_all if f.stem in OWN_PASS]
        top_files = [f for f in top_all if f.stem not in OWN_PASS]
        alone_dir = Path(tmp) / "alone"
        alone_dir.mkdir()
        alone, _ = _inputs([f for f in files if f.stem in ALONE], alone_dir, None)

        high = Path(tmp) / "high.mbtiles"
        print(f"  tippecanoe -> z{maxzoom}  ({len(top_files)} layers, all features)")
        # The archive's maximum, and the camera overzooms it (T85), so it takes `SIMPLIFICATION_AT_MAX` on
        # `TOP_DETAIL` — never 1 (see the constants). Its lines are NOT smoothed: overzoomed to z18, they are the map.
        _run(["tippecanoe", "-o", str(high), "--force", f"--name={name}",
              f"--attribution={attribution}",
              *_common(maxzoom, maxzoom, SIMPLIFICATION_AT_MAX, verbose, full_detail=TOP_DETAIL),
              *[str(f) for f in top_files]])
        passes.append(high)

        for f in own:
            top = BY_NAME[f.stem].maxzoom
            mb = Path(tmp) / f"{f.stem}.mbtiles"
            print(f"  tippecanoe -> z{LOW_BAND_MAX + 1}-{top}  ({f.stem}, its own pass)")
            _run(["tippecanoe", "-o", str(mb), "--force", f"--name={name}",
                  f"--attribution={attribution}",
                  *_common(LOW_BAND_MAX + 1, top, OWN_PASS_SIMPLIFICATION_AT_MAX, verbose,
                           full_detail=OWN_PASS_DETAIL),
                  str(f)])
            passes.append(mb)
        for f in alone:
            spec = BY_NAME[f.stem]
            lo, top = max(minzoom, spec.minzoom), min(maxzoom, spec.maxzoom)
            mb = Path(tmp) / f"{f.stem}.mbtiles"
            print(f"  tippecanoe -> z{lo}-{top}  ({f.stem}, alone)")
            _run(["tippecanoe", "-o", str(mb), "--force", f"--name={name}",
                  f"--attribution={attribution}",
                  *_common(lo, top, ALONE_SIMPLIFICATION_AT_MAX, verbose, full_detail=ALONE_DETAIL,
                           low_detail=ALONE_DETAIL, simplification=ALONE_SIMPLIFICATION),
                  str(f)])
            passes.append(mb)

        # tile-join names the output after its inputs unless told otherwise, so the name
        # and attribution are restated here rather than inherited. The passes hold disjoint
        # (layer, zoom) pairs, so the join merges layers into each tile and never doubles one.
        print(f"  tile-join  -> {out.name}")
        _run(["tile-join", "-f", "-o", str(out), f"--name={name}",
              f"--attribution={attribution}", f"--description={name}",
              *[str(p) for p in passes]])
    return out
