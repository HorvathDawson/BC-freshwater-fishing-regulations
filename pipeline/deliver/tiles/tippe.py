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

WHY THE BUILD IS TWO PASSES
===========================

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

TWO TRAPS, both of which produced a wrong measurement before producing a right one:

  * `--simplification-at-maximum-zoom` applies at the band's OWN maximum. Give the low band
    the production value of 1 and its top zoom is left almost unsimplified, which is worth
    more bytes than the flag ever cost. It gets `--simplification` instead.
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

from pipeline.deliver.tiles.layers import ALL

#: The last zoom built from a pre-filtered input. See the module docstring for why 8.
LOW_BAND_MAX = 8

#: Douglas-Peucker tolerance, in tile units, at every zoom below the maximum.
#:
#: MEASURED, not guessed. On 6,000 low-zoom stream sections at the z6/10/21 tile:
#:   -S 4  (was)   20.5 points per feature, 2,634 KB archive
#:   -S 10 (now)   10.5 points per feature, 2,026 KB  -- half the vertices, 23% smaller
#: At z6 a tile is 4,096 units across and renders ~512 px wide, so 20 points on a river
#: crossing it puts a vertex every 4 px: detail nobody can see, paid for in every fetch.
SIMPLIFICATION = 10


def check() -> None:
    for tool in ("tippecanoe", "tile-join"):
        if shutil.which(tool) is None:
            raise FileNotFoundError(
                f"{tool} is not on $PATH. brew install tippecanoe, or see "
                "https://github.com/felt/tippecanoe")


def _common(minzoom: int, maxzoom: int, simplify_at_max: int, verbose: bool) -> list[str]:
    cmd = [
        f"--minimum-zoom={minzoom}", f"--maximum-zoom={maxzoom}",
        # per-feature tippecanoe.minzoom from the ladder is the ONLY thinning we accept
        "--no-feature-limit", "--no-tile-size-limit",
        "--no-simplification-of-shared-nodes",
        "--detect-shared-borders",
        f"--simplification={SIMPLIFICATION}",
        f"--simplification-at-maximum-zoom={simplify_at_max}",
        # a line whose every point coincides at low zoom is still a real river
        "--no-tiny-polygon-reduction",
    ]
    if not verbose:
        cmd.append("--quiet")
    return cmd


def _filter_to(src: Path, dst: Path, max_minzoom: int) -> int:
    """Copy the features that are DRAWN at or below `max_minzoom`. Returns how many.

    A feature with no `tippecanoe.minzoom` is drawn at every zoom, so it is kept. Nothing
    in this pipeline emits one today — the ladder covers every layer — but reading the
    absence as "z14 only" would silently drop a whole layer out of the low band.
    """
    kept = 0
    with src.open("rb") as fin, dst.open("wb") as fout:
        for line in fin:
            if not line.strip():
                continue
            if orjson.loads(line).get("tippecanoe", {}).get("minzoom", 0) <= max_minzoom:
                fout.write(line)
                kept += 1
    return kept


def _run(cmd: list[str]) -> None:
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"{cmd[0]} failed ({r.returncode})\n{r.stderr[-3000:]}")


def build(layer_dir: Path, out: Path, *, minzoom: int = 4, maxzoom: int = 14,
          name: str = "atlas", verbose: bool = False) -> Path:
    check()
    files = [layer_dir / f"{s.name}.geojsonl" for s in ALL]
    files = [f for f in files if f.exists() and f.stat().st_size > 0]
    if not files:
        raise RuntimeError(f"no layer files in {layer_dir}")

    attribution = "FWA + BC Data Catalogue; OSM contributors"
    out.parent.mkdir(parents=True, exist_ok=True)

    # A band needs at least one zoom on each side of the split to be worth the join.
    if not (minzoom <= LOW_BAND_MAX < maxzoom):
        print(f"  tippecanoe -> {out.name}  ({len(files)} layers, single pass)")
        _run(["tippecanoe", "-o", str(out), "--force", f"--name={name}",
              f"--attribution={attribution}",
              *_common(minzoom, maxzoom, 1, verbose), *[str(f) for f in files]])
        return out

    with tempfile.TemporaryDirectory(prefix="tiles-lowband-") as tmp:
        low_dir = Path(tmp)
        low_files, kept_total = [], 0
        for f in files:
            dst = low_dir / f.name
            kept = _filter_to(f, dst, LOW_BAND_MAX)
            kept_total += kept
            if kept:
                low_files.append(dst)

        low = low_dir / "low.mbtiles"
        high = low_dir / "high.mbtiles"

        print(f"  tippecanoe -> z{minzoom}-{LOW_BAND_MAX}  "
              f"({len(low_files)} layers, {kept_total:,} features drawn there)")
        # `--simplification-at-maximum-zoom=SIMPLIFICATION`, NOT 1: this band's maximum is
        # z8, which in the finished archive is an ordinary zoom and must be simplified like
        # one. See the docstring — passing 1 here costs more than the flag ever saved.
        _run(["tippecanoe", "-o", str(low), "--force", f"--name={name}",
              f"--attribution={attribution}",
              *_common(minzoom, LOW_BAND_MAX, SIMPLIFICATION, verbose),
              *[str(f) for f in low_files]])

        print(f"  tippecanoe -> z{LOW_BAND_MAX + 1}-{maxzoom}  ({len(files)} layers, all features)")
        _run(["tippecanoe", "-o", str(high), "--force", f"--name={name}",
              f"--attribution={attribution}",
              *_common(LOW_BAND_MAX + 1, maxzoom, 1, verbose),
              *[str(f) for f in files]])

        # tile-join names the output after its inputs unless told otherwise, so the name
        # and attribution are restated here rather than inherited.
        print(f"  tile-join  -> {out.name}")
        _run(["tile-join", "-f", "-o", str(out), f"--name={name}",
              f"--attribution={attribution}", f"--description={name}",
              str(low), str(high)])
    return out
