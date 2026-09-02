"""tippecanoe invocation — one pass over every layer file, into one .pmtiles.

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
      Kept, and it is nearly free: the same measurement shows it costs 8% more vertices
      (18.9 -> 20.5 at -S 4) and buys the property that two sections meeting at a
      confluence still share that point after simplification. Without it the river
      develops visible gaps and a downstream trace through the tile jumps.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from pipeline.tiles.layers import ALL


def check() -> None:
    if shutil.which("tippecanoe") is None:
        raise FileNotFoundError(
            "tippecanoe is not on $PATH. brew install tippecanoe, or see "
            "https://github.com/felt/tippecanoe")


def build(layer_dir: Path, out: Path, *, minzoom: int = 4, maxzoom: int = 14,
          name: str = "atlas", verbose: bool = False) -> Path:
    check()
    files = [layer_dir / f"{s.name}.geojsonl" for s in ALL]
    files = [f for f in files if f.exists() and f.stat().st_size > 0]
    if not files:
        raise RuntimeError(f"no layer files in {layer_dir}")

    cmd = [
        "tippecanoe",
        "-o", str(out), "--force",
        f"--minimum-zoom={minzoom}", f"--maximum-zoom={maxzoom}",
        f"--name={name}",
        "--attribution=FWA + BC Data Catalogue; OSM contributors",
        # per-feature tippecanoe.minzoom from the ladder is the ONLY thinning we accept
        "--no-feature-limit", "--no-tile-size-limit",
        # A stream network is a graph drawn as lines: if simplification is free to move
        # the point where two sections meet, the river develops visible gaps and — worse —
        # the two features stop sharing a node, so a trace that walks downstream through
        # the tile jumps. Shared nodes are pinned; only the points BETWEEN them simplify.
        "--no-simplification-of-shared-nodes",
        "--detect-shared-borders",
        # MEASURED, not guessed. On 6,000 low-zoom stream sections, at the z6/10/21 tile:
        #   -S 4  (was)   20.5 points per feature, 2,634 KB archive
        #   -S 10 (now)   10.5 points per feature, 2,026 KB   -- half the vertices, 23% smaller
        # At z6 a tile is 4,096 units across and renders ~512 px wide, so 20 points on a
        # river crossing it puts a vertex every 4 px: detail nobody can see, paid for in
        # every tile fetch.
        "--simplification=10",
        "--simplification-at-maximum-zoom=1",
        # a line whose every point coincides at low zoom is still a real river
        "--no-tiny-polygon-reduction",
    ]
    if not verbose:
        cmd.append("--quiet")
    cmd += [str(f) for f in files]

    print(f"  tippecanoe -> {out.name}  ({len(files)} layers)")
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"tippecanoe failed ({r.returncode})\n{r.stderr[-3000:]}")
    if r.stderr.strip() and verbose:
        print(r.stderr[-2000:])
    return out
