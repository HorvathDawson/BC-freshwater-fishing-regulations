#!/usr/bin/env python3
"""
PMTiles tiered export test script.

Uses existing GeoJSONSeq files from output/regulation_mapping/temp/ to test
a tiered tippecanoe strategy that dramatically reduces PMTiles size by
splitting streams into two zoom tiers:

  Tier 1 — Non-stream layers (polygons, admin, boundaries):
      Single tippecanoe run with --no-clipping (polygon fills need it to
      avoid seam artifacts) and --no-simplification-of-shared-nodes.

  Tier 2 — Streams low-res (z4–8):
      Only the ~3,700 major rivers (minzoom ≤ 8).  Runs WITH clipping
      (default) so long rivers aren't duplicated across hundreds of
      low-zoom tiles.  Keeps --no-simplification-of-shared-nodes to
      prevent gaps at confluences.

  Tier 3 — Streams high-res (z9–12):
      ALL streams.  Also runs WITH clipping — line clipping is visually
      seamless (no artifacts for solid colored lines), and the click
      handler + highlight code work fine with clipped fragments:
        - queryRenderedFeatures returns properties (frontend_group_id)
          regardless of clipping, so JSON lookup works
        - querySourceFeatures collects fragments from loaded tiles that
          tile together perfectly for highlighting

  tile-join merges all three into one PMTiles with a single "streams"
  source-layer — zero frontend changes needed.

Usage:
    python scripts/test_tiered_pmtiles.py
"""

import json
import subprocess
import sys
import time
from pathlib import Path

TEMP_DIR = Path("output/regulation_mapping/temp")
OUTPUT_DIR = Path("output/regulation_mapping")
WORK_DIR = OUTPUT_DIR / "temp_tiered"

# Zoom cutoff: streams at or below this zoom go into the low-res tier
STREAM_LOW_MAX_ZOOM = 8


def file_size_mb(path: Path) -> str:
    """Human-readable file size."""
    if path.exists():
        size = path.stat().st_size
        if size > 1073741824:
            return f"{size / 1073741824:.2f} GB"
        return f"{size / 1048576:.1f} MB"
    return "N/A"


def run_cmd(cmd: list, label: str) -> bool:
    """Run a shell command with timing and live stderr output."""
    print(f"\n{'=' * 60}")
    print(f"  {label}")
    print(f"{'=' * 60}")
    print(f"  $ {' '.join(str(c) for c in cmd)}\n")

    start = time.time()
    # Stream stderr live so tippecanoe progress is visible
    result = subprocess.run(cmd, text=True)
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"\n  FAILED (rc={result.returncode})")
        return False

    print(f"\n  OK ({elapsed:.1f}s)")
    return True


def split_streams(streams_path: Path) -> tuple:
    """Split streams.geojsonseq into low-res (z≤8) and full (all) sets.

    Low-res contains only features with tippecanoe.minzoom ≤ STREAM_LOW_MAX_ZOOM.
    High-res contains ALL features (including the big rivers, so they
    continue appearing at z9+).

    Returns (low_path, hi_path, low_count, total_count).
    """
    low_path = WORK_DIR / "streams_low.geojsonseq"
    hi_path = WORK_DIR / "streams_all.geojsonseq"

    low_count = 0
    total = 0

    with (
        open(streams_path) as src,
        open(low_path, "w") as low_f,
        open(hi_path, "w") as hi_f,
    ):
        for line in src:
            line = line.strip()
            if not line:
                continue
            total += 1

            feat = json.loads(line)
            minzoom = feat.get("tippecanoe", {}).get("minzoom", 12)

            # Low-res: only major rivers
            if minzoom <= STREAM_LOW_MAX_ZOOM:
                low_f.write(line + "\n")
                low_count += 1

            # High-res: ALL streams (including big rivers for z9+ continuity)
            hi_f.write(line + "\n")

    return low_path, hi_path, low_count, total


def main():
    # --- Preflight checks ---
    for tool in ("tippecanoe", "tile-join"):
        result = subprocess.run(["which", tool], capture_output=True)
        if result.returncode != 0:
            print(f"ERROR: {tool} not found on PATH")
            sys.exit(1)

    if not TEMP_DIR.exists():
        print(f"ERROR: {TEMP_DIR} does not exist. Run the pipeline first.")
        sys.exit(1)

    all_files = sorted(TEMP_DIR.glob("*.geojsonseq"))
    if not all_files:
        print(f"ERROR: No .geojsonseq files in {TEMP_DIR}")
        sys.exit(1)

    streams_file = TEMP_DIR / "streams.geojsonseq"
    if not streams_file.exists():
        print("ERROR: streams.geojsonseq not found")
        sys.exit(1)

    non_stream_files = [f for f in all_files if f.name != "streams.geojsonseq"]

    print("Input GeoJSONSeq files:")
    for f in all_files:
        tag = " ← STREAMS" if f.name == "streams.geojsonseq" else ""
        print(f"  {f.name:45s} {file_size_mb(f):>10s}{tag}")

    WORK_DIR.mkdir(parents=True, exist_ok=True)

    # ─── Step 1: Split streams ───────────────────────────────────────────
    print("\n--- Step 1: Splitting streams by minzoom ---")
    streams_low_path, streams_hi_path, low_count, total_count = split_streams(
        streams_file
    )
    print(
        f"  Low-res (z≤{STREAM_LOW_MAX_ZOOM}): {low_count:>8,} features  {file_size_mb(streams_low_path)}"
    )
    print(
        f"  High-res (all):    {total_count:>8,} features  {file_size_mb(streams_hi_path)}"
    )

    # ─── Step 2: Non-stream layers ───────────────────────────────────────
    # Polygons, admin overlays, boundaries, bc_mask, ungazetted points.
    # --no-clipping: polygon fills need full geometry to avoid seam artifacts.
    # --no-simplification-of-shared-nodes: preserve clean polygon borders.
    # --detect-shared-borders: consistent simplification along shared edges.
    poly_pmtiles = WORK_DIR / "non_streams.pmtiles"
    poly_cmd = [
        "tippecanoe",
        "-o",
        str(poly_pmtiles),
        "--force",
        "--hilbert",
        "--minimum-zoom=4",
        "--maximum-zoom=12",
        "--simplification=10",
        "--simplification-at-maximum-zoom=1",
        "--read-parallel",
        "--detect-shared-borders",
        "--no-clipping",
        "--no-feature-limit",
        "--no-tile-size-limit",
        "--no-simplification-of-shared-nodes",
    ]
    for f in non_stream_files:
        poly_cmd.extend(["-L", f"{f.stem}:{f}"])

    if not run_cmd(poly_cmd, "Step 2: Non-stream layers (--no-clipping for polygons)"):
        sys.exit(1)
    print(f"  → {file_size_mb(poly_pmtiles)}")

    # ─── Step 3: Streams low-res (z4–8, clipped) ────────────────────────
    # Only the ~3,700 major rivers.  WITH clipping (no --no-clipping) so
    # long rivers aren't stored in every tile they cross at wide zoom.
    # --no-simplification-of-shared-nodes: still keep confluence vertices
    # intact to avoid gaps where rivers meet.
    streams_low_pmtiles = WORK_DIR / "streams_low.pmtiles"
    streams_low_cmd = [
        "tippecanoe",
        "-o",
        str(streams_low_pmtiles),
        "--force",
        "--hilbert",
        "--minimum-zoom=4",
        "--maximum-zoom=8",  # only low zoom tiles
        "--simplification=10",
        "--simplification-at-maximum-zoom=1",
        "--read-parallel",
        "--no-feature-limit",
        "--no-tile-size-limit",
        "--no-simplification-of-shared-nodes",
        # NOTE: no --no-clipping! Lines clip at tile boundaries.
        # At z4-8 with only major rivers, clipping is visually seamless
        # and prevents massive geometry duplication across tiles.
        "-L",
        f"streams:{streams_low_path}",
    ]

    if not run_cmd(
        streams_low_cmd,
        f"Step 3: Streams low-res (z4-{STREAM_LOW_MAX_ZOOM}, clipped, {low_count:,} features)",
    ):
        sys.exit(1)
    print(f"  → {file_size_mb(streams_low_pmtiles)}")

    # ─── Step 4: Streams high-res (z9–12, clipped) ────────────────────
    # ALL streams, also with clipping.  Line clipping is visually seamless
    # (no artifacts for solid colored lines).  The click handler uses
    # frontend_group_id from tile properties → JSON lookup, so clipped
    # geometry doesn't affect functionality.  querySourceFeatures collects
    # clipped fragments from loaded tiles that tile together for highlighting.
    streams_hi_pmtiles = WORK_DIR / "streams_hi.pmtiles"
    streams_hi_cmd = [
        "tippecanoe",
        "-o",
        str(streams_hi_pmtiles),
        "--force",
        "--hilbert",
        "--minimum-zoom=9",  # starts where low-res ends
        "--maximum-zoom=12",
        "--simplification=10",
        "--simplification-at-maximum-zoom=1",
        "--read-parallel",
        "--no-feature-limit",
        "--no-tile-size-limit",
        "--no-simplification-of-shared-nodes",
        # NO --no-clipping: line clipping is visually seamless and
        # prevents geometry duplication across tiles.
        "-L",
        f"streams:{streams_hi_path}",
    ]

    if not run_cmd(
        streams_hi_cmd,
        f"Step 4: Streams high-res (z9-12, clipped, {total_count:,} features)",
    ):
        sys.exit(1)
    print(f"  → {file_size_mb(streams_hi_pmtiles)}")

    # ─── Step 5: tile-join ───────────────────────────────────────────────
    # Merge all three into one PMTiles.  streams_low and streams_hi both
    # use layer name "streams" so they merge into one source-layer at
    # different zoom ranges.  No frontend changes needed.
    merged_path = WORK_DIR / "regulations_merged_tiered.pmtiles"
    join_cmd = [
        "tile-join",
        "--force",
        "--no-tile-size-limit",
        "-o",
        str(merged_path),
        str(poly_pmtiles),
        str(streams_low_pmtiles),
        str(streams_hi_pmtiles),
    ]

    if not run_cmd(join_cmd, "Step 5: tile-join → merged PMTiles"):
        sys.exit(1)

    # ─── Results ─────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print("  RESULTS")
    print(f"{'=' * 60}")

    print(f"\n  Component sizes:")
    print(f"    Non-stream layers:     {file_size_mb(poly_pmtiles):>10s}")
    print(
        f"    Streams low (z4-{STREAM_LOW_MAX_ZOOM}):    {file_size_mb(streams_low_pmtiles):>10s}"
    )
    print(f"    Streams hi  (z9-12):   {file_size_mb(streams_hi_pmtiles):>10s}")
    print(f"")
    print(f"  Merged (tiered):         {file_size_mb(merged_path):>10s}")

    original = OUTPUT_DIR / "regulations_merged.pmtiles"
    if original.exists():
        print(f"  Original (single-run):   {file_size_mb(original):>10s}")
        orig_size = original.stat().st_size
        new_size = merged_path.stat().st_size
        if orig_size > 0:
            pct = (1 - new_size / orig_size) * 100
            print(f"\n  Size change: {pct:+.1f}%")

    print(f"\n  Output: {merged_path}")
    print(f"\n  To test on the frontend, copy it over the original:")
    print(f"    cp {merged_path} {original}")


if __name__ == "__main__":
    main()
