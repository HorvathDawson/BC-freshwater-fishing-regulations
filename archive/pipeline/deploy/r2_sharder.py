"""R2 Shard Generator — splits the regulation index into edge-fetchable chunks.

Outputs (all written to the deploy directory):

  shards/v{N}/fids/{sha3}.json     — fid → reach_id (2.4M entries, ~4096 files)
  shards/v{N}/reaches/{sha3}.json  — reach_id → {reach metadata, fids[]} (~4096 files)
  shards/v{N}/polys/{sha3}.json    — wbk → reach_id (721K entries, ~4096 files)
  shards/v{N}/MANIFEST.json        — atomicity marker with shard counts + checksums
  tier0.json                       — regulations + reg_sets + enriched search_index

Sharding strategy:
  IDs are SHA-256 hashed, and the first 3 hex chars of the digest determine
  the shard bucket (4096 buckets). This gives uniform distribution regardless
  of the native ID format (numeric fids vs hex reach_ids).

Primary API:
  shard_from_dict(data, output_dir, version)  — called directly by builder.py

CLI (standalone):
  python -m pipeline.deploy.r2_sharder \\
      --input  output/pipeline/regulation_index.json \\
      --output output/pipeline/deploy/ \\
      --version 8
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


# ── Shard prefix computation ─────────────────────────────────────────


def shard_prefix(id_str: str) -> str:
    """Return the 3-char hex SHA-256 prefix for an ID (4096 buckets)."""
    digest = hashlib.sha256(id_str.encode()).hexdigest()
    return digest[:3]


def group_by_prefix(mapping: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Group a flat dict into {prefix: {key: value}} buckets."""
    groups: dict[str, dict[str, Any]] = defaultdict(dict)
    for key, value in mapping.items():
        prefix = shard_prefix(key)
        groups[prefix][key] = value
    return dict(groups)


# ── Shard writers ─────────────────────────────────────────────────────


def _write_shards(
    groups: dict[str, dict[str, Any]],
    output_dir: Path,
    category: str,
) -> int:
    """Write shard files and return the count written."""
    shard_dir = output_dir / category
    shard_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for prefix, entries in groups.items():
        path = shard_dir / f"{prefix}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(entries, f, ensure_ascii=False, separators=(",", ":"))
        count += 1
    return count


# ── Public API ────────────────────────────────────────────────────────


def build_fid_shards(
    reach_segments: dict[str, list[str]],
) -> dict[str, dict[str, str]]:
    """Build fid → reach_id mapping grouped by shard prefix.

    Returns {prefix: {fid: reach_id}}.
    """
    fid_to_reach: dict[str, str] = {}
    for reach_id, fids in reach_segments.items():
        for fid in fids:
            fid_to_reach[fid] = reach_id
    return group_by_prefix(fid_to_reach)


def build_reach_shards(
    reaches: dict[str, dict[str, Any]],
    reach_segments: dict[str, list[str]],
) -> dict[str, dict[str, Any]]:
    """Build reach_id → {reach data + fids[]} grouped by shard prefix.

    Embeds the fid list in each reach so the Worker returns everything
    the click handler needs in a single reach shard fetch.

    Returns {prefix: {reach_id: {...reach, fids: [...]}}}.
    """
    combined: dict[str, Any] = {}
    for reach_id, reach in reaches.items():
        fids = reach_segments.get(reach_id, [])
        combined[reach_id] = {**reach, "fids": fids}
    return group_by_prefix(combined)


def build_poly_shards(
    poly_reaches: dict[str, str],
) -> dict[str, dict[str, str]]:
    """Build wbk → reach_id mapping grouped by shard prefix.

    Returns {prefix: {wbk: reach_id}}.
    """
    return group_by_prefix(poly_reaches)


def build_tier0(
    regulations: dict[str, Any],
    reg_sets: list[str],
    search_index: list[dict[str, Any]],
    reaches: dict[str, Any],
    reach_segments: dict[str, list[str]],
    shard_version: str,
) -> dict[str, Any]:
    """Build the Tier 0 JSON payload (loaded by the browser on startup).

    Enriches each search_index entry's reach IDs with full segment metadata
    so the browser never needs the full reaches dict at load time.

    The enriched search_index replaces bare ``reaches: [rid, ...]`` with
    ``segments: [{ rid, display_name, name_variants, feature_type, reg_set_index,
    watershed_code, min_zoom, regions, bbox, length_km, waterbody_group, fids,
    tributary_reg_ids }, ...]`` where fids is the highlight list from reach_segments.
    """
    enriched_search: list[dict[str, Any]] = []
    for entry in search_index:
        entry_copy = {k: v for k, v in entry.items() if k != "reaches"}
        segments: list[dict[str, Any]] = []
        for rid in entry.get("reaches", []):
            reach = reaches.get(rid)
            if not reach:
                continue
            segments.append(
                {
                    "rid": rid,
                    "display_name": reach.get("display_name", ""),
                    "name_variants": reach.get("name_variants", []),
                    "feature_type": reach.get("feature_type", "stream"),
                    "reg_set_index": reach["reg_set_index"],
                    "watershed_code": reach.get("watershed_code", ""),
                    "min_zoom": reach.get("min_zoom", 11),
                    "regions": reach.get("regions", []),
                    "bbox": reach.get("bbox"),
                    "length_km": reach.get("length_km", 0),
                    "waterbody_group": entry.get("waterbody_group", ""),
                    "fids": reach_segments.get(rid, []),
                    "tributary_reg_ids": reach.get("tributary_reg_ids", []),
                }
            )
        entry_copy["segments"] = segments
        enriched_search.append(entry_copy)

    return {
        "_shard_version": shard_version,
        "regulations": regulations,
        "reg_sets": reg_sets,
        "search_index": enriched_search,
    }


def generate_manifest(
    fid_count: int,
    reach_count: int,
    poly_count: int,
    version: str,
) -> dict[str, Any]:
    """Build the MANIFEST.json content for deploy atomicity."""
    return {
        "version": version,
        "status": "complete",
        "shard_counts": {
            "fids": fid_count,
            "reaches": reach_count,
            "polys": poly_count,
        },
    }


def shard_from_dict(
    data: dict[str, Any],
    output_dir: str | Path,
    version: int | str,
) -> dict[str, Any]:
    """Shard an in-memory regulation index dict into R2-deployable files.

    This is the primary entry point — called directly by builder.py
    with the regulation index dict already in memory.

    Args:
        data:       The regulation index dict (reaches, reach_segments, etc.).
        output_dir: Directory to write shards/ and tier0.json.
        version:    Shard version (e.g. 7 → "v7").

    Returns:
        Summary dict with counts and paths.
    """
    output_dir = Path(output_dir)
    version_str = f"v{version}" if not str(version).startswith("v") else str(version)

    reaches: dict[str, Any] = data.get("reaches", {})
    reach_segments: dict[str, list[str]] = data.get("reach_segments", {})
    poly_reaches: dict[str, str] = data.get("poly_reaches", {})
    regulations: dict[str, Any] = data.get("regulations", {})
    reg_sets: list[str] = data.get("reg_sets", [])
    search_index: list[dict[str, Any]] = data.get("search_index", [])

    shard_root = output_dir / "shards" / version_str

    # ── Build & write fid shards ──
    t0 = time.perf_counter()
    fid_groups = build_fid_shards(reach_segments)
    fid_count = _write_shards(fid_groups, shard_root, "fids")
    total_fids = sum(len(g) for g in fid_groups.values())
    log.info(
        "fid shards: %d files, %d entries (%.1fs)",
        fid_count,
        total_fids,
        time.perf_counter() - t0,
    )

    # ── Build & write reach shards ──
    t0 = time.perf_counter()
    reach_groups = build_reach_shards(reaches, reach_segments)
    reach_shard_count = _write_shards(reach_groups, shard_root, "reaches")
    log.info(
        "reach shards: %d files, %d entries (%.1fs)",
        reach_shard_count,
        len(reaches),
        time.perf_counter() - t0,
    )

    # ── Build & write poly shards ──
    t0 = time.perf_counter()
    poly_groups = build_poly_shards(poly_reaches)
    poly_count = _write_shards(poly_groups, shard_root, "polys")
    log.info(
        "poly shards: %d files, %d entries (%.1fs)",
        poly_count,
        len(poly_reaches),
        time.perf_counter() - t0,
    )

    # ── Write MANIFEST.json ──
    manifest = generate_manifest(fid_count, reach_shard_count, poly_count, version_str)
    manifest_path = shard_root / "MANIFEST.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    log.info("MANIFEST → %s", manifest_path)

    # ── Build & write tier0.json ──
    t0 = time.perf_counter()
    tier0 = build_tier0(
        regulations,
        reg_sets,
        search_index,
        reaches,
        reach_segments,
        version_str,
    )
    tier0_path = output_dir / "tier0.json"
    with open(tier0_path, "w", encoding="utf-8") as f:
        json.dump(tier0, f, ensure_ascii=False, separators=(",", ":"))
    tier0_size = tier0_path.stat().st_size
    log.info(
        "tier0.json → %s (%.1f MB, %.1fs)",
        tier0_path,
        tier0_size / 1_048_576,
        time.perf_counter() - t0,
    )

    return {
        "version": version_str,
        "shard_root": str(shard_root),
        "tier0_path": str(tier0_path),
        "fid_shards": fid_count,
        "reach_shards": reach_shard_count,
        "poly_shards": poly_count,
        "total_fids": total_fids,
        "total_reaches": len(reaches),
        "total_polys": len(poly_reaches),
        "tier0_bytes": tier0_size,
    }


def shard_regulation_index(
    input_path: str | Path,
    output_dir: str | Path,
    version: int | str,
) -> dict[str, Any]:
    """File-based entry point — loads a regulation_index.json and shards it.

    Convenience wrapper around ``shard_from_dict()`` for CLI / standalone use.
    """
    input_path = Path(input_path)
    log.info("Loading %s ...", input_path)
    t0 = time.perf_counter()
    with open(input_path, encoding="utf-8") as f:
        data = json.load(f)
    log.info("Loaded in %.1fs", time.perf_counter() - t0)
    return shard_from_dict(data, output_dir, version)


# ── CLI ───────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Shard regulation_index.json for R2 edge deployment"
    )
    parser.add_argument(
        "--input",
        default="output/pipeline/regulation_index.json",
        help="Path to regulation_index.json",
    )
    parser.add_argument(
        "--output",
        default="output/pipeline/deploy",
        help="Output directory for shards/ and tier0.json",
    )
    parser.add_argument(
        "--version",
        type=int,
        default=7,
        help="Shard version number (default: 7)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    summary = shard_regulation_index(args.input, args.output, args.version)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
