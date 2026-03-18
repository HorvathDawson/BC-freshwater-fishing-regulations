"""
in_season_resolver — Resolve scraped in-season changes to reach IDs.

Reads the scraper output (in_season_changes.json), loads MatchTable
(base entries + overrides) to resolve water names → canonical entries,
then maps through tier0.json (regulations → reg_sets → search_index
segments) to find the reach IDs each change applies to.

Produces in_season.json — a lightweight file the frontend loads at
startup to overlay in-season corrections on existing reaches.

Dependencies: only built-in Python + pipeline.matching (no geo libs).

CLI
---
    python -m pipeline.matching.in_season_resolver
    python -m pipeline.matching.in_season_resolver --scraped path/to/in_season_changes.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import yaml
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .in_season_scraper import ScrapedResult, InSeasonChange, RegionChanges
from .match_table import BaseEntry, MatchTable, OverrideEntry, OVERRIDES_PATH

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Section → synopsis-region mapping (shared with scraper)
# ---------------------------------------------------------------------------

_SECTION_TO_REGION: Dict[str, Optional[str]] = {
    "Provincial level information": None,
    "Region 1 - Vancouver Island (includes Haida Gwaii: WMU 6-12, 6-13)": "REGION 1 - Vancouver Island",
    "Region 2 - Lower Mainland": "REGION 2 - Lower Mainland",
    "Region 3 - Thompson": "REGION 3 - Thompson-Nicola",
    "Region 4 - Kootenay": "REGION 4 - Kootenay",
    "Region 5 - Cariboo": "REGION 5 - Cariboo",
    "Region 6 - Skeena": "REGION 6 - Skeena",
    "Region 7A - Omineca": "REGION 7A - Omineca",
    "Region 7B - Peace": "REGION 7B - Peace",
    "Region 8 - Okanagan": "REGION 8 - Okanagan",
}


# ---------------------------------------------------------------------------
# tier0 index helpers
# ---------------------------------------------------------------------------


def _build_water_to_reg_ids(
    regulations: Dict[str, Dict[str, Any]],
) -> Dict[Tuple[str, Optional[str]], Set[str]]:
    """Build (UPPER_WATER, region) → {reg_id, ...} from tier0 regulations.

    Only synopsis regulations are included (source == "synopsis" or no
    source field — base regs use "zone"/"provincial").
    """
    index: Dict[Tuple[str, Optional[str]], Set[str]] = defaultdict(set)
    for reg_id, reg in regulations.items():
        source = reg.get("source", "")
        if source in ("zone", "provincial"):
            continue
        water = (reg.get("water") or "").upper()
        region = reg.get("region") or None
        if water:
            index[(water, region)].add(reg_id)
    return dict(index)


def _build_reg_id_to_reach_ids(
    search_index: List[Dict[str, Any]],
    reg_sets: List[str],
) -> Dict[str, Set[str]]:
    """Build reg_id → {reach_id, ...} from tier0 search_index segments.

    Only includes reaches where the reg_id is a *direct* match — not
    inherited via tributary BFS.  Each segment carries a
    ``tributary_reg_ids`` list; any reg_id in that list is inherited and
    excluded so that an in-season change to "Duncan River" doesn't
    cascade to every tributary that inherited the Duncan River regulation.
    """
    index: Dict[str, Set[str]] = defaultdict(set)
    for entry in search_index:
        for seg in entry.get("segments", []):
            rid = seg["rid"]
            rsi = seg["reg_set_index"]
            if rsi >= len(reg_sets):
                continue
            trib_set = set(seg.get("tributary_reg_ids") or [])
            for reg_id in reg_sets[rsi].split(","):
                reg_id = reg_id.strip()
                if reg_id and reg_id not in trib_set:
                    index[reg_id].add(rid)
    return dict(index)


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


def _resolve_water_entry(
    water: str,
    region: Optional[str],
    table: MatchTable,
) -> Optional[BaseEntry]:
    """Resolve an in-season water name to a MatchTable entry.

    Uses MatchTable (base entries + overrides with variant_of).
    Returns the resolved entry, or None if unmatched.

    In-season changes don't include MUs, so we use ignore_mus=True
    to fall back to matching on (region, name) regardless of MU set.
    The fallback scan is case-insensitive.
    """
    entry = table.lookup(water, region, [], ignore_mus=True)

    if entry is None:
        return None

    # Follow variant_of: if the entry is a skipped variant, resolve to
    # the target entry's canonical name instead.
    if (
        isinstance(entry, OverrideEntry)
        and entry.skip
        and entry.variant_of is not None
    ):
        target = table.lookup(
            entry.variant_of.name_verbatim,
            entry.variant_of.region,
            entry.variant_of.mus,
            ignore_mus=True,
        )
        if target is not None:
            entry = target

    return entry


def _build_gnis_to_water_names(
    table: MatchTable,
) -> Dict[str, Set[str]]:
    """Build gnis_id → {UPPER_WATER_NAME, ...} from all non-skipped entries.

    Used to expand multi-water overrides (e.g. "CHILLIWACK LAKE, UPPER
    PITT RIVER" with GNIS [13745, 7551]) into the individual water names
    that have regulations in tier0.
    """
    index: Dict[str, Set[str]] = defaultdict(set)
    for entry in table.all_entries():
        if isinstance(entry, OverrideEntry) and entry.skip:
            continue
        water = (entry.criteria.name_verbatim or "").upper()
        for gid in getattr(entry, "gnis_ids", None) or []:
            index[gid].add(water)
    return dict(index)


def resolve_to_reaches(
    scraped: ScrapedResult,
    table: MatchTable,
    tier0: Dict[str, Any],
) -> Dict[str, Any]:
    """Resolve all scraped in-season changes to reach IDs.

    Returns the fully-resolved in_season.json structure.
    """
    regulations = tier0.get("regulations", {})
    reg_sets = tier0.get("reg_sets", [])
    search_index = tier0.get("search_index", [])

    # Build lookup indexes
    water_reg_ids = _build_water_to_reg_ids(regulations)
    reg_id_reaches = _build_reg_id_to_reach_ids(search_index, reg_sets)
    gnis_to_waters = _build_gnis_to_water_names(table)

    changes: List[Dict[str, Any]] = []
    unmatched: List[str] = []
    stats = {"total": 0, "matched": 0, "unmatched": 0}

    for section in scraped.sections:
        synopsis_region = _SECTION_TO_REGION.get(section.section)

        for row in section.rows:
            stats["total"] += 1
            entry = _resolve_water_entry(row.water, synopsis_region, table)

            if entry is None:
                unmatched.append(row.water)
                stats["unmatched"] += 1
                changes.append(
                    {
                        "water": row.water,
                        "region": section.section,
                        "change": row.change,
                        "effective_date": row.effective_date,
                        "reach_ids": [],
                        "match_status": "unmatched",
                    }
                )
                continue

            canonical = (entry.criteria.name_verbatim or "").upper()

            # Find reg_ids for canonical water + region
            reg_ids = water_reg_ids.get((canonical, synopsis_region), set())

            # If no region match, try without region
            if not reg_ids:
                for (w, r), rids in water_reg_ids.items():
                    if w == canonical:
                        reg_ids |= rids

            # If still no reg_ids and entry has GNIS IDs, expand via
            # GNIS → individual water names that have their own regs.
            # Handles multi-water overrides like "CHILLIWACK LAKE, UPPER
            # PITT RIVER" whose GNIS IDs map to separate regulation entries.
            if not reg_ids:
                gnis_ids = getattr(entry, "gnis_ids", None) or []
                for gid in gnis_ids:
                    for water_name in gnis_to_waters.get(gid, set()):
                        if water_name == canonical:
                            continue
                        reg_ids |= water_reg_ids.get(
                            (water_name, synopsis_region), set()
                        )

            # Collect reach_ids from those reg_ids
            reach_ids: Set[str] = set()
            for reg_id in reg_ids:
                reach_ids |= reg_id_reaches.get(reg_id, set())

            stats["matched"] += 1
            changes.append(
                {
                    "water": row.water,
                    "region": section.section,
                    "change": row.change,
                    "effective_date": row.effective_date,
                    "reach_ids": sorted(reach_ids),
                    "match_status": "matched" if reach_ids else "matched_no_reaches",
                }
            )

    return {
        "scraped_at": scraped.scraped_at,
        "source_url": scraped.source_url,
        "changes": changes,
        "unmatched": unmatched,
        "stats": stats,
    }


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------


def _load_scraped(path: Path) -> ScrapedResult:
    """Load a ScrapedResult from in_season_changes.json."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    sections = [
        RegionChanges(
            section=s["section"],
            rows=[InSeasonChange(**r) for r in s.get("rows", [])],
        )
        for s in data.get("sections", [])
    ]
    return ScrapedResult(
        scraped_at=data.get("scraped_at", ""),
        source_url=data.get("source_url", ""),
        sections=sections,
    )


def _load_match_table(
    match_table_path: Path,
    overrides_path: Path,
) -> MatchTable:
    """Load MatchTable from base entries + overrides."""
    with open(match_table_path, encoding="utf-8") as f:
        base_dicts = json.load(f)
    base_entries = [BaseEntry.from_dict(d) for d in base_dicts]

    with open(overrides_path, encoding="utf-8") as f:
        override_dicts = json.load(f)
    overrides = [OverrideEntry.from_dict(d) for d in override_dicts]

    return MatchTable(bases=base_entries, overrides=overrides)


def _print_results(result: Dict[str, Any]) -> None:
    """Print resolution summary."""
    stats = result["stats"]
    print()
    print("=" * 70)
    print("  IN-SEASON CHANGE RESOLUTION")
    print("=" * 70)
    for change in result["changes"]:
        water = change["water"][:40].ljust(40)
        n_reaches = len(change["reach_ids"])
        status = change["match_status"]
        if status == "unmatched":
            print(f"  ❌ {water}  unmatched")
        elif status == "matched_no_reaches":
            print(f"  🔶 {water}  matched (0 reaches)")
        else:
            print(f"  ✅ {water}  {n_reaches} reach(es)")
    print()
    print(f"  Total: {stats['total']}")
    print(f"  Matched: {stats['matched']}")
    print(f"  Unmatched: {stats['unmatched']}")
    if result["unmatched"]:
        print(f"\n  Unmatched waters:")
        for w in result["unmatched"]:
            print(f"    - {w}")
    print("=" * 70)
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resolve in-season changes to reach IDs."
    )
    parser.add_argument(
        "--scraped",
        default=None,
        help="Path to in_season_changes.json (default: from config.yaml)",
    )
    parser.add_argument(
        "--tier0",
        default=None,
        help="Path to tier0.json (default: output/pipeline/deploy/tier0.json)",
    )
    parser.add_argument(
        "--match-table",
        default=None,
        help="Path to match_table.json (default: from config.yaml)",
    )
    parser.add_argument(
        "--overrides",
        default=None,
        help="Path to overrides.json (default: pipeline/matching/overrides.json)",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output path for in_season.json (default: output/pipeline/deploy/in_season.json)",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress summary output")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Resolve defaults from config.yaml
    project_root = Path(__file__).resolve().parents[2]
    with open(project_root / "config.yaml") as f:
        cfg = yaml.safe_load(f)
    v2_cfg = cfg["output"]["pipeline"]

    scraped_path = Path(args.scraped) if args.scraped else Path(v2_cfg["in_season"])
    match_table_path = (
        Path(args.match_table) if args.match_table else Path(v2_cfg["match_table"])
    )
    overrides_path = Path(args.overrides) if args.overrides else OVERRIDES_PATH
    tier0_path = (
        Path(args.tier0) if args.tier0 else Path(v2_cfg["deploy"]) / "tier0.json"
    )
    out_path = Path(args.out) if args.out else Path(v2_cfg["deploy"]) / "in_season.json"

    # Validate inputs exist
    for label, path in [
        ("Scraped changes", scraped_path),
        ("Match table", match_table_path),
        ("Overrides", overrides_path),
        ("tier0", tier0_path),
    ]:
        if not path.exists():
            print(f"ERROR: {label} not found: {path}", file=sys.stderr)
            sys.exit(1)

    # Load data
    print(f"Loading scraped changes: {scraped_path}")
    scraped = _load_scraped(scraped_path)
    total_changes = sum(len(s.rows) for s in scraped.sections)
    print(f"  {total_changes} in-season changes")

    print(f"Loading match table: {match_table_path}")
    print(f"Loading overrides: {overrides_path}")
    table = _load_match_table(match_table_path, overrides_path)

    print(f"Loading tier0: {tier0_path}")
    with open(tier0_path, encoding="utf-8") as f:
        tier0 = json.load(f)

    # Resolve
    result = resolve_to_reaches(scraped, table, tier0)

    if not args.quiet:
        _print_results(result)

    # Write output
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"Wrote {len(result['changes'])} changes → {out_path}")


if __name__ == "__main__":
    main()
