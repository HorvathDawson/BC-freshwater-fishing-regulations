#!/usr/bin/env python3
"""
match_fwa_override.py — fourth and final pass, for whatever the automated
override + gazetteer + FWA-identifier tiers (match_fwa_gazette.py,
match_fwa_identifier.py) couldn't resolve at all: a small, hand-curated
correction file, `wbid_overrides.json`, in this same directory.

Not the same override file the other tiers already check
------------------------------------------------------------
match_fwa_gazette.py's own "override" tier already checks
`common/waterbody_matcher.py::build_override_index()` —
`pipeline/matching/overrides.json`, the *production* regulations pipeline's
own curated table, keyed by name. `wbid_overrides.json` here is a second,
separate file, deliberately kept apart the same way
`bathymetry/bathy_overrides.json` is kept apart from the production
overrides — these entries exist to fix problems specific to *this testbed's*
remaining stragglers (an unnamed FWA polygon no name search could ever find;
a common name like "Rock Lake" no automated tier could safely disambiguate),
not regulation-matching corrections, and this stays a testbed per
live-data/README.md, so it shouldn't carry write access to a production
table. Keyed by **identifier**, not name (unlike the production overrides
file): several of the remaining rows share the same identifier across
sources with different, non-matching names (e.g. bathymetry's "MCIVOR LAKE"
and stocking's "MCIVOR", both identifier `00095CAMB`) — keying by identifier
resolves both with one entry, correctly, whereas a name-keyed lookup would
need two separate (and inconsistent) entries for the same underlying fix.

How each entry was found
--------------------------
Every `waterbody_key` in `wbid_overrides.json` was confirmed against a real,
independent geographic signal — never a name guess alone:
  - `00095CAMB` ("MCIVOR"/"MCIVOR LAKE") — manually looked up against BC's
    own waterbody data (GNIS Name 1 "Campbell Lake"); confirmed live the
    WDIC anchor point for this identifier falls exactly inside that
    polygon (0m).
  - `01032NICL` ("LITTLE ROCK") — "Rock Lake" has 6 province-wide gazetteer
    candidates, too common a name for any automated tier to pick safely;
    the WDIC anchor point falls exactly inside one specific candidate
    (0m, next-closest 132.5km away) — real ground truth settles what name
    alone can't.
  - `00728PARK`/`00748PARK` ("COLLIERY #3"/"HAREWOOD #3" and
    "COLLIERY"/"HAREWOOD RESERVOIR") — both entirely unnamed FWA polygons
    (no GNIS name, no group code — outside every name- and identifier-based
    tier this whole module chain has), resolved by a direct point-in-polygon
    lookup (`common/waterbody_matcher.py::locate_at_point()`) at each
    identifier's own WDIC anchor point instead — two distinct nearby
    waterbodies, not the same lake, confirmed by their own distinct anchor
    coordinates.
Entries with `"skip": true` (Jubilee, HARRISON BAY LAKE, TOD) are confirmed
to have no current FWA polygon at all — recorded so future runs report them
as "reviewed, no polygon exists" rather than an unexplained gap.

CLI
---
    cd live-data/waterbody_db
    python match_fwa_override.py                 # run after match_fwa_identifier.py; writes match_fwa_override
    python match_fwa_override.py --dry-run        # print only, no DB writes
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import waterbody_matcher as wm
from project_config import ProjectConfig

from fetch_wdic import DB_PATH
from match import osm_url
from match_fwa_gazette import _VARIANT_JOIN

logger = logging.getLogger(__name__)

OVERRIDES_PATH = Path(__file__).parent / "wbid_overrides.json"


@dataclass
class OverrideEntry:
    waterbody_keys: List[str] = field(default_factory=list)  # usually one; >1 for a genuine multi-part waterbody (see SPECTACLE - SWAN LAKES in wbid_overrides.json)
    skip: bool = False
    note: str = ""


def load_wbid_overrides(path: Path = OVERRIDES_PATH) -> Dict[str, OverrideEntry]:
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    return {
        entry["identifier"]: OverrideEntry(
            waterbody_keys=entry.get("waterbody_keys", []),
            skip=bool(entry.get("skip", False)),
            note=entry.get("note", ""),
        )
        for entry in raw
    }


@dataclass
class UnmatchedRow:
    source: str
    source_id: str
    identifier: str
    name: str
    name_variants: List[str]
    approx_lat: float
    approx_lng: float


def load_unmatched(conn: sqlite3.Connection) -> List[UnmatchedRow]:
    """Only match_fwa_identifier rows still status='unmatched' — the very
    last thing three automated tiers couldn't resolve. See module
    docstring."""
    rows = conn.execute(
        """SELECT source, source_id, identifier, name, name_variants, approx_lat, approx_lng
           FROM match_fwa_identifier WHERE status = 'unmatched'"""
    ).fetchall()
    return [
        UnmatchedRow(source, source_id, identifier, name,
                     [v for v in (variants or "").split(_VARIANT_JOIN) if v], lat, lng)
        for source, source_id, identifier, name, variants, lat, lng in rows
    ]


@dataclass
class FinalMatch:
    source: str
    source_id: str
    identifier: str
    name: str
    name_variants: List[str]
    approx_lat: float
    approx_lng: float
    status: str                # matched | ignored | unmatched
    waterbody_key: str = ""
    gnis_name: str = ""
    source_layer: str = ""
    note: str = ""              # curator's own reasoning — always populated for a curated entry, matched or ignored


def match_override(
    rows: List[UnmatchedRow], overrides: Dict[str, OverrideEntry], index: "wm.LakeIndex",
) -> List[FinalMatch]:
    results: List[FinalMatch] = []
    for r in rows:
        entry = overrides.get(r.identifier)
        if entry is None:
            results.append(FinalMatch(
                r.source, r.source_id, r.identifier, r.name, r.name_variants,
                r.approx_lat, r.approx_lng, "unmatched",
            ))
            continue
        if entry.skip or not entry.waterbody_keys:
            # Curated + confirmed to have no current FWA polygon at all —
            # distinct from "unmatched" (which means "no curated entry
            # exists yet, still genuinely unresolved"). See wbid_overrides.json.
            results.append(FinalMatch(
                r.source, r.source_id, r.identifier, r.name, r.name_variants,
                r.approx_lat, r.approx_lng, "ignored", note=entry.note,
            ))
            continue
        # Usually one key; >1 for a genuine multi-part waterbody (see
        # SPECTACLE - SWAN LAKES) — aggregated the same way
        # common/waterbody_matcher.py's build_resolved() does for its own
        # multi-part ties, not just the first key silently dropped.
        names = sorted({index.display_name.get(k, "") for k in entry.waterbody_keys} - {""})
        layers = sorted({index.source_layer.get(k, "") for k in entry.waterbody_keys} - {""})
        results.append(FinalMatch(
            r.source, r.source_id, r.identifier, r.name, r.name_variants,
            r.approx_lat, r.approx_lng, "matched",
            waterbody_key=",".join(entry.waterbody_keys),
            gnis_name=", ".join(names),
            source_layer=",".join(layers),
            note=entry.note,
        ))
    return results


def write_matches(conn: sqlite3.Connection, results: List[FinalMatch]) -> None:
    conn.execute("DROP TABLE IF EXISTS match_fwa_override")
    conn.execute(
        """CREATE TABLE match_fwa_override (
            source              TEXT NOT NULL,
            source_id           TEXT NOT NULL,
            identifier          TEXT,
            name                TEXT,
            name_variants       TEXT,
            approx_lat          REAL,
            approx_lng          REAL,
            approx_map_url      TEXT,
            status              TEXT NOT NULL,
            waterbody_key       TEXT,
            gnis_name           TEXT,
            source_layer        TEXT,
            note                TEXT,
            matched_at          TEXT,
            PRIMARY KEY (source, source_id)
        )"""
    )
    now = datetime.now(timezone.utc).isoformat()
    conn.executemany(
        """INSERT INTO match_fwa_override
           (source, source_id, identifier, name, name_variants, approx_lat, approx_lng, approx_map_url,
            status, waterbody_key, gnis_name, source_layer, note, matched_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
            (r.source, r.source_id, r.identifier, r.name, _VARIANT_JOIN.join(r.name_variants),
             r.approx_lat, r.approx_lng, osm_url(r.approx_lat, r.approx_lng), r.status,
             r.waterbody_key, r.gnis_name, r.source_layer, r.note, now)
            for r in results
        ],
    )
    conn.commit()


def _print_breakdown(results: List[FinalMatch]) -> None:
    print("\n" + "=" * 78)
    print("FWA_IDENTIFIER_MATCHES (unmatched) -> CURATED wbid_overrides.json")
    print("=" * 78)
    matched = [r for r in results if r.status == "matched"]
    ignored = [r for r in results if r.status == "ignored"]
    unreviewed = [r for r in results if r.status == "unmatched"]

    print(f"\n{len(matched)} matched, {len(ignored)} confirmed no-polygon (ignored, curated), "
          f"{len(unreviewed)} not yet reviewed at all — of {len(results)} total.")

    if matched:
        print("\nMatched:")
        for r in matched:
            print(f"  {r.source:12} {r.identifier:10} {r.name or r.name_variants[0] if r.name_variants else r.name:30} "
                  f"-> waterbody_key={r.waterbody_key} ({r.gnis_name or 'unnamed'})")
    if ignored:
        print("\nConfirmed no FWA polygon (ignored):")
        for r in ignored:
            print(f"  {r.source:12} {r.identifier:10} {r.name or (r.name_variants[0] if r.name_variants else '')}")
    if unreviewed:
        print("\nNot yet reviewed:")
        for r in unreviewed:
            print(f"  {r.source:12} {r.identifier:10} {r.name or (r.name_variants[0] if r.name_variants else '')} "
                  f"— {r.approx_lat},{r.approx_lng}")


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    cfg = ProjectConfig()
    parser = argparse.ArgumentParser(
        description="For match_fwa_identifier rows still unmatched, apply this "
                    "directory's own curated wbid_overrides.json.",
    )
    parser.add_argument(
        "--gpkg", type=Path,
        default=cfg.get_path("data", "fetch", "output_gpkg", default="data/bc_fisheries_data.gpkg"),
        help="FWA GeoPackage (lakes/wetlands/manmade layers) — needed to display "
             "each override's own GNIS name/layer, not to re-derive the match.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Match and print the breakdown without writing match_fwa_override to waterbody_db.db.",
    )
    args = parser.parse_args(argv)

    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"{DB_PATH} not found — run fetch_all.py, match.py, match_fwa_gazette.py, "
            "then match_fwa_identifier.py first."
        )
    if not args.gpkg.exists():
        raise FileNotFoundError(f"FWA GeoPackage not found at {args.gpkg}")

    overrides = load_wbid_overrides()
    logger.info("%d curated identifier(s) loaded from %s.", len(overrides), OVERRIDES_PATH)

    conn = sqlite3.connect(DB_PATH)
    try:
        rows = load_unmatched(conn)
        if not rows:
            logger.info("Nothing unmatched in match_fwa_identifier — nothing for this stage to do.")

        logger.info("Building FWA lake index from %s ...", args.gpkg)
        index = wm.build_lake_index(args.gpkg)

        results = match_override(rows, overrides, index)

        if args.dry_run:
            logger.info("--dry-run: not writing match_fwa_override to waterbody_db.db")
        else:
            write_matches(conn, results)
    finally:
        conn.close()

    _print_breakdown(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
