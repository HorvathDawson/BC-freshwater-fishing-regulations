#!/usr/bin/env python3
"""
match_fwa_identifier.py — third pass, for whatever match_fwa_gazette.py's
override + gazetteer tiers left unmatched: try FWA's own group-code
identifier join directly.

Why this exists, and why it's tier 3 not tier 1
----------------------------------------------------
`common/waterbody_matcher.py`'s full T1-T4 cascade — the one
`stocking/match_waterbodies.py` and `bathymetry/match_bathymetry.py` both
run — puts this exact join (T1_identifier: the row's own 50K wbid matched
directly against FWA's `WATERBODY_KEY_GROUP_CODE_50K` column, which carries
the *same string* WDIC's own `WATERBODY_IDENTIFIER` does — see that module's
own docstring) *first*, and it resolves the large majority of rows on its
own there. This module chain deliberately did the opposite ordering instead:
match.py already used the identifier once, against WDIC (a frozen reference
snapshot); match_fwa_gazette.py then corroborated by name (override, then
gazetteer search) *independently* of the identifier entirely. Only what
neither of those independent signals could resolve reaches this module,
which finally tries the identifier again — this time against FWA's *own*,
*current* group-code column, not WDIC's frozen one. Whatever this resolves
is worth a second look either way: an identifier a name search couldn't
corroborate at all is exactly the kind of row `common/waterbody_matcher.py`'s
own `_confirmed_by_name()` would flag for review, not accept blindly — see
"Ties" below for how that's handled.

Input: match_fwa_gazette only
-----------------------------------
Reads *only* `match_fwa_gazette` (`status = 'unmatched'`) — never
`match_wbid` or the raw per-source tables — same "read only the immediately
prior stage's own output" discipline match_fwa_gazette.py itself follows.
`identifier` and the WDIC anchor point (`approx_lat`/`approx_lng`, carried
through onto every match_fwa_gazette row, matched or not, for exactly this
reason) both come from that one table.

Ties: distance to the WDIC anchor, same as tier 2
--------------------------------------------------------
A group code can map to more than one FWA `WATERBODY_KEY` (FWA's own
multi-part-waterbody grouping, or a genuine coincidental collision) — when it
does, this picks whichever candidate's polygon sits closest to the row's own
WDIC anchor point, the same `_closest()` logic match_fwa_gazette.py already
uses for its own ties (reused here rather than reimplemented — see that
module).

CLI
---
    python -m pipeline.recurring.anglerinfo.match_fwa_identifier                 # run after match_fwa_gazette.py; writes match_fwa_identifier
    python -m pipeline.recurring.anglerinfo.match_fwa_identifier --dry-run        # print only, no DB writes
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pyproj import Transformer
from shapely.geometry import Point

from . import waterbody_matcher as wm
from project_config import ProjectConfig

from .fetch_wdic import DB_PATH
from .match import osm_url
from .match_fwa_gazette import _closest, _VARIANT_JOIN

logger = logging.getLogger(__name__)

_TO_ALBERS = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)


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
    """Only match_fwa_gazette rows still status='unmatched' — see module
    docstring's "Input: match_fwa_gazette only"."""
    rows = conn.execute(
        """SELECT source, source_id, identifier, name, name_variants, approx_lat, approx_lng
           FROM match_fwa_gazette WHERE status = 'unmatched'"""
    ).fetchall()
    return [
        UnmatchedRow(source, source_id, identifier, name,
                     [v for v in (variants or "").split(_VARIANT_JOIN) if v], lat, lng)
        for source, source_id, identifier, name, variants, lat, lng in rows
    ]


@dataclass
class IdentifierMatch:
    source: str
    source_id: str
    identifier: str
    name: str
    name_variants: List[str]
    status: str                # matched | unmatched (identifier not in FWA's own group-code column at all)
    n_candidates: int = 0        # size of the group-code pool, before distance picks a winner (>1 = FWA multi-part waterbody or a group-code collision)
    waterbody_key: str = ""
    approx_lat: float = 0.0        # carried through from match_wbid via match_fwa_gazette — WDIC's own anchor point, for any downstream consumer (map pin, further point-in-polygon, etc.)
    approx_lng: float = 0.0
    gnis_name: str = ""
    source_layer: str = ""
    distance_m: Optional[float] = None   # winning candidate's polygon distance from the WDIC anchor point (0 = point falls inside it)


def match_identifier(rows: List[UnmatchedRow], index: "wm.LakeIndex") -> List[IdentifierMatch]:
    results: List[IdentifierMatch] = []
    for r in rows:
        pool = index.group_code_map.get(r.identifier, set()) if r.identifier else set()
        if not pool:
            results.append(IdentifierMatch(
                r.source, r.source_id, r.identifier, r.name, r.name_variants, "unmatched",
                approx_lat=r.approx_lat, approx_lng=r.approx_lng,
            ))
            continue

        x, y = _TO_ALBERS.transform(r.approx_lng, r.approx_lat)
        key, dist = _closest(pool, Point(x, y), index)
        if key is None:
            results.append(IdentifierMatch(
                r.source, r.source_id, r.identifier, r.name, r.name_variants, "unmatched",
                approx_lat=r.approx_lat, approx_lng=r.approx_lng, n_candidates=len(pool),
            ))
            continue

        results.append(IdentifierMatch(
            r.source, r.source_id, r.identifier, r.name, r.name_variants, "matched",
            approx_lat=r.approx_lat, approx_lng=r.approx_lng,
            n_candidates=len(pool), waterbody_key=key,
            gnis_name=index.display_name.get(key, ""),
            source_layer=index.source_layer.get(key, ""),
            distance_m=dist,
        ))
    return results


def write_matches(conn: sqlite3.Connection, results: List[IdentifierMatch]) -> None:
    conn.execute("DROP TABLE IF EXISTS match_fwa_identifier")
    conn.execute(
        """CREATE TABLE match_fwa_identifier (
            source              TEXT NOT NULL,
            source_id           TEXT NOT NULL,
            identifier          TEXT,
            name                TEXT,
            name_variants       TEXT,
            approx_lat          REAL,
            approx_lng          REAL,
            approx_map_url      TEXT,
            status              TEXT NOT NULL,
            n_candidates        INTEGER,
            waterbody_key       TEXT,
            gnis_name           TEXT,
            source_layer        TEXT,
            distance_m          REAL,
            matched_at          TEXT,
            PRIMARY KEY (source, source_id)
        )"""
    )
    now = datetime.now(timezone.utc).isoformat()
    conn.executemany(
        """INSERT INTO match_fwa_identifier
           (source, source_id, identifier, name, name_variants, approx_lat, approx_lng, approx_map_url,
            status, n_candidates, waterbody_key, gnis_name, source_layer, distance_m, matched_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
            (r.source, r.source_id, r.identifier, r.name, _VARIANT_JOIN.join(r.name_variants),
             r.approx_lat, r.approx_lng, osm_url(r.approx_lat, r.approx_lng), r.status, r.n_candidates,
             r.waterbody_key, r.gnis_name, r.source_layer, r.distance_m, now)
            for r in results
        ],
    )
    conn.commit()


def _print_breakdown(results: List[IdentifierMatch]) -> None:
    print("\n" + "=" * 78)
    print("FWA_GAZETTE_MATCHES (unmatched) -> FWA GROUP-CODE IDENTIFIER JOIN")
    print("=" * 78)
    for source in ("stocking", "bathymetry", "map_markers"):
        subset = [r for r in results if r.source == source]
        if not subset:
            continue
        matched = [r for r in subset if r.status == "matched"]
        total = len(subset)
        pct = f"{len(matched) / total:.1%}" if total else "—"
        print(f"\n{source}: {len(matched)}/{total} of the round-2-unmatched rows now matched ({pct})")

    total = len(results)
    matched = [r for r in results if r.status == "matched"]
    print(f"\nCOMBINED: {len(matched)}/{total} of round 2's unmatched rows matched in round 3 "
          f"({len(matched)/total:.1%})" if total else "\nNothing was unmatched after round 2 — nothing to try here.")

    ties = [r for r in matched if r.n_candidates > 1]
    if ties:
        print(f"  {len(ties)} row(s) had a group code shared by >1 FWA waterbody_key "
              "(multi-part waterbody or collision) — resolved by distance to the WDIC anchor")


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    cfg = ProjectConfig()
    parser = argparse.ArgumentParser(
        description="For match_fwa_gazette rows still unmatched, try FWA's own "
                    "WATERBODY_KEY_GROUP_CODE_50K identifier join directly.",
    )
    parser.add_argument(
        "--gpkg", type=Path,
        default=cfg.get_path("data", "fetch", "output_gpkg", default="data/bc_fisheries_data.gpkg"),
        help="FWA GeoPackage (lakes/wetlands/manmade layers).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Match and print the breakdown without writing match_fwa_identifier to anglerinfo.db.",
    )
    args = parser.parse_args(argv)

    if not DB_PATH.exists():
        raise FileNotFoundError(f"{DB_PATH} not found — run fetch_all.py, match.py, then match_fwa_gazette.py first.")
    if not args.gpkg.exists():
        raise FileNotFoundError(f"FWA GeoPackage not found at {args.gpkg}")

    conn = sqlite3.connect(DB_PATH)
    try:
        rows = load_unmatched(conn)
        if not rows:
            raise RuntimeError(
                f"{DB_PATH} has no match_fwa_gazette rows at all — run match_fwa_gazette.py first."
            )
        logger.info("%d row(s) still unmatched after match_fwa_gazette.py's override+gazetteer tiers.", len(rows))

        logger.info("Building FWA lake index from %s ...", args.gpkg)
        index = wm.build_lake_index(args.gpkg)

        results = match_identifier(rows, index)

        if args.dry_run:
            logger.info("--dry-run: not writing match_fwa_identifier to anglerinfo.db")
        else:
            write_matches(conn, results)
    finally:
        conn.close()

    _print_breakdown(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
