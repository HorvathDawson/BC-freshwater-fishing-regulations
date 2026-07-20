#!/usr/bin/env python3
"""
match_wbid_gazette.py — a fifth stage, but for a different population than
match_fwa_gazette.py/match_fwa_identifier.py/match_fwa_override.py: rows
`match.py` itself never matched to WDIC at all (`match_wbid.status =
'unmatched'`) — so there's no WDIC anchor point to disambiguate a name
search's candidates by, the way every other stage in this chain does.

Two different anchors, by source
------------------------------------
  * `map_markers` — gofishbc markers carry their **own** lat/lng
    (`map_markers.lat`/`.lng`, independent of WDIC entirely). This module
    joins back to that one table for exactly this population (the only
    exception to the rest of this chain's "read only the immediately prior
    stage" discipline — there is no prior-stage coordinate to read at all
    for these rows). When a name search finds candidates, ties are
    disambiguated the same way every other stage does: `_closest()`
    (imported from match_fwa_gazette.py, not reimplemented), picking
    whichever candidate's FWA polygon sits closest to the marker's own
    point. When a name search finds *nothing at all* — confirmed live for
    all 4 of this population's markers ("Green Timbers", "Hall Road Pond",
    "Ida Anne", "Sanctuary Pond") — falls back to a direct point-in-polygon
    lookup at the marker's own coordinate
    (`common/waterbody_matcher.py::locate_at_point()`, reused not
    reimplemented): exactly `stocking/match_waterbodies.py`'s own documented
    T1_geo/T2_geo precedent for a marker whose target FWA polygon is real
    but entirely unnamed, so no name-based tier could ever reach it (their
    own worked example is literally "Hall Road Pond"). "Green Timbers"
    additionally self-confirms: the polygon located this way carries group
    code `01184LFRA` — the row's *own* identifier, exactly.
  * `stocking`/`bathymetry` — FIDQ/WSA carry no coordinate of their own at
    all, so there's no anchor to disambiguate by. Two independent signals
    instead, cross-validated against each other rather than either one ever
    trusted alone — the gazetteer name search's own result is checked
    against the row's own identifier, and the identifier join's own result
    is checked against the row's own name, mirroring
    `common/waterbody_matcher.py::match_row_t1()`'s own
    identifier-join-then-narrow-then-`_confirmed_by_name()` design (reused
    directly, not reimplemented) in both directions:
      - A gazetteer name hit is checked against the row's own 50K identifier
        suffix (zfill(WATERBODY_KEY_50K, 5) + WATERSHED_GROUP_CODE_50K —
        e.g. `50002SQAM`'s suffix is `SQAM`) via `_group_code_agrees()`: a
        candidate whose own `WATERSHED_GROUP_CODE_50K` actively disagrees is
        dropped; a candidate with **no** group code recorded at all passes
        as inconclusive, not a contradiction — confirmed live, "DEADMANS
        LAKE"'s own polygon has no group code whatsoever (a real FWA lake
        entirely outside the 1:50K scheme, which is exactly why no T1/T2
        tier could ever reach it), so requiring an exact match would have
        wrongly rejected a correct name-only match. Also used to narrow a
        multi-candidate name search down to one, same as
        `match_row_fallback()`'s own T3 tie-narrowing.
      - A unique `index.group_code_map` hit is checked against the row's own
        name (+ every name_variants entry) via `wm._confirmed_by_name()`
        (the exact function `match_row_t1()` itself calls) — confirmed live,
        stocking's "GREEN TIMBERS" (`01184LFRA`) has no gazetted name at all
        (inconclusive, passes) and resolves via this identifier join alone,
        landing on the *exact same* polygon a marker sharing the identical
        identifier independently located by point-in-polygon — real
        cross-confirmation between two unrelated methods, not a guess.

Input: match_wbid only (+ map_markers for its own coordinates)
--------------------------------------------------------------------
Reads `match_wbid` (`status = 'unmatched'`) — never
`fidq_stocking_records`/`bathy_surveys` — plus a direct `map_markers.lat`/
`.lng` lookup for marker rows specifically (see above; no other table in
this chain has a coordinate for these rows to read instead).

CLI
---
    cd live-data/waterbody_db
    python match_wbid_gazette.py                 # run after match.py; writes match_wbid_gazette
    python match_wbid_gazette.py --dry-run        # print only, no DB writes
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyproj import Transformer
from shapely.geometry import Point

import waterbody_matcher as wm
from project_config import ProjectConfig

from fetch_wdic import DB_PATH
from match import osm_url
from match_fwa_gazette import _VARIANT_JOIN, _closest, _search_candidates

logger = logging.getLogger(__name__)

_TO_ALBERS = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)


@dataclass
class UnmatchedRow:
    source: str
    source_id: str
    identifier: str
    name: str
    name_variants: List[str]
    marker_lat: Optional[float] = None   # map_markers' own coordinate — only ever set for source == "map_markers"
    marker_lng: Optional[float] = None


def load_unmatched(conn: sqlite3.Connection) -> List[UnmatchedRow]:
    """match_wbid rows match.py itself never matched to WDIC at all — see
    module docstring. Joins map_markers for its own lat/lng (the only
    coordinate this population has access to); stocking/bathymetry rows get
    none (FIDQ/WSA carry no coordinate of their own)."""
    rows = conn.execute(
        """SELECT source, source_id, identifier, name, name_variants
           FROM match_wbid WHERE status = 'unmatched'"""
    ).fetchall()
    marker_coords: Dict[str, tuple] = {
        str(marker_id): (lat, lng)
        for marker_id, lat, lng in conn.execute("SELECT marker_id, lat, lng FROM map_markers")
    }
    results: List[UnmatchedRow] = []
    for source, source_id, identifier, name, variants in rows:
        variant_list = [v for v in (variants or "").split(_VARIANT_JOIN) if v]
        lat, lng = marker_coords.get(source_id, (None, None)) if source == "map_markers" else (None, None)
        results.append(UnmatchedRow(source, source_id, identifier, name, variant_list, lat, lng))
    return results


@dataclass
class WbidGazetteMatch:
    source: str
    source_id: str
    identifier: str
    name: str
    name_variants: List[str]
    approx_lat: Optional[float]      # the marker's own coordinate — only ever set for source == "map_markers"; NULL for stocking/bathymetry (FIDQ/WSA carry none)
    approx_lng: Optional[float]
    status: str                # matched | unmatched
    method: str = ""             # identifier+name | identifier_join | marker_anchor | marker_point_in_polygon | single_hit | watershed_group_code | ""
    n_candidates: int = 0
    waterbody_key: str = ""
    gnis_name: str = ""
    source_layer: str = ""
    distance_m: Optional[float] = None   # only set for method == "marker_anchor"


def _group_code_agrees(key: str, identifier: str, index: "wm.LakeIndex") -> bool:
    """True unless the candidate's own WATERSHED_GROUP_CODE_50K is known and
    actively disagrees with the row's own identifier suffix
    (zfill(WATERBODY_KEY_50K, 5) + WATERSHED_GROUP_CODE_50K). No group code
    recorded at all passes as inconclusive, not a contradiction — see module
    docstring's "DEADMANS LAKE" case."""
    group = identifier[5:] if len(identifier) > 5 else ""
    if not group:
        return True
    candidate_group = index.watershed_group_code.get(key)
    return candidate_group is None or candidate_group == group


def _confirmed_by_any_name(index: "wm.LakeIndex", names: List[str], pool: Set[str]) -> bool:
    """common/waterbody_matcher.py::_confirmed_by_name(), reused directly
    (not reimplemented), checked against every one of the row's own names
    (primary + variants) — any one confirming is enough. True unless at
    least one name returns real gazetteer candidates that never overlap
    ``pool``, and none of the row's names ever do."""
    saw_real_disagreement = False
    for raw_name in names:
        if not raw_name or not raw_name.strip():
            continue
        if wm._confirmed_by_name(index, raw_name, pool):
            return True
        saw_real_disagreement = True
    return not saw_real_disagreement


def match_wbid_gazette(rows: List[UnmatchedRow], index: "wm.LakeIndex") -> List[WbidGazetteMatch]:
    results: List[WbidGazetteMatch] = []
    for r in rows:
        has_marker_point = r.source == "map_markers" and r.marker_lat is not None and r.marker_lng is not None

        if has_marker_point:
            candidates = _search_candidates(index, r.name, *r.name_variants)
            if not candidates:
                # No gazetteer name to search by at all — the same situation
                # stocking/match_waterbodies.py's own T1_geo/T2_geo tiers
                # exist for (a real but entirely unnamed FWA polygon; see
                # module docstring's "Hall Road Pond" precedent). Point-in-
                # polygon needs only the coordinate, not a name.
                hit = wm.locate_at_point(index, r.marker_lat, r.marker_lng)
                if hit.waterbody_key:
                    results.append(WbidGazetteMatch(
                        r.source, r.source_id, r.identifier, r.name, r.name_variants,
                        r.marker_lat, r.marker_lng, "matched",
                        method="marker_point_in_polygon", n_candidates=0, waterbody_key=hit.waterbody_key,
                        gnis_name=index.display_name.get(hit.waterbody_key, ""),
                        source_layer=index.source_layer.get(hit.waterbody_key, ""),
                    ))
                    continue
                results.append(WbidGazetteMatch(
                    r.source, r.source_id, r.identifier, r.name, r.name_variants,
                    r.marker_lat, r.marker_lng, "unmatched",
                ))
                continue

            x, y = _TO_ALBERS.transform(r.marker_lng, r.marker_lat)
            key, dist = _closest(candidates, Point(x, y), index)
            if key is None:
                results.append(WbidGazetteMatch(
                    r.source, r.source_id, r.identifier, r.name, r.name_variants,
                    r.marker_lat, r.marker_lng, "unmatched", n_candidates=len(candidates),
                ))
                continue
            results.append(WbidGazetteMatch(
                r.source, r.source_id, r.identifier, r.name, r.name_variants,
                r.marker_lat, r.marker_lng, "matched",
                method="marker_anchor", n_candidates=len(candidates), waterbody_key=key,
                gnis_name=index.display_name.get(key, ""),
                source_layer=index.source_layer.get(key, ""),
                distance_m=dist,
            ))
            continue

        # stocking/bathymetry — no coordinate of their own at all. Two
        # independent signals, cross-validated against each other rather
        # than either trusted alone (see module docstring).
        name_candidates = _search_candidates(index, r.name, *r.name_variants)
        id_pool = index.group_code_map.get(r.identifier, set()) if r.identifier else set()
        names = [r.name, *r.name_variants]

        key: Optional[str] = None
        method = ""
        overlap = name_candidates & id_pool
        if len(overlap) == 1:
            # Both signals independently agree on the same candidate — the
            # strongest possible confirmation.
            key, method = next(iter(overlap)), "identifier+name"
        elif name_candidates and not id_pool:
            verified = {k for k in name_candidates if _group_code_agrees(k, r.identifier, index)}
            if len(verified) == 1:
                key = next(iter(verified))
                method = "single_hit" if len(name_candidates) == 1 else "watershed_group_code"
        elif id_pool and not name_candidates and len(id_pool) == 1:
            candidate = next(iter(id_pool))
            if _confirmed_by_any_name(index, names, id_pool):
                key, method = candidate, "identifier_join"

        if key is None:
            results.append(WbidGazetteMatch(
                r.source, r.source_id, r.identifier, r.name, r.name_variants,
                None, None, "unmatched", n_candidates=len(name_candidates | id_pool),
            ))
            continue
        results.append(WbidGazetteMatch(
            r.source, r.source_id, r.identifier, r.name, r.name_variants,
            None, None, "matched",
            method=method, n_candidates=len(name_candidates | id_pool), waterbody_key=key,
            gnis_name=index.display_name.get(key, ""),
            source_layer=index.source_layer.get(key, ""),
        ))
    return results


def write_matches(conn: sqlite3.Connection, results: List[WbidGazetteMatch]) -> None:
    conn.execute("DROP TABLE IF EXISTS match_wbid_gazette")
    conn.execute(
        """CREATE TABLE match_wbid_gazette (
            source              TEXT NOT NULL,
            source_id           TEXT NOT NULL,
            identifier          TEXT,
            name                TEXT,
            name_variants       TEXT,
            approx_lat          REAL,
            approx_lng          REAL,
            approx_map_url      TEXT,
            status              TEXT NOT NULL,
            method              TEXT,
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
        """INSERT INTO match_wbid_gazette
           (source, source_id, identifier, name, name_variants, approx_lat, approx_lng, approx_map_url,
            status, method, n_candidates, waterbody_key, gnis_name, source_layer, distance_m, matched_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
            (r.source, r.source_id, r.identifier, r.name, _VARIANT_JOIN.join(r.name_variants),
             r.approx_lat, r.approx_lng, osm_url(r.approx_lat, r.approx_lng),
             r.status, r.method, r.n_candidates, r.waterbody_key, r.gnis_name, r.source_layer,
             r.distance_m, now)
            for r in results
        ],
    )
    conn.commit()


def _print_breakdown(results: List[WbidGazetteMatch]) -> None:
    print("\n" + "=" * 78)
    print("MATCH_WBID (unmatched) -> GAZETTEER SEARCH (marker anchor / watershed group code)")
    print("=" * 78)
    for source in ("stocking", "bathymetry", "map_markers"):
        subset = [r for r in results if r.source == source]
        if not subset:
            continue
        matched = [r for r in subset if r.status == "matched"]
        print(f"\n{source}: {len(matched)}/{len(subset)} matched")

    matched = [r for r in results if r.status == "matched"]
    print(f"\nCOMBINED: {len(matched)}/{len(results)} matched")
    for r in matched:
        label = r.name or (r.name_variants[0] if r.name_variants else "")
        print(f"  {r.source:12} {r.identifier:12} {label:25} [{r.method}] "
              f"-> waterbody_key={r.waterbody_key} ({r.gnis_name or 'unnamed'})")
    unmatched = [r for r in results if r.status == "unmatched"]
    if unmatched:
        print("\nStill unmatched:")
        for r in unmatched:
            label = r.name or (r.name_variants[0] if r.name_variants else "")
            print(f"  {r.source:12} {r.identifier:12} {label}")


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    cfg = ProjectConfig()
    parser = argparse.ArgumentParser(
        description="For match_wbid rows never matched to WDIC at all, try gazetteer "
                    "search disambiguated by marker coordinate or watershed group code.",
    )
    parser.add_argument(
        "--gpkg", type=Path,
        default=cfg.get_path("data", "fetch", "output_gpkg", default="data/bc_fisheries_data.gpkg"),
        help="FWA GeoPackage (lakes/wetlands/manmade layers).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Match and print the breakdown without writing match_wbid_gazette to waterbody_db.db.",
    )
    args = parser.parse_args(argv)

    if not DB_PATH.exists():
        raise FileNotFoundError(f"{DB_PATH} not found — run fetch_all.py then match.py first.")
    if not args.gpkg.exists():
        raise FileNotFoundError(f"FWA GeoPackage not found at {args.gpkg}")

    conn = sqlite3.connect(DB_PATH)
    try:
        rows = load_unmatched(conn)
        if not rows:
            logger.info("No match_wbid rows are unmatched — nothing for this stage to do.")

        logger.info("Building FWA lake index from %s ...", args.gpkg)
        index = wm.build_lake_index(args.gpkg)

        results = match_wbid_gazette(rows, index)

        if args.dry_run:
            logger.info("--dry-run: not writing match_wbid_gazette to waterbody_db.db")
        else:
            write_matches(conn, results)
    finally:
        conn.close()

    _print_breakdown(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
