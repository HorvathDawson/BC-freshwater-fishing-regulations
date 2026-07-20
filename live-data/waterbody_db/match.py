#!/usr/bin/env python3
"""
match.py — match FIDQ stocking + WSA bathymetry + gofishbc marker records
directly to WHSE_FISH.WDIC_WATERBODY_POLY_SVW, BC's own 1:50,000-scale
waterbody reference layer — not FWA's lakes/wetlands/manmade layers.

Why WDIC, not FWA
------------------
Both FIDQ's wbid and the WSA bathymetry catalogue's
WATERBODY_IDENTIFIER_WSA_50K use the exact same scheme:
zfill(WATERBODY_KEY_50K, 5) + WATERSHED_GROUP_CODE_50K. FWA's own
WATERBODY_KEY_GROUP_CODE_50K column carries the *same string*, but it's a
label FWA's own polygon layers happen to also carry — WDIC is the actual
1:50K reference dataset that identifier scheme was built against.

../stocking/ and ../bathymetry/'s own matchers (T1-T5 cascades in
live-data/common/waterbody_matcher.py) join against FWA's group-code column
first, falling back to WDIC only for what that join misses. That produced a
real, recurring failure mode this session kept re-discovering in different
shapes: FWA's own generalized polygons can drift from what an identifier
actually means (confirmed live: PEAR LAKE's FWA WATERSHED_GROUP_CODE
changed from "OKAN" to "KETL" sometime after FIDQ's snapshot), and layering
more automated signals on top (WDIC-as-fallback, survey-polygon overlap,
name search, curated overrides, geographic plausibility gates) to catch
that drift kept surfacing *new* disagreements rather than settling them —
including one case (bathymetry survey 00765PARK) where the "independent"
survey-polygon geometry turned out to likely be linked via the same
underlying (wrong) identifier as the catalogue record itself, so it wasn't
independent evidence at all.

This module is a deliberate reset: match directly against WDIC — the
identifier scheme's own source of truth — first, with nothing else layered
on top yet. No FWA join, no name fallback, no overrides. Just: does this
identifier exist in WDIC at all, and if so, what's its polygon's
approximate location? That answers the actual open question before any
more matching logic gets built on top of it: how much of both datasets
resolves cleanly via the identifier alone against its own reference layer.

Scope (deliberately narrow for this first pass)
-------------------------------------------------
  * Identifier-only. No gazetteer name search, no FWA fallback, no curated
    overrides — those all assume WDIC coverage gaps that haven't actually
    been measured yet.
  * Reads already-fetched data from this directory's shared
    `waterbody_db.db` — `fidq_stocking_records`, `map_markers` (both
    fetch_stocking.py), and `bathy_surveys` (fetch_bathymetry.py) — rather
    than re-fetching from FIDQ/WSA/gofishbc. `map_markers` matches on its
    own `wbid` column directly (gofishbc's marker data, not FIDQ's stocking
    history — a marker can exist with no stocking record at all).
  * Every matched row also carries every other name variant its own source
    has, alongside the primary `name`/`wdic_gazetted_name` pair: FIDQ's own
    `aliases` for stocking (itself a comma-separated list of alternate names
    — confirmed live, e.g. "STIRLING ARM, TAYLOR ARM, TWO RIVERS ARM" — split
    into individual names here, not carried through as one opaque blob, since
    match_fwa_gazette.py's gazetteer search needs each name searched
    separately), `MAP_TITLE` for bathymetry (gofishbc markers have no
    separate name-variant field — `title` already fills that role as `name`).
    Stored as `name_variants`, a "; "-joined list — carried through for
    match_fwa_gazette.py's own second-pass search, and for display/review;
    not matched on by this module itself.
  * For every identifier WDIC has a record for, stores the matched
    WDIC_WATERBODY_POLY_ID/WBODY_ID/GAZETTED_NAME plus an *approximate*
    lat/lng (the polygon's own representative_point(), reprojected from
    WDIC's native EPSG:3005 to WGS84) — a coordinate any downstream step
    (point-in-polygon against FWA, a map pin, etc.) can use later, without
    this module itself making any FWA-matching decision.

One db, three fetchers
------------------------
This directory is a testbed: `fetch_stocking.py`/`fetch_bathymetry.py` here
are replicated copies of `../stocking/`'s and `../bathymetry/`'s own fetch
scripts, and `fetch_wdic.py` fetches the *entire* WDIC layer (self-contained
— no dependency on the other two, see its own docstring) — all three write
into this single `waterbody_db.db` instead of three separate per-feed DBs.
This module reads all of it from that one file/connection: no cross-directory
DB paths, unlike the original `wbid50k/` version this folder replaced.

CLI
---
    cd live-data/waterbody_db
    python fetch_all.py                     # run all three fetchers into waterbody_db.db
    python match.py                          # match all, write to waterbody_db.db
    python match.py --dry-run                # match all, print only, no DB writes
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fetch_wdic import DB_PATH

logger = logging.getLogger(__name__)

# Delimiter joining name_variants for storage in match_wbid (a plain TEXT
# column — sqlite has no array type). Distinct from any delimiter a source
# field itself uses (FIDQ's own `aliases` is comma-separated — see
# _split_variants()), so splitting back out on load is unambiguous.
_VARIANT_JOIN = "; "


def osm_url(lat: Optional[float], lng: Optional[float]) -> str:
    """OpenStreetMap link centered on (lat, lng) with a marker dropped at the
    exact point — "" when there's no coordinate to link to (a row never
    matched to WDIC has no approx_lat/approx_lng at all). Shared by every
    matching-table writer in this directory (match_fwa_gazette.py and
    match_fwa_identifier.py both import this rather than reimplementing it),
    since all three tables carry the same WDIC-derived anchor point through
    unchanged — see each module's own docstring."""
    if lat is None or lng is None:
        return ""
    return f"https://www.openstreetmap.org/?mlat={lat}&mlon={lng}#map=15/{lat}/{lng}"


def _split_variants(raw: str, *, exclude: str = "") -> List[str]:
    """FIDQ's own `aliases` field is a comma-separated list of alternate
    names (confirmed live, e.g. "STIRLING ARM, TAYLOR ARM, TWO RIVERS ARM"),
    not one name — splitting here is what makes each individual name
    searchable by match_fwa_gazette.py's gazetteer lookup later; passing the
    whole blob through as a single search term would never match anything
    (the gazetteer index is keyed by single title-cased names). Dedupes and
    drops anything identical to ``exclude`` (the row's own primary name —
    redundant to carry twice)."""
    exclude_norm = exclude.strip().upper()
    seen: List[str] = []
    for part in raw.split(","):
        part = part.strip()
        if not part or part.upper() == exclude_norm or part in seen:
            continue
        seen.append(part)
    return seen


@dataclass
class SourceRecord:
    source: str          # "stocking" | "bathymetry" | "map_markers"
    source_id: str        # waterbody_id (stocking), identifier (bathymetry), or marker_id (markers) — unique within its source
    identifier: str        # the 50K wbid / WATERBODY_IDENTIFIER_WSA_50K value
    name: str              # gazetted name / marker title, carried through for display/comparison only — not matched on
    name_variants: List[str] = field(default_factory=list)  # every other known name for this waterbody (FIDQ aliases, bathymetry MAP_TITLE) — display + match_fwa_gazette.py input, not matched on here


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def load_stocking_records(conn: sqlite3.Connection) -> List[SourceRecord]:
    if not _table_exists(conn, "fidq_stocking_records"):
        logger.warning("fidq_stocking_records not in %s — run fetch_stocking.py first, skipping.", DB_PATH)
        return []
    rows = conn.execute(
        """SELECT DISTINCT waterbody_id, waterbody_identifier, gazetted_name, aliases
           FROM fidq_stocking_records"""
    ).fetchall()
    seen: Dict[str, SourceRecord] = {}
    for wid, wbid, name, aliases in rows:
        wid = str(wid)
        wbid = (wbid or "").strip()
        if not wbid or wid in seen:
            continue
        name = (name or "").strip()
        seen[wid] = SourceRecord("stocking", wid, wbid, name, _split_variants(aliases or "", exclude=name))
    return list(seen.values())


def load_bathymetry_records(conn: sqlite3.Connection) -> List[SourceRecord]:
    if not _table_exists(conn, "bathy_surveys"):
        logger.warning("bathy_surveys not in %s — run fetch_bathymetry.py first, skipping.", DB_PATH)
        return []
    rows = conn.execute(
        """SELECT identifier, gazetted_name, map_title FROM bathy_surveys WHERE identifier != ''"""
    ).fetchall()
    seen: Dict[str, SourceRecord] = {}
    for ident, name, map_title in rows:
        ident = (ident or "").strip()
        if not ident or ident in seen:
            continue
        name = (name or "").strip()
        map_title = (map_title or "").strip()
        variants = [map_title] if map_title and map_title.upper() != name.upper() else []
        seen[ident] = SourceRecord("bathymetry", ident, ident, name, variants)
    return list(seen.values())


def load_marker_records(conn: sqlite3.Connection) -> List[SourceRecord]:
    if not _table_exists(conn, "map_markers"):
        logger.warning("map_markers not in %s — run fetch_stocking.py first, skipping.", DB_PATH)
        return []
    rows = conn.execute(
        """SELECT marker_id, wbid, title FROM map_markers WHERE wbid IS NOT NULL AND wbid != ''"""
    ).fetchall()
    seen: Dict[str, SourceRecord] = {}
    for marker_id, wbid, title in rows:
        marker_id = str(marker_id)
        wbid = (wbid or "").strip()
        if not wbid or marker_id in seen:
            continue
        # No separate name-variant field for markers — `title` already fills
        # that role as `name`.
        seen[marker_id] = SourceRecord("map_markers", marker_id, wbid, (title or "").strip())
    return list(seen.values())


def load_wdic_cache(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    rows = conn.execute(
        "SELECT identifier, poly_id, wbody_id, gazetted_name, approx_lat, approx_lng FROM wdic_cache"
    ).fetchall()
    return {
        ident: {"poly_id": poly_id, "wbody_id": wbody_id, "gazetted_name": gazetted_name,
                "approx_lat": lat, "approx_lng": lng}
        for ident, poly_id, wbody_id, gazetted_name, lat, lng in rows
    }


@dataclass
class WdicMatch:
    source: str
    source_id: str
    identifier: str
    name: str
    name_variants: List[str]
    status: str                       # matched | unmatched
    wdic_poly_id: str = ""
    wdic_wbody_id: str = ""
    wdic_gazetted_name: str = ""
    approx_lat: Optional[float] = None
    approx_lng: Optional[float] = None


def match_records(records: List[SourceRecord], wdic_cache: Dict[str, Dict[str, Any]]) -> List[WdicMatch]:
    results: List[WdicMatch] = []
    for r in records:
        hit = wdic_cache.get(r.identifier)
        if hit is None:
            results.append(WdicMatch(r.source, r.source_id, r.identifier, r.name, r.name_variants, "unmatched"))
            continue
        results.append(WdicMatch(
            r.source, r.source_id, r.identifier, r.name, r.name_variants, "matched",
            wdic_poly_id=hit["poly_id"], wdic_wbody_id=hit["wbody_id"],
            wdic_gazetted_name=hit["gazetted_name"], approx_lat=hit["approx_lat"], approx_lng=hit["approx_lng"],
        ))
    return results


def write_matches(conn: sqlite3.Connection, results: List[WdicMatch]) -> None:
    conn.execute("DROP TABLE IF EXISTS match_wbid")
    conn.execute(
        """CREATE TABLE match_wbid (
            source              TEXT NOT NULL,
            source_id           TEXT NOT NULL,
            identifier          TEXT,
            name                TEXT,
            name_variants       TEXT,
            status              TEXT NOT NULL,
            wdic_poly_id        TEXT,
            wdic_wbody_id       TEXT,
            wdic_gazetted_name  TEXT,
            approx_lat          REAL,
            approx_lng          REAL,
            approx_map_url      TEXT,
            matched_at          TEXT,
            PRIMARY KEY (source, source_id)
        )"""
    )
    now = datetime.now(timezone.utc).isoformat()
    conn.executemany(
        """INSERT INTO match_wbid
           (source, source_id, identifier, name, name_variants, status, wdic_poly_id,
            wdic_wbody_id, wdic_gazetted_name, approx_lat, approx_lng, approx_map_url, matched_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
            (r.source, r.source_id, r.identifier, r.name, _VARIANT_JOIN.join(r.name_variants),
             r.status, r.wdic_poly_id, r.wdic_wbody_id, r.wdic_gazetted_name,
             r.approx_lat, r.approx_lng, osm_url(r.approx_lat, r.approx_lng), now)
            for r in results
        ],
    )
    conn.commit()


def _print_breakdown(results: List[WdicMatch]) -> None:
    print("\n" + "=" * 78)
    print("WBID -> WDIC 1:50K MATCH (identifier only, no FWA/name/override yet)")
    print("=" * 78)
    for source in ("stocking", "bathymetry", "map_markers"):
        subset = [r for r in results if r.source == source]
        if not subset:
            continue
        matched = [r for r in subset if r.status == "matched"]
        total = len(subset)
        pct = f"{len(matched) / total:.1%}" if total else "—"
        print(f"\n{source}: {len(matched)}/{total} matched ({pct})")

    total = len(results)
    matched = [r for r in results if r.status == "matched"]
    print(f"\nCOMBINED: {len(matched)}/{total} matched ({len(matched)/total:.1%})" if total else "")

    # Name sanity check (display only, not used to decide anything): of the
    # matched rows with both a source name and a WDIC name, how many agree?
    both_named = [r for r in matched if r.name.strip() and r.wdic_gazetted_name.strip()]
    if both_named:
        def _norm(s: str) -> str:
            return s.strip().upper()
        agree = sum(1 for r in both_named if _norm(r.name) == _norm(r.wdic_gazetted_name))
        print(f"  of {len(both_named)} matched row(s) with a name on both sides: "
              f"{agree} ({agree/len(both_named):.1%}) agree exactly")


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(
        description="Match FIDQ stocking + WSA bathymetry + gofishbc marker records directly to WDIC's 1:50K reference layer.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Match and print the breakdown without writing match_wbid to waterbody_db.db.",
    )
    args = parser.parse_args(argv)

    if not DB_PATH.exists():
        raise FileNotFoundError(f"{DB_PATH} not found — run fetch_all.py (or fetch_wdic.py) first.")

    conn = sqlite3.connect(DB_PATH)
    try:
        wdic_cache = load_wdic_cache(conn)
        if not wdic_cache:
            raise RuntimeError(f"{DB_PATH} has no wdic_cache rows — run fetch_wdic.py first.")

        stocking_records = load_stocking_records(conn)
        bathy_records = load_bathymetry_records(conn)
        marker_records = load_marker_records(conn)
        logger.info("%d stocking record(s), %d bathymetry record(s), %d marker record(s) with a usable identifier.",
                    len(stocking_records), len(bathy_records), len(marker_records))

        results = match_records(stocking_records + bathy_records + marker_records, wdic_cache)

        if args.dry_run:
            logger.info("--dry-run: not writing match_wbid to waterbody_db.db")
        else:
            write_matches(conn, results)
    finally:
        conn.close()

    _print_breakdown(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
