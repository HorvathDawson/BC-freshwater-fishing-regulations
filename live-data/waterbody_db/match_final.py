#!/usr/bin/env python3
"""
match_final.py — the aggregated result of every other matching table in
this directory: one row per (source, source_id), the single answer this
whole pipeline landed on for it, and which stage produced that answer.

The one exception to "read only the prior stage"
------------------------------------------------------
Every other script in this directory reads only its immediately preceding
stage's own output (see each module's own docstring). This one is
deliberately the exception — its entire job is aggregation, so it reads
`match_wbid` plus all four downstream tables
(`match_fwa_gazette`/`match_fwa_identifier`/`match_fwa_override`/
`match_wbid_gazette`) and picks the first one with a real answer, in
pipeline order.

Two disjoint tracks, by match_wbid.status
------------------------------------------------
A `match_wbid` row is either `status = 'matched'` (WDIC itself resolved the
identifier — see match.py) or `'unmatched'` (it never did). Only one of the
two downstream chains ever applies to a given row, never both:
  - `matched`  -> match_fwa_gazette -> match_fwa_identifier -> match_fwa_override
  - `unmatched` -> match_wbid_gazette
The first stage in the relevant chain with `status = 'matched'` wins;
`resolved_by` records which one. A `match_fwa_override` row with
`status = 'ignored'` (curated + confirmed no current FWA polygon at all —
see wbid_overrides.json) becomes `status = 'no_polygon'` here, kept
distinct from a row nothing has resolved yet at all (`'unmatched'`).

Coordinate: whichever stage actually has one
--------------------------------------------------
`match_wbid`'s own `approx_lat`/`approx_lng` (WDIC's anchor) is preferred
when present; for the `match_wbid_gazette` track (WDIC never matched at
all), the marker's own coordinate carried on that table is used instead —
same "propagate whichever coordinate exists" approach every table in this
chain already follows (see match.py's own `osm_url()`).

CLI
---
    cd live-data/waterbody_db
    python match_final.py                 # run after every other stage; writes match_final
    python match_final.py --dry-run        # print only, no DB writes
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from fetch_wdic import DB_PATH
from match import osm_url

logger = logging.getLogger(__name__)


@dataclass
class FinalRow:
    source: str
    source_id: str
    identifier: str
    name: str
    name_variants: str          # already "; "-joined by the source table — passed through verbatim, not re-parsed
    approx_lat: Optional[float]
    approx_lng: Optional[float]
    status: str                # matched | no_polygon | unmatched
    resolved_by: str = ""        # match_fwa_gazette | match_fwa_identifier | match_fwa_override | match_wbid_gazette | ""
    method: str = ""             # that stage's own tier/method column, carried through verbatim
    waterbody_key: str = ""
    gnis_name: str = ""
    source_layer: str = ""
    note: str = ""                # match_fwa_override's own curator note, when that's the resolving stage


def _load_table(conn: sqlite3.Connection, table: str, columns: List[str]) -> Dict[Tuple[str, str], dict]:
    """(source, source_id) -> row dict, for whichever of ``columns`` the
    table actually has (each downstream table's schema differs slightly —
    see each module's own write_matches())."""
    cols = ", ".join(columns)
    rows = conn.execute(f"SELECT source, source_id, {cols} FROM {table}").fetchall()
    return {
        (r[0], r[1]): dict(zip(["source", "source_id", *columns], r))
        for r in rows
    }


def build_final_rows(conn: sqlite3.Connection) -> List[FinalRow]:
    wbid = _load_table(conn, "match_wbid", [
        "identifier", "name", "name_variants", "status", "approx_lat", "approx_lng",
    ])
    gazette = _load_table(conn, "match_fwa_gazette", [
        "status", "tier", "waterbody_key", "gnis_name", "source_layer",
    ])
    identifier = _load_table(conn, "match_fwa_identifier", [
        "status", "waterbody_key", "gnis_name", "source_layer",
    ])
    override = _load_table(conn, "match_fwa_override", [
        "status", "waterbody_key", "gnis_name", "source_layer", "note",
    ])
    wbid_gazette = _load_table(conn, "match_wbid_gazette", [
        "status", "method", "waterbody_key", "gnis_name", "source_layer", "approx_lat", "approx_lng",
    ])

    results: List[FinalRow] = []
    for key, w in wbid.items():
        base = FinalRow(
            source=w["source"], source_id=w["source_id"], identifier=w["identifier"],
            name=w["name"], name_variants=w["name_variants"],
            approx_lat=w["approx_lat"], approx_lng=w["approx_lng"], status="unmatched",
        )

        if w["status"] == "matched":
            g = gazette.get(key)
            if g and g["status"] == "matched":
                base.status, base.resolved_by, base.method = "matched", "match_fwa_gazette", g["tier"]
                base.waterbody_key, base.gnis_name, base.source_layer = g["waterbody_key"], g["gnis_name"], g["source_layer"]
                results.append(base)
                continue

            i = identifier.get(key)
            if i and i["status"] == "matched":
                base.status, base.resolved_by = "matched", "match_fwa_identifier"
                base.waterbody_key, base.gnis_name, base.source_layer = i["waterbody_key"], i["gnis_name"], i["source_layer"]
                results.append(base)
                continue

            o = override.get(key)
            if o and o["status"] == "matched":
                base.status, base.resolved_by = "matched", "match_fwa_override"
                base.waterbody_key, base.gnis_name, base.source_layer = o["waterbody_key"], o["gnis_name"], o["source_layer"]
                base.note = o["note"]
                results.append(base)
                continue
            if o and o["status"] == "ignored":
                base.status, base.resolved_by, base.note = "no_polygon", "match_fwa_override", o["note"]
                results.append(base)
                continue

            results.append(base)  # nothing resolved it — stays "unmatched"
            continue

        # match_wbid itself never matched WDIC at all — the wbid_gazette track.
        wg = wbid_gazette.get(key)
        if wg and wg["status"] == "matched":
            base.status, base.resolved_by, base.method = "matched", "match_wbid_gazette", wg["method"]
            base.waterbody_key, base.gnis_name, base.source_layer = wg["waterbody_key"], wg["gnis_name"], wg["source_layer"]
            # match_wbid has no coordinate for this row (WDIC never matched
            # it) — wbid_gazette's own (the marker's own point, when there
            # is one) is the only coordinate that exists for it at all.
            if wg["approx_lat"] is not None:
                base.approx_lat, base.approx_lng = wg["approx_lat"], wg["approx_lng"]
            results.append(base)
            continue

        results.append(base)  # stays "unmatched"
    return results


def write_matches(conn: sqlite3.Connection, results: List[FinalRow]) -> None:
    conn.execute("DROP TABLE IF EXISTS match_final")
    conn.execute(
        """CREATE TABLE match_final (
            source              TEXT NOT NULL,
            source_id           TEXT NOT NULL,
            identifier          TEXT,
            name                TEXT,
            name_variants       TEXT,
            approx_lat          REAL,
            approx_lng          REAL,
            approx_map_url      TEXT,
            status              TEXT NOT NULL,
            resolved_by         TEXT,
            method              TEXT,
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
        """INSERT INTO match_final
           (source, source_id, identifier, name, name_variants, approx_lat, approx_lng, approx_map_url,
            status, resolved_by, method, waterbody_key, gnis_name, source_layer, note, matched_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
            (r.source, r.source_id, r.identifier, r.name, r.name_variants,
             r.approx_lat, r.approx_lng, osm_url(r.approx_lat, r.approx_lng),
             r.status, r.resolved_by, r.method, r.waterbody_key, r.gnis_name, r.source_layer, r.note, now)
            for r in results
        ],
    )
    conn.commit()


def _print_breakdown(results: List[FinalRow]) -> None:
    print("\n" + "=" * 78)
    print("MATCH_FINAL — aggregated result across every matching table")
    print("=" * 78)
    for source in ("stocking", "bathymetry", "map_markers"):
        subset = [r for r in results if r.source == source]
        if not subset:
            continue
        matched = [r for r in subset if r.status == "matched"]
        no_poly = [r for r in subset if r.status == "no_polygon"]
        unmatched = [r for r in subset if r.status == "unmatched"]
        print(f"\n{source}: {len(matched)} matched, {len(no_poly)} no_polygon, "
              f"{len(unmatched)} unmatched (of {len(subset)})")

    total = len(results)
    matched = [r for r in results if r.status == "matched"]
    no_poly = [r for r in results if r.status == "no_polygon"]
    unmatched = [r for r in results if r.status == "unmatched"]
    print(f"\nCOMBINED: {len(matched)} matched, {len(no_poly)} no_polygon, "
          f"{len(unmatched)} unmatched (of {total})")
    print(f"  {(len(matched) + len(no_poly)) / total:.1%} accounted for "
          f"(matched or confirmed no-polygon)" if total else "")

    by_stage: Dict[str, int] = {}
    for r in matched:
        by_stage[r.resolved_by] = by_stage.get(r.resolved_by, 0) + 1
    if by_stage:
        print("\n  matched, by resolving stage:")
        for stage, count in sorted(by_stage.items(), key=lambda kv: -kv[1]):
            print(f"    {stage:<22} {count}")

    if unmatched:
        print("\nStill unmatched (no table resolved these at all):")
        for r in unmatched:
            label = r.name or (r.name_variants.split("; ")[0] if r.name_variants else "")
            print(f"  {r.source:12} {r.identifier:12} {label}")


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(
        description="Aggregate every matching table in this directory into one final result per row.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Match and print the breakdown without writing match_final to waterbody_db.db.",
    )
    args = parser.parse_args(argv)

    if not DB_PATH.exists():
        raise FileNotFoundError(f"{DB_PATH} not found — run the rest of the pipeline first.")

    conn = sqlite3.connect(DB_PATH)
    try:
        missing = [
            t for t in ("match_wbid", "match_fwa_gazette", "match_fwa_identifier",
                        "match_fwa_override", "match_wbid_gazette")
            if not conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)
            ).fetchone()
        ]
        if missing:
            raise FileNotFoundError(
                f"{DB_PATH} is missing table(s) {missing} — run the full pipeline "
                "(match.py, match_fwa_gazette.py, match_fwa_identifier.py, "
                "match_fwa_override.py, match_wbid_gazette.py) first."
            )

        results = build_final_rows(conn)

        if args.dry_run:
            logger.info("--dry-run: not writing match_final to waterbody_db.db")
        else:
            write_matches(conn, results)
    finally:
        conn.close()

    _print_breakdown(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
