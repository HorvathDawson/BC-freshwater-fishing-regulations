#!/usr/bin/env python3
"""
export.py — precompute this package's match results into a single static
artifact the main regulations build reads directly, replacing
pipeline/matching/bathymetry_matcher.py's live re-matching at build time.

Reads match_final (this package's own aggregated result — see match_final.py)
plus two raw tables it doesn't itself carry (bathy_surveys for per-sheet PDF
filenames, map_markers for amenity/access fields), and writes one combined
JSON:

    data/bathymetry_pdfs/waterbody_matches.json
    {
      "generated_at": "...",
      "wbk_bathymetry": {"<wbk>": [{"pdf", "title", "name"}, ...]},
      "wbk_names":      {"<wbk>": [{"name", "source": "bathymetry"|"stocking"}, ...]},
      "wbk_marker":      {"<wbk>": {<10-field amenity whitelist>}}
    }

match_final's own schema is NOT widened for this — pdf filenames and marker
amenity fields are payload, not matching results, and match_final is
one-row-per-(source, source_id) while bathymetry pdfs are inherently
one-to-many (one WSA identifier can span several map sheets). This script is
the join layer, run separately from (and after) the match chain.

Regeneration is manual/occasional — the same cadence as refreshing
data/wsa_bathymetry_maps.csv itself (whenever the fetch+match chain above is
re-run), not tied to the regulations build. The output is committed, and
pipeline/enrichment/waterbody_accessor.py just reads it — no live matching,
no gpkg dependency, at build time.

CLI
---
    python -m pipeline.recurring.waterbody_db.export
    python -m pipeline.recurring.waterbody_db.export --out /path/to/waterbody_matches.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set

from .fetch_wdic import DB_PATH

logger = logging.getLogger(__name__)

DEFAULT_OUT_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "bathymetry_pdfs" / "waterbody_matches.json"
)

# Matches match.py's own _VARIANT_JOIN exactly — match_final.name_variants is
# passed straight through from match_wbid, joined the same way there.
_VARIANT_JOIN = "; "

# Map-marker amenity/access fields attached to a reach — see
# pipeline/enrichment/reach_builder.py's reaches[rid]["marker"]. Deliberately
# excludes nearest_town/surface_area/elevation/gpx_file/stocking_report_url/
# region/lat/lng/title (not part of this whitelist).
MARKER_FIELDS = (
    "more_info", "description", "photo_url", "road_access", "boat_launch",
    "fishing_dock", "campsite", "washroom", "wheelchair_access", "hike_in_required",
)


def _split_keys(waterbody_key: str) -> List[str]:
    """A tied match_final row's waterbody_key is comma-joined (multi-part
    waterbody) — split it the same way every downstream consumer of this
    column already does."""
    return [k for k in (waterbody_key or "").split(",") if k]


def _split_variants(name_variants: str) -> List[str]:
    return [v for v in (name_variants or "").split(_VARIANT_JOIN) if v]


def build_wbk_bathymetry(conn: sqlite3.Connection) -> Dict[str, List[Dict[str, str]]]:
    """<wbk> -> [{"pdf", "title", "name"}, ...] for every matched bathymetry
    survey, fanned out to every map sheet (bathy_surveys.pdf_filename) that
    identifier covers — mirrors the old
    bathymetry_matcher.build_wbk_bathymetry_map()'s fan-out exactly."""
    rows = conn.execute(
        "SELECT source_id, waterbody_key FROM match_final "
        "WHERE source = 'bathymetry' AND status = 'matched'"
    ).fetchall()

    out: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    seen_by_wbk: Dict[str, Set[str]] = defaultdict(set)
    for identifier, waterbody_key in rows:
        sheets = conn.execute(
            "SELECT pdf_filename, map_title, gazetted_name FROM bathy_surveys "
            "WHERE identifier = ?",
            (identifier,),
        ).fetchall()
        for wbk in _split_keys(waterbody_key):
            for pdf_filename, map_title, gazetted_name in sheets:
                pdf = (pdf_filename or "").strip().lower()
                if not pdf or pdf in seen_by_wbk[wbk]:
                    continue
                seen_by_wbk[wbk].add(pdf)
                title = (map_title or "").strip()
                name = (gazetted_name or "").strip()
                out[wbk].append({"pdf": pdf, "title": title or name, "name": name or title})
    return dict(out)


def build_wbk_names(conn: sqlite3.Connection) -> Dict[str, List[Dict[str, str]]]:
    """<wbk> -> [{"name", "source"}, ...] — every name (primary + all
    name_variants) match_final carries for a matched stocking or bathymetry
    row, not just one representative name per source."""
    rows = conn.execute(
        "SELECT source, waterbody_key, name, name_variants FROM match_final "
        "WHERE source IN ('stocking', 'bathymetry') AND status = 'matched'"
    ).fetchall()

    out: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    seen_by_wbk: Dict[str, Set[str]] = defaultdict(set)
    for source, waterbody_key, name, name_variants in rows:
        names = {n.strip() for n in [name, *_split_variants(name_variants)] if n and n.strip()}
        for wbk in _split_keys(waterbody_key):
            for n in sorted(names):
                key = (n, source)
                if key in seen_by_wbk[wbk]:
                    continue
                seen_by_wbk[wbk].add(key)
                out[wbk].append({"name": n, "source": source})
    return dict(out)


def build_wbk_marker(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    """<wbk> -> {whitelisted amenity fields} for every matched gofishbc
    marker. A tied waterbody_key gets the same marker dict written under
    each resulting wbk (a lake only has one gofishbc marker in practice)."""
    rows = conn.execute(
        "SELECT source_id, waterbody_key FROM match_final "
        "WHERE source = 'map_markers' AND status = 'matched'"
    ).fetchall()

    cols = ", ".join(MARKER_FIELDS)
    out: Dict[str, Dict[str, Any]] = {}
    for marker_id, waterbody_key in rows:
        row = conn.execute(
            f"SELECT {cols} FROM map_markers WHERE marker_id = ?", (marker_id,)
        ).fetchone()
        if row is None:
            continue
        marker = {field: value for field, value in zip(MARKER_FIELDS, row) if value not in (None, "")}
        if not marker:
            continue
        for wbk in _split_keys(waterbody_key):
            out[wbk] = marker
    return out


def build_export(db_path: Path = DB_PATH) -> Dict[str, Any]:
    if not db_path.exists():
        raise FileNotFoundError(
            f"{db_path} not found — run the fetch+match chain first "
            "(see this package's README's CLI section)."
        )
    conn = sqlite3.connect(db_path)
    try:
        missing = [
            t for t in ("match_final", "bathy_surveys", "map_markers")
            if not conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)
            ).fetchone()
        ]
        if missing:
            raise FileNotFoundError(
                f"{db_path} is missing table(s) {missing} — run the full fetch+match "
                "chain first (fetch_all, match, match_fwa_gazette, match_fwa_identifier, "
                "match_fwa_override, match_wbid_gazette, match_final)."
            )
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "wbk_bathymetry": build_wbk_bathymetry(conn),
            "wbk_names": build_wbk_names(conn),
            "wbk_marker": build_wbk_marker(conn),
        }
    finally:
        conn.close()


def main(argv: List[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(
        description="Precompute waterbody_db's match results into a static build-time artifact.",
    )
    parser.add_argument("--db", type=Path, default=DB_PATH, help="waterbody_db.db path.")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT_PATH, help="Output JSON path.")
    args = parser.parse_args(argv)

    export = build_export(args.db)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False, indent=2, sort_keys=True)

    logger.info(
        "Wrote %s — %d wbk(s) with bathymetry, %d wbk(s) with names, %d wbk(s) with marker info -> %s",
        "waterbody_matches.json",
        len(export["wbk_bathymetry"]),
        len(export["wbk_names"]),
        len(export["wbk_marker"]),
        args.out,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
