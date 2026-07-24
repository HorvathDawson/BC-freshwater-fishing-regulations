"""
stocking_resolver — resolve FIDQ stocking records to reach IDs.

Modeled on pipeline/recurring/in_season_resolver.py, but simpler: in-season
notices only ever carry a water *name*, so that resolver has to go through
MatchTable + tier0's search index to find a reach. Stocking rows already
carry a resolved waterbody_key (pipeline/recurring/anglerinfo's own
match_final table), so this just needs waterbody_key -> reach_id — a flat
lookup against poly_reaches.json (written by pipeline/enrichment/builder.py
right after build_regulation_index() runs), no name matching at all.

No separate "scraper" here — pipeline/recurring/anglerinfo's own
fetch_stocking.py + match chain already is the fetch+resolve step. This
module's whole job is reading what's already matched: match_final
(source='stocking') for the waterbody_key, fidq_stocking_records for the
actual release history.

Produces stocking.json — structurally analogous to in_season.json, but not
yet consumed by any frontend display (that's future work once stocking info
display is built). Kept fresh by an inline call from builder.py's main build
step, same non-fatal pattern as in-season.

CLI
---
    python -m pipeline.recurring.stocking_resolver
    python -m pipeline.recurring.stocking_resolver --db path/to/anglerinfo.db --poly-reaches path/to/poly_reaches.json --out path/to/stocking.json
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
from typing import Any, Dict, List, Optional

from project_config import ProjectConfig
# Import DB_PATH from the lightweight paths module, not fetch_wdic — the latter
# top-level-imports geopandas/pyproj, which the stocking cron doesn't install.
from pipeline.recurring.anglerinfo.paths import DB_PATH as WATERBODY_DB_PATH

logger = logging.getLogger(__name__)

# Fields surfaced per release row — a useful subset of fidq_stocking_records'
# full schema (see pipeline/recurring/anglerinfo/README.md), not every column.
_RELEASE_FIELDS = (
    "release_date", "species_name", "brood_year", "strain_name",
    "source_name", "origin", "life_stage", "released_quantity", "average_weight",
)


def _split_keys(waterbody_key: str) -> List[str]:
    return [k for k in (waterbody_key or "").split(",") if k]


def _load_poly_reaches(path: Path) -> Dict[str, str]:
    if not path.exists():
        logger.warning(
            "poly_reaches.json not found at %s — stocking rows will resolve "
            "with no reach_id. Run a full build first (it's written by "
            "pipeline/enrichment/builder.py).",
            path,
        )
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def resolve_stocking(
    db_path: Path = WATERBODY_DB_PATH,
    poly_reaches_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Read match_final (source='stocking') + fidq_stocking_records, resolve
    each row's waterbody_key(s) to a reach_id via poly_reaches, and return
    the stocking.json structure."""
    if not db_path.exists():
        raise FileNotFoundError(
            f"{db_path} not found — run pipeline.recurring.anglerinfo's "
            "fetch+match chain first."
        )
    if poly_reaches_path is None:
        cfg = ProjectConfig()
        poly_reaches_path = cfg.get_path(
            "output", "pipeline", "deploy", default="output/pipeline/deploy"
        ) / "poly_reaches.json"
    poly_reaches = _load_poly_reaches(poly_reaches_path)

    conn = sqlite3.connect(db_path)
    try:
        if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='match_final'"
        ).fetchone():
            raise FileNotFoundError(
                f"{db_path} has no match_final table — run the full match chain "
                "(match, match_fwa_gazette, match_fwa_identifier, match_fwa_override, "
                "match_wbid_gazette, match_final) first."
            )

        rows = conn.execute(
            "SELECT source_id, name, waterbody_key, status FROM match_final "
            "WHERE source = 'stocking'"
        ).fetchall()

        release_cols = ", ".join(_RELEASE_FIELDS)
        waterbodies: List[Dict[str, Any]] = []
        resolved_to_reach = 0
        for waterbody_id, name, waterbody_key, status in rows:
            reach_id = None
            if status == "matched":
                for wbk in _split_keys(waterbody_key):
                    reach_id = poly_reaches.get(wbk)
                    if reach_id:
                        break

            releases = [
                dict(zip(_RELEASE_FIELDS, r))
                for r in conn.execute(
                    f"SELECT {release_cols} FROM fidq_stocking_records WHERE waterbody_id = ?",
                    (waterbody_id,),
                ).fetchall()
            ]

            if reach_id:
                resolved_to_reach += 1
            waterbodies.append({
                "waterbody_key": waterbody_key or None,
                "reach_id": reach_id,
                "fidq_name": name,
                "releases": releases,
            })
    finally:
        conn.close()

    total = len(waterbodies)
    from pipeline.recurring.provenance import provenance

    return {
        **provenance(
            generator="stocking_resolver",
            source="FIDQ (BC gov Fish Inventories Data Queries)",
            source_url="https://a100.gov.bc.ca/pub/fidq/main.do",
            attribution=(
                "Fish stocking data sourced from the Province of British Columbia "
                "(Fish Inventories Data Queries) and used under the Province's "
                "copyright terms (https://www2.gov.bc.ca/gov/content/home/copyright)."
            ),
        ),
        # legacy top-level keys kept for existing consumers
        "source": "FIDQ (BC gov Fish Inventories Data Queries)",
        "waterbodies": waterbodies,
        "stats": {
            "total": total,
            "resolved_to_reach": resolved_to_reach,
            "unresolved": total - resolved_to_reach,
        },
    }


def resolve_stocking_from_json(
    source_json_path: Path,
    poly_reaches_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Db-free re-resolve: rebuild the stocking.json structure from a previously
    generated stocking.json, re-mapping each waterbody's ``reach_id`` against the
    current ``poly_reaches`` while carrying the (frozen) FIDQ ``releases`` and
    ``fidq_name`` through unchanged.

    Lets the lightweight daily cron run without the 66 MB anglerinfo.db — the
    only thing that changes between the heavy manual fetch+match runs is the
    reach mapping, which lives in poly_reaches.json. Full/local runs still use
    :func:`resolve_stocking` against the db (the source of truth for releases).
    """
    if not source_json_path.exists():
        raise FileNotFoundError(
            f"{source_json_path} not found — the db-free re-resolve needs an "
            "existing stocking.json (fetched from R2 in CI)."
        )
    if poly_reaches_path is None:
        cfg = ProjectConfig()
        poly_reaches_path = cfg.get_path(
            "output", "pipeline", "deploy", default="output/pipeline/deploy"
        ) / "poly_reaches.json"
    poly_reaches = _load_poly_reaches(poly_reaches_path)

    with open(source_json_path, encoding="utf-8") as f:
        src = json.load(f)

    waterbodies: List[Dict[str, Any]] = []
    resolved_to_reach = 0
    for wb in src.get("waterbodies", []):
        waterbody_key = wb.get("waterbody_key")
        reach_id = None
        # poly_reaches only maps legitimately-built reaches, so attempting a
        # lookup for every key is equivalent to the db path's status gate.
        for wbk in _split_keys(waterbody_key or ""):
            reach_id = poly_reaches.get(wbk)
            if reach_id:
                break
        if reach_id:
            resolved_to_reach += 1
        waterbodies.append({
            "waterbody_key": waterbody_key or None,
            "reach_id": reach_id,
            "fidq_name": wb.get("fidq_name"),
            "releases": wb.get("releases", []),
        })

    total = len(waterbodies)
    from pipeline.recurring.provenance import provenance

    return {
        **provenance(
            generator="stocking_resolver",
            source="FIDQ (BC gov Fish Inventories Data Queries)",
            source_url="https://a100.gov.bc.ca/pub/fidq/main.do",
            attribution=(
                "Fish stocking data sourced from the Province of British Columbia "
                "(Fish Inventories Data Queries) and used under the Province's "
                "copyright terms (https://www2.gov.bc.ca/gov/content/home/copyright)."
            ),
        ),
        "source": "FIDQ (BC gov Fish Inventories Data Queries)",
        "waterbodies": waterbodies,
        "stats": {
            "total": total,
            "resolved_to_reach": resolved_to_reach,
            "unresolved": total - resolved_to_reach,
        },
    }


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cfg = ProjectConfig()

    parser = argparse.ArgumentParser(description="Resolve FIDQ stocking records to reach IDs.")
    parser.add_argument("--db", type=Path, default=WATERBODY_DB_PATH, help="anglerinfo.db path.")
    parser.add_argument(
        "--source-json", type=Path, default=None,
        help="Existing stocking.json to re-resolve reach_ids from, db-free "
             "(used by the lightweight CI cron). Takes precedence over --db.",
    )
    parser.add_argument(
        "--poly-reaches", type=Path, default=None,
        help="poly_reaches.json path (default: output/pipeline/deploy/poly_reaches.json).",
    )
    parser.add_argument(
        "--out", type=Path,
        default=cfg.get_path("output", "pipeline", "stocking", default="output/pipeline/matching/stocking.json"),
        help="Output stocking.json path.",
    )
    args = parser.parse_args(argv)

    if args.source_json is not None:
        result = resolve_stocking_from_json(args.source_json, args.poly_reaches)
    else:
        result = resolve_stocking(args.db, args.poly_reaches)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    stats = result["stats"]
    logger.info(
        "Wrote %d waterbody(ies) (%d resolved to a reach, %d unresolved) -> %s",
        stats["total"], stats["resolved_to_reach"], stats["unresolved"], args.out,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
