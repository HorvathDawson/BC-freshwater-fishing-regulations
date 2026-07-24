"""
stocking resolver — DECOUPLED into fetch-shaping (cron) vs matching (pipeline).

Two capabilities, mirroring the hydro split:

  * ``export_records`` (CRON) — read fidq_stocking_records and write per-waterbody
    ``cron/stocking/records/<waterbody_id>.json`` + a small ``stocking_index.json``.
    NO reach resolution, NO match_final, NO poly_reaches — just fetch-shaped data
    keyed by the FIDQ waterbody_id. The light weekly cron owns this.

  * ``build_matches`` (PIPELINE) — read match_final (source='stocking') + poly_reaches
    and write the static ``stocking_matches.json``::

        { "reach":     { "<reach_id>": ["<waterbody_id>", ...] },
          "waterbody": { "<waterbody_key>": ["<waterbody_id>", ...] } }

    Produced once per full build (it only changes when reaches/matches change).

The webapp loads ``stocking_matches.json`` and, on info-panel open, maps a reach_id
(and/or waterbody_key) → waterbody_ids, then fetches ``records/<waterbody_id>.json``.

CLI
---
    python -m pipeline.recurring.stocking.resolver records --out <cron/stocking dir>
    python -m pipeline.recurring.stocking.resolver matches --poly-reaches <path> --out <file>
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from collections import defaultdict
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
            "poly_reaches.json not found at %s — stocking matches will be empty. "
            "Run a full build first (it's written by pipeline/enrichment/builder.py).",
            path,
        )
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _provenance() -> Dict[str, Any]:
    from pipeline.recurring.provenance import provenance

    return provenance(
        generator="stocking_resolver",
        source="FIDQ (BC gov Fish Inventories Data Queries)",
        source_url="https://a100.gov.bc.ca/pub/fidq/main.do",
        attribution=(
            "Fish stocking data sourced from the Province of British Columbia "
            "(Fish Inventories Data Queries) and used under the Province's "
            "copyright terms (https://www2.gov.bc.ca/gov/content/home/copyright)."
        ),
    )


# ── CRON: per-waterbody records + index (no reach resolution) ────────────────

def export_records(db_path: Path, out_dir: Path) -> Dict[str, Any]:
    """Write cron/stocking/records/<waterbody_id>.json for every FIDQ waterbody
    that has stocking history, plus stocking_index.json. Keyed by waterbody_id —
    the frontend resolves reach_id → waterbody_id via the static match table."""
    if not db_path.exists():
        raise FileNotFoundError(
            f"{db_path} not found — run pipeline.recurring.anglerinfo's fetch chain first."
        )
    records_dir = out_dir / "records"
    records_dir.mkdir(parents=True, exist_ok=True)

    release_cols = ", ".join(_RELEASE_FIELDS)
    conn = sqlite3.connect(db_path)
    try:
        if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='fidq_stocking_records'"
        ).fetchone():
            raise FileNotFoundError(
                f"{db_path} has no fidq_stocking_records — run fetch_stocking first."
            )
        # One entry per waterbody that has any release row. gazetted_name rides along.
        waterbodies = conn.execute(
            "SELECT DISTINCT waterbody_id, gazetted_name FROM fidq_stocking_records"
        ).fetchall()

        index_entries: List[Dict[str, Any]] = []
        for waterbody_id, name in waterbodies:
            wid = str(waterbody_id)
            releases = [
                dict(zip(_RELEASE_FIELDS, r))
                for r in conn.execute(
                    f"SELECT {release_cols} FROM fidq_stocking_records WHERE waterbody_id = ? "
                    "ORDER BY release_date DESC",
                    (waterbody_id,),
                ).fetchall()
            ]
            if not releases:
                continue
            (records_dir / f"{wid}.json").write_text(
                json.dumps({"id": wid, "name": name, "releases": releases},
                           separators=(",", ":"), ensure_ascii=False),
                encoding="utf-8",
            )
            index_entries.append({"id": wid, "name": name, "n": len(releases)})
    finally:
        conn.close()

    index = {**_provenance(), "count": len(index_entries), "waterbodies": index_entries}
    (out_dir / "stocking_index.json").write_text(
        json.dumps(index, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    return {"count": len(index_entries)}


# ── PIPELINE: static reach/waterbody → waterbody_id match table ──────────────

def build_matches(db_path: Path, poly_reaches_path: Path) -> Dict[str, Any]:
    """From match_final (source='stocking') + poly_reaches, build the static
    reach_id/waterbody_key → [waterbody_id] index the frontend joins on."""
    if not db_path.exists():
        raise FileNotFoundError(
            f"{db_path} not found — run pipeline.recurring.anglerinfo's fetch+match chain first."
        )
    poly_reaches = _load_poly_reaches(poly_reaches_path)

    reach: Dict[str, List[str]] = defaultdict(list)
    waterbody: Dict[str, List[str]] = defaultdict(list)
    conn = sqlite3.connect(db_path)
    try:
        if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='match_final'"
        ).fetchone():
            raise FileNotFoundError(
                f"{db_path} has no match_final table — run the full match chain first."
            )
        rows = conn.execute(
            "SELECT source_id, waterbody_key, status FROM match_final WHERE source = 'stocking'"
        ).fetchall()
        for waterbody_id, waterbody_key, status in rows:
            wid = str(waterbody_id)
            if status != "matched":
                continue
            for wbk in _split_keys(waterbody_key):
                waterbody[wbk].append(wid)
                rid = poly_reaches.get(wbk)
                if rid:
                    reach[rid].append(wid)
    finally:
        conn.close()

    return {
        "reach": {k: sorted(set(v)) for k, v in reach.items()},
        "waterbody": {k: sorted(set(v)) for k, v in waterbody.items()},
    }


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cfg = ProjectConfig()
    deploy = cfg.get_path("output", "pipeline", "deploy", default="output/pipeline/deploy")

    parser = argparse.ArgumentParser(description="Stocking: per-waterbody records (cron) or match table (pipeline).")
    parser.add_argument("--db", type=Path, default=WATERBODY_DB_PATH, help="anglerinfo.db path.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_rec = sub.add_parser("records", help="CRON: write per-waterbody records + index.")
    p_rec.add_argument("--out", type=Path, default=deploy / "cron" / "stocking",
                       help="cron/stocking output dir (records/ + stocking_index.json).")

    p_mat = sub.add_parser("matches", help="PIPELINE: write the reach→waterbody match table.")
    p_mat.add_argument("--poly-reaches", type=Path, default=deploy / "poly_reaches.json",
                       help="poly_reaches.json path.")
    p_mat.add_argument("--out", type=Path, default=deploy / "cron" / "stocking" / "stocking_matches.json",
                       help="Output stocking_matches.json path.")

    args = parser.parse_args(argv)

    if args.command == "records":
        stats = export_records(args.db, args.out)
        logger.info("Wrote %d per-waterbody record file(s) → %s", stats["count"], args.out)
    elif args.command == "matches":
        matches = build_matches(args.db, args.poly_reaches)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(matches, separators=(",", ":"), ensure_ascii=False),
                            encoding="utf-8")
        logger.info("Wrote match table (%d reach keys, %d waterbody keys) → %s",
                    len(matches["reach"]), len(matches["waterbody"]), args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
