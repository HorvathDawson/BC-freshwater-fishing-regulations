"""Mobile SQLite bundler — re-serializes the regulation index for on-device use.

This is a *sibling* of :mod:`pipeline.deploy.r2_sharder`.  Where the R2 sharder
splits the index into thousands of hash-bucketed JSON files optimised for edge
random-access (one Worker fetch per map click), this module packs the **same
in-memory index dict** into a single SQLite database that a React Native app
downloads once and queries locally — enabling fully offline search + tap
resolution with no per-interaction network round-trips.

Single source of truth: the fid / reach / poly / tier0 payloads are produced by
reusing the R2 sharder's builders (:func:`build_fid_shards`,
:func:`build_reach_shards`, :func:`build_poly_shards`, :func:`build_tier0`) and
flattening their bucketed output into tables.  The two deploy artifacts can
therefore never drift.

Outputs (written under ``{output_dir}/mobile/v{N}/``):
  regulations.sqlite       — the queryable database (see schema below)
  regulations.sqlite.gz    — gzipped copy for transfer (app gunzips on device)
  manifest.json            — version, counts, file sizes + sha256 for integrity

Schema:
  meta(key PK, value)                       — version, built_at, counts
  regulations(reg_id PK, json)              — reg_id → regulation display info
  reg_sets(idx PK, reg_ids)                 — comma-joined reg_id strings by index
  reaches(reach_id PK, json)                — reach metadata incl. fids[] + bathymetry
  fids(fid PK, reach_id)                     — tile fid → reach_id (tap resolution)
  polys(wbk PK, reach_id)                    — waterbody_key → reach_id
  search(id PK, display_name, feature_type, waterbody_group, min_zoom, bbox,
         regions, zones, management_units, total_length_km, name_variants,
         nearby_towns, segments)             — one row per named feature
  search_fts(display_name, name_variants, nearby_towns)  — FTS5 over search (rowid=id)

The map itself is unchanged: the app range-reads the same PMTiles from R2, and
bathymetry depth-map PDFs are fetched on demand from ``bathymetry/<pdf>``.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.deploy.r2_sharder import (
    build_fid_shards,
    build_poly_shards,
    build_reach_shards,
    build_tier0,
)

log = logging.getLogger(__name__)

DB_NAME = "regulations.sqlite"
GZ_NAME = "regulations.sqlite.gz"
MANIFEST_NAME = "manifest.json"


# ── Helpers ───────────────────────────────────────────────────────────


def _flatten(grouped: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Merge {prefix: {key: value}} shard buckets into one flat {key: value}."""
    flat: dict[str, Any] = {}
    for bucket in grouped.values():
        flat.update(bucket)
    return flat


def _names_text(name_variants: list[dict[str, Any]]) -> str:
    """Join variant names into a single searchable string for FTS."""
    return " ".join(nv.get("name", "") for nv in name_variants if nv.get("name"))


def _towns_text(nearby_towns: list) -> str:
    """Join nearby-town names into a searchable FTS string.

    Accepts both the current ``[{"name", "km"}, ...]`` records and the legacy
    ``["name", ...]`` string list so older indexes still shard cleanly.
    """
    parts = []
    for t in nearby_towns:
        if isinstance(t, dict):
            name = t.get("name", "")
        else:
            name = t
        if name:
            parts.append(name)
    return " ".join(parts)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ── Schema ────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE regulations (reg_id TEXT PRIMARY KEY, json TEXT NOT NULL);
CREATE TABLE reg_sets (idx INTEGER PRIMARY KEY, reg_ids TEXT NOT NULL);
CREATE TABLE reaches (reach_id TEXT PRIMARY KEY, json TEXT NOT NULL);
CREATE TABLE fids (fid TEXT PRIMARY KEY, reach_id TEXT NOT NULL);
CREATE TABLE polys (wbk TEXT PRIMARY KEY, reach_id TEXT NOT NULL);
CREATE TABLE search (
    id INTEGER PRIMARY KEY,
    display_name TEXT,
    feature_type TEXT,
    waterbody_group TEXT,
    min_zoom INTEGER,
    bbox TEXT,
    regions TEXT,
    zones TEXT,
    management_units TEXT,
    total_length_km REAL,
    name_variants TEXT,
    nearby_towns TEXT,
    segments TEXT
);
CREATE VIRTUAL TABLE search_fts USING fts5(
    display_name, name_variants, nearby_towns, tokenize='unicode61'
);
"""


def _build_db(db_path: Path, data: dict[str, Any], version_str: str) -> dict[str, int]:
    """Create and populate the SQLite database. Returns row counts."""
    reaches: dict[str, Any] = data.get("reaches", {})
    reach_segments: dict[str, list[str]] = data.get("reach_segments", {})
    poly_reaches: dict[str, str] = data.get("poly_reaches", {})
    regulations: dict[str, Any] = data.get("regulations", {})
    reg_sets: list[str] = data.get("reg_sets", [])
    search_index: list[dict[str, Any]] = data.get("search_index", [])

    # Reuse the R2 sharder builders, then flatten the bucketed output so the
    # mobile tables carry identical content to the web shards (single source).
    fid_to_reach = _flatten(build_fid_shards(reach_segments))
    reach_payloads = _flatten(build_reach_shards(reaches, reach_segments))
    wbk_to_reach = _flatten(build_poly_shards(poly_reaches))

    # tier0 enrichment gives the exact search_index (with segments) the web
    # app consumes — reuse it verbatim so search behaviour matches.
    tier0 = build_tier0(
        regulations, reg_sets, search_index, reaches, reach_segments, version_str
    )
    enriched_search: list[dict[str, Any]] = tier0["search_index"]

    conn = sqlite3.connect(db_path)
    try:
        conn.executescript("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;")
        conn.executescript(_SCHEMA)

        conn.executemany(
            "INSERT INTO regulations (reg_id, json) VALUES (?, ?)",
            [
                (reg_id, json.dumps(info, ensure_ascii=False, separators=(",", ":")))
                for reg_id, info in regulations.items()
            ],
        )
        conn.executemany(
            "INSERT INTO reg_sets (idx, reg_ids) VALUES (?, ?)",
            list(enumerate(reg_sets)),
        )
        conn.executemany(
            "INSERT INTO reaches (reach_id, json) VALUES (?, ?)",
            [
                (rid, json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
                for rid, payload in reach_payloads.items()
            ],
        )
        conn.executemany(
            "INSERT INTO fids (fid, reach_id) VALUES (?, ?)",
            list(fid_to_reach.items()),
        )
        conn.executemany(
            "INSERT INTO polys (wbk, reach_id) VALUES (?, ?)",
            list(wbk_to_reach.items()),
        )

        # Search rows + parallel FTS rows keyed by the same rowid (id).
        search_rows: list[tuple] = []
        fts_rows: list[tuple] = []
        for idx, entry in enumerate(enriched_search):
            name_variants = entry.get("name_variants", [])
            nearby_towns = entry.get("nearby_towns", [])
            search_rows.append(
                (
                    idx,
                    entry.get("display_name", ""),
                    entry.get("feature_type", ""),
                    entry.get("waterbody_group", ""),
                    entry.get("min_zoom", 11),
                    json.dumps(entry.get("bbox"), separators=(",", ":")),
                    json.dumps(entry.get("regions", []), separators=(",", ":")),
                    json.dumps(entry.get("zones", []), separators=(",", ":")),
                    json.dumps(
                        entry.get("management_units", []), separators=(",", ":")
                    ),
                    entry.get("total_length_km", 0),
                    json.dumps(name_variants, ensure_ascii=False, separators=(",", ":")),
                    json.dumps(nearby_towns, ensure_ascii=False, separators=(",", ":")),
                    json.dumps(
                        entry.get("segments", []),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                )
            )
            fts_rows.append(
                (
                    idx,
                    entry.get("display_name", ""),
                    _names_text(name_variants),
                    _towns_text(nearby_towns),
                )
            )

        conn.executemany(
            "INSERT INTO search (id, display_name, feature_type, waterbody_group, "
            "min_zoom, bbox, regions, zones, management_units, total_length_km, "
            "name_variants, nearby_towns, segments) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            search_rows,
        )
        conn.executemany(
            "INSERT INTO search_fts (rowid, display_name, name_variants, nearby_towns) "
            "VALUES (?, ?, ?, ?)",
            fts_rows,
        )

        counts = {
            "regulations": len(regulations),
            "reg_sets": len(reg_sets),
            "reaches": len(reach_payloads),
            "fids": len(fid_to_reach),
            "polys": len(wbk_to_reach),
            "search": len(enriched_search),
        }
        built_at = datetime.now(timezone.utc).isoformat()
        meta_rows = [
            ("shard_version", version_str),
            ("built_at", built_at),
            *[(f"count_{k}", str(v)) for k, v in counts.items()],
        ]
        conn.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", meta_rows)

        conn.commit()
        conn.executescript("PRAGMA optimize; VACUUM;")
        conn.commit()
    finally:
        conn.close()

    return counts


# ── Public API ────────────────────────────────────────────────────────


def build_mobile_sqlite(
    data: dict[str, Any],
    output_dir: str | Path,
    version: int | str,
) -> dict[str, Any]:
    """Pack the in-memory regulation index into a downloadable SQLite bundle.

    Args:
        data:       The regulation index dict (same object passed to
                    :func:`pipeline.deploy.r2_sharder.shard_from_dict`).
        output_dir: Deploy directory; the bundle lands in ``mobile/v{N}/``.
        version:    Shard version (e.g. 8 → "v8"), shared with the web shards.

    Returns:
        Summary dict with the output paths, row counts and file sizes.
    """
    output_dir = Path(output_dir)
    version_str = f"v{version}" if not str(version).startswith("v") else str(version)
    mobile_dir = output_dir / "mobile" / version_str
    mobile_dir.mkdir(parents=True, exist_ok=True)

    db_path = mobile_dir / DB_NAME
    gz_path = mobile_dir / GZ_NAME
    manifest_path = mobile_dir / MANIFEST_NAME

    # SQLite INSERTs append — start from a clean file each build.
    if db_path.exists():
        db_path.unlink()

    t0 = time.perf_counter()
    counts = _build_db(db_path, data, version_str)
    log.info(
        "mobile sqlite built in %.1fs — %d fids, %d reaches, %d polys, %d search",
        time.perf_counter() - t0,
        counts["fids"],
        counts["reaches"],
        counts["polys"],
        counts["search"],
    )

    # Gzip for transfer (app decompresses on device).
    with open(db_path, "rb") as f_in, gzip.open(gz_path, "wb", compresslevel=6) as f_out:
        shutil.copyfileobj(f_in, f_out)

    db_bytes = db_path.stat().st_size
    gz_bytes = gz_path.stat().st_size

    manifest = {
        "version": version_str,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "counts": counts,
        "pmtiles": "freshwater_atlas.pmtiles",
        "files": [
            {
                "name": DB_NAME,
                "gzip": GZ_NAME,
                "bytes": db_bytes,
                "gzip_bytes": gz_bytes,
                "sha256": _sha256(db_path),
                "gzip_sha256": _sha256(gz_path),
            }
        ],
    }
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    log.info(
        "mobile bundle → %s (%.1f MB db, %.1f MB gz)",
        mobile_dir,
        db_bytes / 1_048_576,
        gz_bytes / 1_048_576,
    )

    return {
        "version": version_str,
        "mobile_dir": str(mobile_dir),
        "db_path": str(db_path),
        "gz_path": str(gz_path),
        "manifest_path": str(manifest_path),
        "db_bytes": db_bytes,
        "gz_bytes": gz_bytes,
        "counts": counts,
    }
