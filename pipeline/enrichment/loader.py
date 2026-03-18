"""Phase 1 — Load & merge all inputs into RegulationRecords.

Reads raw synopsis rows, match_table (base + overrides), and session
state, then merges them by positional index into a list of frozen
RegulationRecord objects.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from pipeline.matching.base_entry_builder import load_overrides
from pipeline.matching.match_table import (
    AnyEntry,
    BaseEntry,
    MatchTable,
    OverrideEntry,
)

from .models import RegulationRecord

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Reg-ID generation
# ---------------------------------------------------------------------------

_SANITIZE_RE = re.compile(r"[^A-Z0-9]+")


def _make_reg_id(water: str, mu: List[str], region: Optional[str]) -> str:
    """Generate a deterministic regulation ID.

    Format: R{zone}_{SANITIZED_NAME}_{sorted_mus}
    Zone is derived from the first MU prefix (e.g. "2-7" → "2").
    Falls back to "X" if no MU available.
    """
    # Zone from first MU
    zone = "X"
    for m in sorted(mu):
        parts = m.split("-", 1)
        if parts[0].isdigit():
            zone = parts[0]
            break

    # Sanitise water name
    name = _SANITIZE_RE.sub("_", water.upper()).strip("_")
    if not name:
        name = "UNNAMED"

    # MU suffix
    mu_str = "_".join(sorted(mu)) if mu else "NOMU"

    return f"R{zone}_{name}_{mu_str}"


# ---------------------------------------------------------------------------
# Raw row flattening (same logic as parser.py)
# ---------------------------------------------------------------------------


def _flatten_raw_pages(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Flatten synopsis_raw_data.json pages → individual row dicts."""
    rows: List[Dict[str, Any]] = []
    for page in pages:
        region = page.get("context", {}).get("region")
        for row in page.get("rows", []):
            row_dict = dict(row)
            if region and not row_dict.get("region"):
                row_dict["region"] = region
            rows.append(row_dict)
    return rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_and_merge(
    raw_path: Path,
    match_table_path: Path,
    overrides_path: Path,
    session_path: Path,
) -> List[RegulationRecord]:
    """Load all inputs, merge by positional index, return RegulationRecords.

    Steps:
        1. Flatten raw pages → N rows
        2. Load match_table.json → base entries (parallel array, same N)
        3. Load overrides.json → override entries
        4. Build MatchTable (base + overrides, override wins)
        5. Load session_state.json → parsed results + status per row
        6. For each row i, merge into RegulationRecord

    Raises ValueError if array lengths are mismatched.
    """
    # 1. Raw rows
    logger.info("Loading raw synopsis rows from %s", raw_path)
    with open(raw_path, encoding="utf-8") as f:
        pages = json.load(f)
    raw_rows = _flatten_raw_pages(pages)
    logger.info("  %d rows flattened", len(raw_rows))

    # 2. Base entries
    logger.info("Loading match table from %s", match_table_path)
    with open(match_table_path, encoding="utf-8") as f:
        base_dicts = json.load(f)
    base_entries = [BaseEntry.from_dict(d) for d in base_dicts]
    logger.info("  %d base entries", len(base_entries))

    # 3. Overrides
    logger.info("Loading overrides from %s", overrides_path)
    overrides = load_overrides(overrides_path)
    logger.info("  %d override entries", len(overrides))

    # 4. Build lookup table
    table = MatchTable(bases=base_entries, overrides=overrides)

    # 5. Session state
    logger.info("Loading session state from %s", session_path)
    with open(session_path, encoding="utf-8") as f:
        session = json.load(f)
    results: List[Optional[Dict[str, Any]]] = session["results"]
    statuses: List[str] = session["status"]

    # Validate alignment
    n = len(raw_rows)
    if len(base_entries) != n:
        raise ValueError(
            f"match_table has {len(base_entries)} entries but raw data has {n} rows"
        )
    if len(results) != n:
        raise ValueError(
            f"session has {len(results)} results but raw data has {n} rows"
        )

    # 6. Merge
    records: List[RegulationRecord] = []
    skipped = 0
    unmatched = 0

    for i in range(n):
        row = raw_rows[i]
        base = base_entries[i]
        water = row.get("water", "")
        mu = row.get("mu", [])
        region = base.criteria.region or row.get("region") or ""

        # Lookup via MatchTable (override wins if exists)
        entry: Optional[AnyEntry] = table.lookup(
            name_verbatim=base.criteria.name_verbatim,
            region=base.criteria.region,
            mus=base.criteria.mus,
        )

        if entry is None:
            # Fallback to base entry directly
            logger.debug("Row %d (%s): no MatchTable hit, using base entry", i, water)
            entry = base
            unmatched += 1

        # Skip entries flagged by overrides
        if isinstance(entry, OverrideEntry) and entry.skip:
            logger.debug("Row %d (%s): skipped by override", i, water)
            skipped += 1
            continue

        # Resolve region from entry criteria (overrides may fix null regions)
        region = entry.criteria.region or region

        # Parse status
        status = statuses[i]
        parsed = results[i] if status == "success" else None
        parse_status = "success" if status == "success" else "failed"

        reg_id = _make_reg_id(water, mu, region)

        records.append(
            RegulationRecord(
                index=i,
                reg_id=reg_id,
                water=water,
                region=region,
                mu=tuple(mu),
                raw_regs=row.get("raw_regs", ""),
                symbols=tuple(row.get("symbols", [])),
                page=row.get("page", 0),
                image=row.get("image", ""),
                match_entry=entry,
                parsed=parsed,
                parse_status=parse_status,
            )
        )

    # Collision check (Debbie's concern)
    seen_ids: Dict[str, int] = {}
    for rec in records:
        if rec.reg_id in seen_ids:
            # Append index suffix to disambiguate
            original = rec.reg_id
            new_id = f"{original}__{rec.index}"
            logger.warning(
                "Reg ID collision: %s (rows %d and %d) → renaming to %s",
                original,
                seen_ids[original],
                rec.index,
                new_id,
            )
            # Frozen dataclass — must recreate
            records[records.index(rec)] = RegulationRecord(
                index=rec.index,
                reg_id=new_id,
                water=rec.water,
                region=rec.region,
                mu=rec.mu,
                raw_regs=rec.raw_regs,
                symbols=rec.symbols,
                page=rec.page,
                image=rec.image,
                match_entry=rec.match_entry,
                parsed=rec.parsed,
                parse_status=rec.parse_status,
            )
            seen_ids[new_id] = rec.index
        else:
            seen_ids[rec.reg_id] = rec.index

    logger.info(
        "Phase 1 complete: %d records (%d skipped, %d unmatched lookups)",
        len(records),
        skipped,
        unmatched,
    )
    return records
