"""Migrate the checked-in EntryFiles to the current `entry_models` schema.

One-time, idempotent, in-place migration for a datatype rev:
  * rename legacy Extent keys everywhere they appear (rule.extents, rule.tributary_excludes,
    entry.scope, tributaries.excludes): `item -> item_id`, `area -> area_id`, `kind -> area_kind`;
  * re-serialize every entry through the `EntryFile` model, which puts fields in canonical order and
    MATERIALIZES newer additive fields (source_symbols, revisit, revisit_note, parse_review) with
    their defaults — so the on-disk shape matches what a fresh ingest writes.

Nothing semantic changes (all legacy item/area/kind values are carried over verbatim under the new
names). Safe to re-run: a migrated file round-trips to itself.

    .venv/bin/python -m pipeline.hack.migrate_entries [--dry-run]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.parsing.entry_models import EntryFile
from pipeline.curated import CURATED, SOURCE

_ROOT = Path(__file__).resolve().parents[2]
_ENTRIES_DIR = CURATED.regulations.entries.synopsis

_EXTENT_RENAMES = {"item": "item_id", "area": "area_id", "kind": "area_kind"}


def _migrate_extent(ex: dict) -> None:
    for old, new in _EXTENT_RENAMES.items():
        if old in ex and new not in ex:
            ex[new] = ex.pop(old)


def _migrate_entry_dict(e: dict) -> None:
    """Rename legacy Extent keys in every extent list on the entry (mutates in place)."""
    for ex in e.get("scope") or []:
        _migrate_extent(ex)
    for ex in (e.get("tributaries") or {}).get("excludes") or []:
        _migrate_extent(ex)
    for r in e.get("rules") or []:
        for ex in (r.get("extents") or []):
            _migrate_extent(ex)
        for ex in (r.get("tributary_excludes") or []):
            _migrate_extent(ex)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="validate + report only; write nothing")
    args = ap.parse_args()

    total = 0
    paths = (sorted(_ENTRIES_DIR.glob("region-*.json"))
             + sorted((_ENTRIES_DIR / "reviewed").glob("region-*.json")))   # incl. the curator overlay
    for path in paths:
        data = json.loads(path.read_text(encoding="utf-8"))
        for e in data.get("entries", []):
            _migrate_entry_dict(e)
        ef = EntryFile.model_validate(data)               # legacy keys ignored; renamed keys carried over
        canonical = ef.model_dump_json(indent=2)          # canonical order + materialized defaults
        if canonical != path.read_text(encoding="utf-8"):
            total += 1
            if not args.dry_run:
                path.write_text(canonical, encoding="utf-8")
            print(f"{'would migrate' if args.dry_run else 'migrated'}: {path.name} ({len(ef.entries)} entries)")
    print(f"\n{total} file(s) {'to migrate' if args.dry_run else 'migrated'}")


if __name__ == "__main__":
    main()
