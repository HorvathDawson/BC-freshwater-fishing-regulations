"""Backfill entry-level `tributaries.included` from the synopsis tributary symbol.

The parser skewed nearly every entry to `tributaries.included=false`, even for rows the synopsis flags
with the **[Includes Tributaries]** symbol ("Incl. Tribs"). That symbol is the authoritative entry-level
signal but never survived into `regs_verbatim`, so the flag was lost. This backfills it from the SAME
source the parser reads (`load_synopsis_rows`), joining each entry to its row by verbatim regs (name as
tiebreaker — the exact join `curation-review` uses for the source image).

Additive + safe: only flips `included: false -> true` for symbol-flagged rows, never the reverse, and
never touches a `locked` (hand-reviewed) entry. Each file is re-validated through the `EntryFile` model
before writing.

    .venv/bin/python -m pipeline.oneoff.backfill_tributary_symbols [--dry-run]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.parsing.entry_models import EntryFile
from pipeline.parsing.rows import load_synopsis_rows, row_includes_tributaries

_ROOT = Path(__file__).resolve().parents[2]
_ENTRIES_DIR = _ROOT / "pipeline" / "parsing" / "entries"


def _symbol_index() -> dict[str, list[tuple[str, bool]]]:
    """raw_regs -> [(water_lower, has_tributary_symbol)] — same key the source-image join uses."""
    idx: dict[str, list[tuple[str, bool]]] = {}
    for r in load_synopsis_rows():
        raw = r.get("raw_regs")
        if raw:
            idx.setdefault(raw, []).append((str(r.get("water", "")).lower(), row_includes_tributaries(r)))
    return idx


def _row_has_symbol(entry: dict, idx: dict[str, list[tuple[str, bool]]]) -> bool | None:
    """Whether the entry's synopsis row is tributary-flagged. None = no confident row match (skip)."""
    cands = idx.get(entry.get("regs_verbatim", ""))
    if not cands:
        return None
    if len(cands) == 1:
        return cands[0][1]
    name = str(entry.get("identity", {}).get("name", "")).lower()
    for water, has in cands:
        if water == name:
            return has
    return None  # ambiguous shared regs, no name match -> don't guess


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="report changes without writing")
    args = ap.parse_args()

    idx = _symbol_index()
    total_changed = 0
    for path in sorted(_ENTRIES_DIR.glob("region-*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        changed: list[str] = []
        for e in data.get("entries", []):
            if e.get("locked"):
                continue
            tribs = e.setdefault("tributaries", {"included": False, "only": False, "excludes": []})
            if tribs.get("included") is True:
                continue
            if _row_has_symbol(e, idx) is True:
                tribs["included"] = True
                changed.append(e["entry_id"])
        if not changed:
            continue
        EntryFile.model_validate(data)  # never write a file the model would reject
        if not args.dry_run:
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        total_changed += len(changed)
        print(f"{path.name}: {len(changed)} entries -> included=true")
        for eid in changed:
            print(f"    {eid}")
    verb = "would set" if args.dry_run else "set"
    print(f"\n{verb} tributaries.included=true on {total_changed} entries")


if __name__ == "__main__":
    main()
