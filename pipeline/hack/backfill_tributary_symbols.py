"""Backfill synopsis symbol provenance onto entries (and the entry-level tributaries flag).

The parser skewed nearly every entry to `tributaries.included=false`, even for rows the synopsis flags
with the **[Includes Tributaries]** symbol ("Incl. Tribs"). That symbol is the authoritative entry-level
signal but never survived into the entry, so the flag was lost. This joins each entry to its synopsis
row (SAME source the parser reads, `load_synopsis_rows`) and writes back:
  * `source.symbols` — the verbatim row symbols ('Incl. Tribs' / 'Classified' / 'Stocked'), so the entry
    is self-describing and re-validatable without the extraction file;
  * `tributaries.included = true` when the row is tributary-flagged.

Additive + safe: only flips `included: false -> true` for symbol-flagged rows, never the reverse, and
never touches a `locked` (hand-reviewed) entry. Each file is re-validated through the `EntryFile` model
before writing.

    .venv/bin/python -m pipeline.hack.backfill_tributary_symbols [--dry-run]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.regs.parsing.entry_models import EntryFile
from pipeline.regs.parsing.rows import load_synopsis_rows, symbols_include_tributaries
from pipeline.common.curated import CURATED, SOURCE

_ROOT = Path(__file__).resolve().parents[2]
_ENTRIES_DIR = CURATED.regulations.entries.synopsis


def _symbol_index() -> dict[str, list[tuple[str, list[str]]]]:
    """raw_regs -> [(water_lower, symbols)] — same key the source-image join uses."""
    idx: dict[str, list[tuple[str, list[str]]]] = {}
    for r in load_synopsis_rows():
        raw = r.get("raw_regs")
        if raw:
            idx.setdefault(raw, []).append((str(r.get("water", "")).lower(), list(r.get("symbols", []))))
    return idx


def _row_symbols(entry: dict, idx: dict[str, list[tuple[str, list[str]]]]) -> list[str] | None:
    """The entry's synopsis-row symbols. None = no confident row match (skip)."""
    cands = idx.get(entry.get("regs_verbatim", ""))
    if not cands:
        return None
    if len(cands) == 1:
        return cands[0][1]
    name = str(entry.get("identity", {}).get("name", "")).lower()
    for water, syms in cands:
        if water == name:
            return syms
    return None  # ambiguous shared regs, no name match -> don't guess


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="report changes without writing")
    args = ap.parse_args()

    idx = _symbol_index()
    total_changed = 0
    paths = (sorted(_ENTRIES_DIR.glob("region-*.json"))
             + sorted((_ENTRIES_DIR / "reviewed").glob("region-*.json")))   # incl. the curator overlay
    for path in paths:
        data = json.loads(path.read_text(encoding="utf-8"))
        changed: list[str] = []
        for e in data.get("entries", []):
            syms = _row_symbols(e, idx)
            if syms is None:
                continue
            entry_changed = False
            if (e.get("source") or {}).get("symbols") != syms:  # store provenance (re-validatable later)
                e["source"] = {**(e.get("source") or {}), "symbols": syms}
                entry_changed = True
            if not e.get("locked"):                             # never override a hand-reviewed flag
                tribs = e.setdefault("tributaries", {"included": False, "only": False, "excludes": []})
                want = symbols_include_tributaries(syms)
                if want and tribs.get("included") is not True:  # additive: only false -> true
                    tribs["included"] = True
                    entry_changed = True
            if entry_changed:
                changed.append(e["entry_id"])
        if not changed:
            continue
        EntryFile.model_validate(data)  # never write a file the model would reject
        if not args.dry_run:
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        total_changed += len(changed)
        print(f"{path.name}: {len(changed)} entries updated (source.symbols / included)")
        for eid in changed:
            print(f"    {eid}")
    verb = "would update" if args.dry_run else "updated"
    print(f"\n{verb} {total_changed} entries")


if __name__ == "__main__":
    main()
