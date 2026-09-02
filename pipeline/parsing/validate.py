"""Self-check a candidate parse against its batch — the SAME gate ingest runs.

Designed to be run by the parsing agent itself (see PARSE_PROMPT.md batch envelope) so it can iterate
until clean before submitting. Self-contained: needs only the batch file (which carries each item's
`raw_regs` + `bindable_ids`) and the candidate JSON — no registry or FWA data.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.validate batch_000.json candidate.json

Exit code 0 = every candidate entry is valid; 1 = at least one failed (details printed).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipeline.parsing import io
from pipeline.parsing.entry_models import Entry, unused_splits, validate_entry_splits
from pipeline.parsing.rows import symbols_include_tributaries

load_batch_items = io.load_batch_items                   # shared helper (io is the single home)


def _candidates(candidate_path: str | Path) -> list[dict]:
    """Accept an array of {index, entry}, a single {index, entry}, or a bare entry (index -1)."""
    data = json.loads(Path(candidate_path).read_text(encoding="utf-8"))
    if isinstance(data, dict) and "entry" in data:
        return [data]
    if isinstance(data, dict):                       # a bare entry object
        return [{"index": data.get("_index", -1), "entry": data}]
    return list(data)


def validate_candidate(item: dict, entry_data: dict) -> tuple[Entry | None, list[str], list[str]]:
    """Validate one entry against its batch item. Returns (Entry|None, errors, unused_split_ids)."""
    data = dict(entry_data)
    data["regs_verbatim"] = item.get("raw_regs", "")      # inject authoritative source; don't trust the copy
    if "symbols" in item:                                 # authoritative synopsis symbols: store as provenance
        syms = list(item["symbols"])                      # AND derive entry-level tributaries.included from them
        data["source_symbols"] = syms
        tribs = dict(data.get("tributaries") or {})
        tribs["included"] = symbols_include_tributaries(syms)
        data["tributaries"] = tribs
    if item.get("entry_id"):                              # authoritative id/status — never trusted from the model
        data["entry_id"] = item["entry_id"]
    # WHO the entry is about is export-time knowledge, not a parsing result. Trusting the model's
    # copy cost us the name on 843 of 1,021 entries: it title-cased 777 of them, and on 66 it
    # rewrote the name outright — dropping the parenthetical that CARRIES THE REACH
    # ("THOMPSON RIVER (upstream of Kamloops Lake)" -> "Thompson River"; both MICHEL CREEK rows
    # collapsed to "Michel Creek" and became indistinguishable), substituting the registry's name
    # for the synopsis's ('"LINK" RIVER' -> "Marble River"), and "correcting" spellings the book
    # actually prints (MCDONNEL -> McDonell). Same class of bug as the id: inject, never trust.
    ident = dict(data.get("identity") or {})
    for field, key in (("name", "name"), ("display_name", "display_name"), ("region", "region")):
        if item.get(key) is not None:
            ident[field] = item[key]
    if item.get("row_mus") is not None:                    # the ROW's MUs, never the item's union:
        ident["mus"] = list(item["row_mus"])               # the row is printed under one MU heading
    data["identity"] = ident
    if item.get("item_id"):                               # registry ids this entry covers: the matched
        data["matched"] = [item["item_id"], *item.get("also_item_ids", [])]   # item + a combined
                                                          # override's other items (Chilliwack+Vedder)
    if item.get("no_registry"):
        data["registry_status"] = "no_registry"
        data["registry_note"] = item.get("registry_note", "")
    else:
        data["registry_status"] = "matched"
        data["registry_note"] = ""
    try:
        entry = Entry(**data)
    except Exception as e:                                 # noqa: BLE001
        return None, [f"schema: {e}"], []
    allowed = set(item.get("bindable_ids", []))
    by_item = {k: set(v) for k, v in (item.get("bindable_by_item") or {}).items()}
    split_errs = validate_entry_splits(entry, allowed, by_item)
    if split_errs:
        return None, split_errs, []
    return entry, [], unused_splits(entry, allowed)


def run(batch_path: str, candidate_path: str) -> int:
    items = load_batch_items(batch_path)
    ok = fail = 0
    for obj in _candidates(candidate_path):
        idx = obj.get("index")
        item = items.get(idx)
        if item is None:
            print(f"FAIL index={idx}: not in batch {batch_path}")
            fail += 1
            continue
        entry, errors, unused = validate_candidate(item, obj.get("entry", {}))
        if entry is None:
            print(f"FAIL index={idx} ({item.get('name','')}):")
            for e in errors:
                print(f"    - {e}")
            fail += 1
            continue
        ok += 1
        note = f"  (unused boundaries: {unused})" if unused else ""
        print(f"OK   index={idx} {entry.entry_id}: {len(entry.rules)} rule(s){note}")
    print(f"\n{ok} valid, {fail} failed")
    return 1 if fail else 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate a candidate parse against its batch file.")
    ap.add_argument("batch", help="batch_NNN.json (carries raw_regs + bindable_ids per item)")
    ap.add_argument("candidate", help="candidate response JSON ([{index, entry}] or a single entry)")
    args = ap.parse_args()
    sys.exit(run(args.batch, args.candidate))


if __name__ == "__main__":
    main()
