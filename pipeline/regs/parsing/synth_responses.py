"""Synthesize parse "responses" from the CURRENT EntryFiles, so the review pass can QA every existing
entry WITHOUT re-parsing it.

The agent review step reviews a batch's response (the parsed {index, entry} objects) against the reg
text. Normally those come from a fresh parse; but to review the entries we ALREADY have (fill each
entry's `parse_review`), we can reconstruct the responses from the EntryFiles: for every batch item
(index -> entry_id) emit `{index, entry}` using the entry on disk. Then:

    # 1. export ALL entries into batches (a full export — no --skip-existing/--only-changed)
    PYTHONPATH=$PWD .venv/bin/python -m pipeline.regs.parsing.batch_exporter --registry output/v2/full/registry.json
    # 2. rebuild responses from the entries on disk (this script) — no credits
    PYTHONPATH=$PWD .venv/bin/python -m pipeline.regs.parsing.synth_responses
    # 3. REVIEW every batch (HUMAN — spends haiku credits) then ingest to bake parse_review
    PYTHONPATH=$PWD .venv/bin/python -m pipeline.regs.parsing.dispatch --review --review-model haiku
    PYTHONPATH=$PWD .venv/bin/python -m pipeline.regs.parsing.ingest output/parse/responses/*.json

Only the review step spends credits (haiku), and ingest preserves locked entries.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.regs.parsing.ingest import _load_all_batch_items, load_batch_items
from pipeline.regs.parsing import io as _io


def _entries_by_id(entries_dir: Path) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for p in sorted(entries_dir.glob("region-*.json")):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        for e in data.get("entries", []):
            if e.get("entry_id"):
                out[e["entry_id"]] = e
    return out


def synth(batches_dir: Path, responses_dir: Path, entries_dir: Path) -> dict:
    by_id = _entries_by_id(entries_dir)
    responses_dir.mkdir(parents=True, exist_ok=True)
    written, missing = 0, []
    for bf in sorted(batches_dir.glob("batch_*.json")):
        items = load_batch_items(bf)                     # {index: item}
        objs = []
        for idx, item in sorted(items.items()):
            e = by_id.get(item.get("entry_id"))
            if e is None:
                missing.append(item.get("entry_id"))
                continue
            objs.append({"index": idx, "entry": e})
        (responses_dir / bf.name).write_text(json.dumps(objs, ensure_ascii=False, indent=2), encoding="utf-8")
        written += 1
    return {"batches": written, "missing": missing}


def main() -> None:
    ap = argparse.ArgumentParser(description="Rebuild parse responses from the current EntryFiles (for a review-only pass).")
    ap.add_argument("--out-dir", help="parse work dir (default: <root>/output/parse).")
    ap.add_argument("--entries-dir", help="EntryFiles dir (default: pipeline/regs/parsing/entries).")
    args = ap.parse_args()

    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        from pipeline.regs.parsing.batch_exporter import default_work_dir
        out_dir = default_work_dir()
    entries_dir = Path(args.entries_dir) if args.entries_dir else _io.entries_dir()

    rep = synth(out_dir / "batches", out_dir / "responses", entries_dir)
    print(f"synthesized responses for {rep['batches']} batch(es) from {entries_dir}")
    if rep["missing"]:
        uniq = sorted(set(rep["missing"]))
        print(f"  {len(rep['missing'])} batch item(s) had no matching entry on disk (skipped): "
              f"{uniq[:10]}{' …' if len(uniq) > 10 else ''}")


if __name__ == "__main__":
    main()
