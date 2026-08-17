"""Ingest agent responses into the checked-in EntryFiles — the validation gate.

Takes the JSON a parsing agent produced for one or more batches, validates every entry through the
SAME `Entry` model + split-id check the parser must satisfy (with `regs_verbatim` injected from the
batch, never trusted from the model), and writes the successes into `pipeline/parsing/entries/region-N.json`.
Nothing is applied unless it validates; failures are reported and left out. A `locked` entry already on
disk is NEVER overwritten (that's the human-curation freeze — the full merge tool comes later).

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.ingest responses/batch_000.json --dry-run
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.ingest responses/*.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.parsing.entry_models import Entry, EntryFile
from pipeline.parsing.validate import load_batch_items, validate_candidate


def _load_all_batch_items(batches_dir: Path) -> dict[int, dict]:
    items: dict[int, dict] = {}
    for p in sorted(batches_dir.glob("batch_*.json")):
        items.update(load_batch_items(p))
    return items


def _parse_response(text: str) -> list[dict]:
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        stripped = "\n".join(lines).strip()
    data = json.loads(stripped)
    if isinstance(data, dict) and "entry" in data:
        return [data]
    if not isinstance(data, list):
        raise ValueError(f"expected a JSON array of {{index, entry}}, got {type(data).__name__}")
    return data


def ingest(response_texts: list[str], batch_items: dict[int, dict]) -> tuple[dict[int, Entry], dict]:
    """Validate all responses against their batch items. Returns (accepted {index: Entry}, report)."""
    accepted: dict[int, Entry] = {}
    report: dict = {"accepted": [], "failed": [], "duplicates": [], "unknown_index": [], "unused": {}}
    seen: set[int] = set()
    for text in response_texts:
        for obj in _parse_response(text):
            idx = obj.get("index")
            item = batch_items.get(idx)
            if item is None:
                report["unknown_index"].append(idx)
                continue
            if idx in seen:
                report["duplicates"].append(idx)
                continue
            seen.add(idx)
            entry, errors, unused = validate_candidate(item, obj.get("entry", {}))
            if entry is None:
                report["failed"].append({"index": idx, "errors": errors})
                continue
            accepted[idx] = entry
            report["accepted"].append(idx)
            if unused:
                report["unused"][idx] = unused
    return accepted, report


def _region_of(index: int, batch_items: dict[int, dict]) -> str:
    return str(batch_items.get(index, {}).get("region") or "unknown")


def write_entry_files(accepted: dict[int, Entry], batch_items: dict[int, dict], entries_dir: Path) -> dict:
    """Merge accepted entries into per-region EntryFiles, preserving any `locked` entry on disk.
    Returns a per-region write report."""
    by_region: dict[str, dict[int, Entry]] = {}
    for idx, entry in accepted.items():
        by_region.setdefault(_region_of(idx, batch_items), {})[idx] = entry

    written: dict[str, dict] = {}
    entries_dir.mkdir(parents=True, exist_ok=True)
    for region, entries in by_region.items():
        path = entries_dir / f"region-{region}.json"
        existing: dict[str, Entry] = {}
        if path.exists():
            ef = EntryFile(**json.loads(path.read_text(encoding="utf-8")))
            existing = {e.entry_id: e for e in ef.entries}
        kept_locked = 0
        for entry in entries.values():
            prior = existing.get(entry.entry_id)
            if prior is not None and prior.locked:
                kept_locked += 1                       # never overwrite a human-frozen entry
                continue
            existing[entry.entry_id] = entry
        ef = EntryFile(region=region, entries=sorted(existing.values(), key=lambda e: e.entry_id))
        path.write_text(ef.model_dump_json(indent=2), encoding="utf-8")
        written[region] = {"path": str(path), "entries": len(ef.entries), "kept_locked": kept_locked}
    return written


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest agent responses into checked-in EntryFiles.")
    ap.add_argument("responses", nargs="+", help="response JSON file(s)")
    ap.add_argument("--batches-dir", help="dir with batch_*.json (default: <out>/parse/batches)")
    ap.add_argument("--entries-dir", help="output EntryFiles dir (default: pipeline/parsing/entries)")
    ap.add_argument("--dry-run", action="store_true", help="validate + report only; write nothing")
    args = ap.parse_args()

    if args.batches_dir:
        batches_dir = Path(args.batches_dir)
    else:
        from pipeline.parsing.batch_exporter import default_work_dir
        batches_dir = default_work_dir() / "batches"
    entries_dir = Path(args.entries_dir) if args.entries_dir else (Path(__file__).resolve().parent / "entries")

    batch_items = _load_all_batch_items(batches_dir)
    texts = [Path(p).read_text(encoding="utf-8") for p in args.responses]
    accepted, report = ingest(texts, batch_items)

    print("Ingest summary:")
    print(f"  accepted : {len(report['accepted'])}")
    print(f"  failed   : {len(report['failed'])}")
    if report["duplicates"]:
        print(f"  DUPLICATE index: {report['duplicates']}")
    if report["unknown_index"]:
        print(f"  unknown index  : {report['unknown_index']}")
    for f in report["failed"]:
        print(f"    FAILED {f['index']}: {f['errors']}")
    for idx, unused in report["unused"].items():
        print(f"    ADVISORY {idx}: unused boundaries {unused}")

    if args.dry_run:
        print("Dry run — nothing written.")
        return
    if not accepted:
        print("No valid entries to write.")
        return
    written = write_entry_files(accepted, batch_items, entries_dir)
    for region, info in sorted(written.items()):
        locked_note = f" (kept {info['kept_locked']} locked)" if info["kept_locked"] else ""
        print(f"  region {region}: {info['entries']} entries -> {info['path']}{locked_note}")


if __name__ == "__main__":
    main()
