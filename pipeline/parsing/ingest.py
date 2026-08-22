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
from collections import defaultdict
from pathlib import Path

from pipeline.parsing.entry_models import Entry, EntryFile
from pipeline.parsing.validate import load_batch_items, validate_candidate


def _load_all_batch_items(batches_dir: Path) -> dict[int, dict]:
    items: dict[int, dict] = {}
    for p in sorted(batches_dir.glob("batch_*.json")):
        items.update(load_batch_items(p))
    return items


def load_reviews(reviews_dir: Path, batches_dir: Path) -> dict[int, dict]:
    """Map row index -> durable parse_review dict from the agent reviewer's per-batch files
    (`reviews/batch_NNN.review.json`). Every row in a reviewed batch gets a record: `changes_requested`
    with its issues, or `pass` when the reviewer flagged nothing for it. Absent = entry never reviewed."""
    out: dict[int, dict] = {}
    if not reviews_dir.exists():
        return out
    for rp in sorted(reviews_dir.glob("batch_*.review.json")):
        try:
            obj = json.loads(rp.read_text(encoding="utf-8"))
        except Exception:                                 # noqa: BLE001 — skip a garbage review file
            continue
        bid = rp.name.split(".", 1)[0]                    # 'batch_000'
        batch_path = batches_dir / f"{bid}.json"
        if not batch_path.exists():
            continue
        indices = load_batch_items(batch_path).keys()
        by_idx: dict[int, list] = defaultdict(list)
        for iss in obj.get("issues", []):
            if iss.get("index") is not None:
                by_idx[iss["index"]].append(
                    {"severity": iss.get("severity", "low"), "problem": iss.get("problem", ""),
                     "fix": iss.get("fix", "")})
        for idx in indices:
            issues = by_idx.get(idx, [])
            out[idx] = {
                "verdict": "changes_requested" if issues else "pass",
                "model": obj.get("model", ""),
                "reviewed_at": obj.get("reviewed_at", ""),
                "issues": issues,
            }
    return out



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


def ingest(response_texts: list[str], batch_items: dict[int, dict],
           reviews: dict[int, dict] | None = None) -> tuple[dict[int, Entry], dict]:
    """Validate all responses against their batch items. Returns (accepted {index: Entry}, report).
    `reviews` (index -> parse_review) is injected onto each entry as durable agent-review state."""
    reviews = reviews or {}
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
            entry_data = obj.get("entry", {})
            if idx in reviews:                            # persist the agent review onto the entry
                entry_data = {**entry_data, "parse_review": reviews[idx]}
            entry, errors, unused = validate_candidate(item, entry_data)
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
    ap.add_argument("--reviews-dir", help="dir with batch_*.review.json (default: <out>/parse/reviews)")
    ap.add_argument("--entries-dir", help="output EntryFiles dir (default: pipeline/parsing/entries)")
    ap.add_argument("--dry-run", action="store_true", help="validate + report only; write nothing")
    args = ap.parse_args()

    if args.batches_dir:
        batches_dir = Path(args.batches_dir)
    else:
        from pipeline.parsing.batch_exporter import default_work_dir
        batches_dir = default_work_dir() / "batches"
    reviews_dir = Path(args.reviews_dir) if args.reviews_dir else (batches_dir.parent / "reviews")
    entries_dir = Path(args.entries_dir) if args.entries_dir else (Path(__file__).resolve().parent / "entries")

    batch_items = _load_all_batch_items(batches_dir)
    reviews = load_reviews(reviews_dir, batches_dir)
    texts = [Path(p).read_text(encoding="utf-8") for p in args.responses]
    accepted, report = ingest(texts, batch_items, reviews)

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
