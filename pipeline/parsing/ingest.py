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

from pipeline.parsing import io
from pipeline.parsing.entry_models import Entry, EntryFile
from pipeline.parsing.validate import load_batch_items, validate_candidate

_load_all_batch_items = io.load_all_batch_items          # shared helper (io is the single home)


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



_parse_response = io.parse_response                      # shared helper (io is the single home)


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


def write_entry_files(accepted: dict[int, Entry], batch_items: dict[int, dict], entries_dir: Path,
                      *, review_only_on_locked: bool = False) -> dict:
    """Merge accepted entries into per-region EntryFiles (atomic, via `io.write_entryfile`), preserving
    any `locked` entry on disk. Returns a per-region write report.

    `review_only_on_locked` (a review pass): a locked entry's curated content is still NOT overwritten,
    but its `parse_review` IS refreshed from the incoming entry — so "review everything" can stamp a
    verdict on locked entries too without disturbing the human's edits."""
    by_region: dict[str, dict[int, Entry]] = {}
    for idx, entry in accepted.items():
        by_region.setdefault(_region_of(idx, batch_items), {})[idx] = entry

    written: dict[str, dict] = {}
    entries_dir.mkdir(parents=True, exist_ok=True)
    for region, entries in by_region.items():
        path = entries_dir / f"region-{region}.json"
        existing: dict[str, Entry] = {}
        if path.exists():
            existing = {e.entry_id: e for e in EntryFile(**json.loads(path.read_text(encoding="utf-8"))).entries}
        kept_locked = 0
        for entry in entries.values():
            prior = existing.get(entry.entry_id)
            if prior is not None and prior.locked:
                if review_only_on_locked:              # refresh ONLY the review verdict on a locked entry
                    existing[entry.entry_id] = prior.model_copy(update={"parse_review": entry.parse_review})
                kept_locked += 1                       # never overwrite a human-frozen entry's content
                continue
            existing[entry.entry_id] = entry
        io.write_entryfile(path, region, existing.values())
        written[region] = {"path": str(path), "entries": len(existing), "kept_locked": kept_locked}
    return written


def apply_reviews(reviews_dir: Path, batches_dir: Path, entries_dir: Path) -> dict:
    """Write ONLY each entry's `parse_review` from a completed review pass — no content change, so it is
    safe on locked/curated entries (unlike a full re-ingest, which would re-derive fields). Maps a
    review's row index -> entry_id via the batch items, then stamps the verdict onto the matching entry.
    Returns {updated, requested, missing}."""
    reviews = load_reviews(reviews_dir, batches_dir)          # index -> parse_review dict
    if not reviews:
        return {"updated": 0, "requested": 0, "missing": 0}
    items = _load_all_batch_items(batches_dir)                # index -> batch item (for entry_id)
    pr_by_id: dict[str, dict] = {}
    for idx, pr in reviews.items():
        eid = (items.get(idx) or {}).get("entry_id")
        if eid:
            pr_by_id[eid] = pr
    matched: set[str] = set()
    for p in sorted(Path(entries_dir).glob("region-*.json")):
        region = p.stem.split("region-")[1]
        by_id = io.read_entryfile(p)
        changed = False
        for eid, e in by_id.items():
            if eid in pr_by_id:
                e["parse_review"] = pr_by_id[eid]
                matched.add(eid)
                changed = True
        if changed:
            io.write_entryfile(p, region, by_id.values())
    return {"updated": len(matched), "requested": len(pr_by_id), "missing": len(set(pr_by_id) - matched)}


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest agent responses into checked-in EntryFiles.")
    ap.add_argument("responses", nargs="*", help="response JSON file(s) (omit with --apply-reviews)")
    ap.add_argument("--apply-reviews", action="store_true",
                    help="don't ingest responses — just stamp each entry's parse_review from the review "
                    "pass (reviews/*.review.json). Content untouched; safe on locked entries.")
    ap.add_argument("--batches-dir", help="dir with batch_*.json (default: <out>/parse/batches)")
    ap.add_argument("--reviews-dir", help="dir with batch_*.review.json (default: <out>/parse/reviews)")
    ap.add_argument("--entries-dir", help="output EntryFiles dir (default: pipeline/parsing/entries)")
    ap.add_argument("--dry-run", action="store_true", help="validate + report only; write nothing")
    ap.add_argument("--review-only-locked", action="store_true",
                    help="a review pass: refresh ONLY parse_review on locked entries (don't overwrite "
                    "their curated content). Use when ingesting a review-only re-parse of all entries.")
    args = ap.parse_args()

    if args.batches_dir:
        batches_dir = Path(args.batches_dir)
    else:
        from pipeline.parsing.batch_exporter import default_work_dir
        batches_dir = default_work_dir() / "batches"
    reviews_dir = Path(args.reviews_dir) if args.reviews_dir else (batches_dir.parent / "reviews")
    entries_dir = Path(args.entries_dir) if args.entries_dir else (Path(__file__).resolve().parent / "entries")

    if args.apply_reviews:
        rep = apply_reviews(reviews_dir, batches_dir, entries_dir)
        print(f"Applied parse_review to {rep['updated']}/{rep['requested']} entr(ies)"
              + (f"  ({rep['missing']} review(s) had no matching entry)" if rep["missing"] else ""))
        return

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
    written = write_entry_files(accepted, batch_items, entries_dir,
                                review_only_on_locked=args.review_only_locked)
    for region, info in sorted(written.items()):
        locked_note = f" (kept {info['kept_locked']} locked)" if info["kept_locked"] else ""
        print(f"  region {region}: {info['entries']} entries -> {info['path']}{locked_note}")


if __name__ == "__main__":
    main()
