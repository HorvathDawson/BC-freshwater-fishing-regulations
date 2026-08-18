"""Export matched synopsis rows into self-contained batch prompts for a coding-agent (Opus) parse.

For each pending row: match it to a registry item, build the constrained parse context (bindable
boundaries + reachable areas + species menu), and write a batch prompt an agent can parse directly.
Unmatched/ambiguous rows are reported and EXCLUDED (hand-curated later) — never guessed.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.batch_exporter --batch-size 40

Writes under the working dir (default: <out>/parse/):
  batches/batch_NNN.json    — {batch, rows_digest, items:[{index,item_id,name,region,raw_regs,bindable_ids}]}
  batches/batch_NNN.prompt.txt — the self-contained prompt (rules + examples + each item's menu + envelope)
  manifest.json             — batch layout + rows_digest + unmatched report
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path

from pipeline.matching.matcher import load_overrides, match_rows, region_num
from pipeline.parsing.parse_context import build_parse_context, render_batch_prompt
from pipeline.registry import default_registry_path, load_registry
from pipeline.parsing.rows import load_synopsis_rows


def compute_rows_digest(rows: list[dict]) -> str:
    """Stable digest over (water, raw_regs) so ingest can detect data drift since export."""
    h = hashlib.sha256()
    for r in rows:
        h.update((r.get("water", "") + "\x1f" + r.get("raw_regs", "") + "\x1e").encode("utf-8"))
    return h.hexdigest()


def default_work_dir() -> Path:
    from project_config import get_config
    return Path(get_config().fwa_output_dir) / "parse"


def load_existing_entry_ids(entries_dir: Path) -> set[str]:
    """entry_ids already present in checked-in EntryFiles (resume: don't re-export them)."""
    ids: set[str] = set()
    if not entries_dir.exists():
        return ids
    for p in entries_dir.glob("region-*.json"):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        for e in data.get("entries", []):
            if e.get("entry_id"):
                ids.add(e["entry_id"])
    return ids


def _item_payload(index: int, row: dict, item, ctx) -> dict:
    return {
        "index": index,
        "item_id": item.id,
        "name": item.name,
        "region": region_num(row),
        "raw_regs": row.get("raw_regs", ""),
        "bindable_ids": sorted(ctx.bindable_ids),
    }


def export(rows, registry, out_dir: Path, batch_size: int, overrides, existing_ids, force: bool) -> dict:
    batches_dir = out_dir / "batches"
    batches_dir.mkdir(parents=True, exist_ok=True)
    for stale in batches_dir.glob("batch_*"):
        stale.unlink()

    digest = compute_rows_digest(rows)
    matches = match_rows(rows, registry, overrides)

    pending: list[tuple[int, object]] = []       # (index, ParseContext)
    unmatched: list[dict] = []
    skipped_existing: list[int] = []
    excluded_empty: list[int] = []

    for m in matches:
        row = rows[m.index]
        if m.item_id is None:
            unmatched.append({"index": m.index, "water": m.water, "status": m.status, "reason": m.reason})
            continue
        if not row.get("raw_regs", "").strip():
            excluded_empty.append(m.index)
            continue
        item = registry[m.item_id]
        entry_id = m.item_id
        if entry_id in existing_ids and not force:
            skipped_existing.append(m.index)
            continue
        ctx = build_parse_context(item, raw_regs=row.get("raw_regs", ""),
                                  entry_id=entry_id, region=region_num(row), row_index=m.index)
        pending.append((m.index, ctx, item, row))

    manifest_batches: list[dict] = []
    for b in range(0, len(pending), batch_size):
        chunk = pending[b:b + batch_size]
        bid = b // batch_size
        items = [_item_payload(idx, row, item, ctx) for (idx, ctx, item, row) in chunk]
        (batches_dir / f"batch_{bid:03d}.json").write_text(
            json.dumps({"batch": bid, "rows_digest": digest, "items": items}, ensure_ascii=False, indent=2),
            encoding="utf-8")
        (batches_dir / f"batch_{bid:03d}.prompt.txt").write_text(
            render_batch_prompt([ctx for (_, ctx, _, _) in chunk]), encoding="utf-8")
        manifest_batches.append({"id": bid, "count": len(chunk), "indices": [i for (i, _, _, _) in chunk]})

    manifest = {
        "created_at": datetime.now().isoformat(),
        "total_rows": len(rows),
        "rows_digest": digest,
        "batch_size": batch_size,
        "pending_count": len(pending),
        "unmatched": unmatched,
        "skipped_existing": skipped_existing,
        "excluded_empty": excluded_empty,
        "batches": manifest_batches,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def main() -> None:
    ap = argparse.ArgumentParser(description="Export matched synopsis rows into agent-parse batches.")
    ap.add_argument("--batch-size", type=int, default=15,
                    help="rows per batch (default 15 — smaller = finer resume granularity if a run dies mid-batch)")
    ap.add_argument("--registry", help="registry.json (default: build output).")
    ap.add_argument("--overrides", help="matcher overrides JSON.")
    ap.add_argument("--entries-dir", help="checked-in EntryFiles dir (resume skip).")
    ap.add_argument("--out-dir", help="working dir (default: <out>/parse).")
    ap.add_argument("--force", action="store_true", help="re-export rows already present in EntryFiles.")
    args = ap.parse_args()

    rows = load_synopsis_rows()
    registry = load_registry(Path(args.registry) if args.registry else default_registry_path())
    default_ov = Path(__file__).resolve().parents[1] / "matching" / "overrides.json"
    overrides = load_overrides(args.overrides or (default_ov if default_ov.exists() else None))
    out_dir = Path(args.out_dir) if args.out_dir else default_work_dir()
    entries_dir = Path(args.entries_dir) if args.entries_dir else (Path(__file__).resolve().parent / "entries")
    existing = load_existing_entry_ids(entries_dir)

    manifest = export(rows, registry, out_dir, args.batch_size, overrides, existing, args.force)
    print(f"Exported {manifest['pending_count']} rows into {len(manifest['batches'])} batch(es) -> {out_dir/'batches'}")
    print(f"  unmatched: {len(manifest['unmatched'])}  skipped-existing: {len(manifest['skipped_existing'])}  "
          f"empty-regs: {len(manifest['excluded_empty'])}")
    print(f"  manifest: {out_dir/'manifest.json'}")


if __name__ == "__main__":
    main()
