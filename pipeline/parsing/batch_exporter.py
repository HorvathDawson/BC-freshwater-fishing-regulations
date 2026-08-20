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
import re
from datetime import datetime
from pathlib import Path

from pipeline.matching.matcher import load_overrides, match_rows, parse_reg_mus, region_num
from pipeline.parsing.parse_context import (
    build_no_registry_context, build_parse_context, render_batch_prompt,
)
from pipeline.registry import default_registry_path, load_registry
from pipeline.parsing.rows import load_synopsis_rows


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (text or "").lower()).strip("_") or "row"


def compute_rows_digest(rows: list[dict]) -> str:
    """Stable digest over (water, raw_regs) so ingest can detect data drift since export."""
    h = hashlib.sha256()
    for r in rows:
        h.update((r.get("water", "") + "\x1f" + r.get("raw_regs", "") + "\x1e").encode("utf-8"))
    return h.hexdigest()


def default_work_dir() -> Path:
    # Own top-level dir: the parse work (batches/responses/reviews) is unrelated to the FWA graph
    # artifacts, so it no longer piggybacks under output/pipeline/graph. <project-root>/output/parse.
    return Path(__file__).resolve().parents[2] / "output" / "parse"


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


def _item_payload(index: int, row: dict, ctx) -> dict:
    """Batch payload for one item. `entry_id`, `registry_status`, and `registry_note` are injected
    into the Entry at ingest (authoritative — never trusted from the model), same as `raw_regs`."""
    return {
        "index": index,
        "entry_id": ctx.entry_id,
        "item_id": ctx.item_id or None,
        "name": ctx.name,
        "region": region_num(row),
        "mus": list(ctx.mus),
        "raw_regs": row.get("raw_regs", ""),
        "bindable_ids": sorted(ctx.bindable_ids),
        "no_registry": ctx.no_registry,
        "registry_note": ctx.registry_note,
    }


def export(rows, registry, out_dir: Path, batch_size: int, overrides, existing_ids, force: bool,
           skip_existing: bool = False) -> dict:
    """Export matchable rows into stable batches. The batch layout is a PURE FUNCTION of (rows,
    registry) — it must NOT depend on what has already been ingested, or a re-run would renumber the
    batches and desync them from responses/ (resume is keyed on batch id). `skip_existing` (opt-in,
    default off) drops rows already in EntryFiles; leave it off for a resumable run and let dispatch
    (response exists) + ingest (locked preserved) handle 'already done'."""
    batches_dir = out_dir / "batches"
    batches_dir.mkdir(parents=True, exist_ok=True)
    for stale in batches_dir.glob("batch_*"):
        stale.unlink()

    digest = compute_rows_digest(rows)
    matches = match_rows(rows, registry, overrides)

    pending: list[tuple[int, object, dict]] = []       # (index, ParseContext, row)
    unmatched: list[dict] = []                          # held-back report (also parsed as no_registry)
    no_registry_count = 0
    skipped_existing: list[int] = []
    excluded_empty: list[int] = []

    for m in matches:
        row = rows[m.index]
        if not row.get("raw_regs", "").strip():
            excluded_empty.append(m.index)             # nothing to split — a pointer/blank row
            continue
        if m.item_id is None:
            # No registry match: still parse the reg text into rules, flagged no_registry (the reg
            # content is captured for the curator even though no locators can be bound).
            unmatched.append({"index": m.index, "water": m.water, "status": m.status, "reason": m.reason})
            entry_id = f"noreg_{_slug(m.water)}_{m.index}"
            if skip_existing and entry_id in existing_ids and not force:
                skipped_existing.append(m.index)
                continue
            note = f"{m.status}: {m.reason}" if m.reason else m.status
            ctx = build_no_registry_context(
                entry_id=entry_id, name=m.water, raw_regs=row.get("raw_regs", ""), registry_note=note,
                region=region_num(row), mus=tuple(sorted(parse_reg_mus(row))), row_index=m.index)
            pending.append((m.index, ctx, row))
            no_registry_count += 1
            continue
        item = registry[m.item_id]
        entry_id = m.item_id
        if skip_existing and entry_id in existing_ids and not force:
            skipped_existing.append(m.index)
            continue
        ctx = build_parse_context(item, raw_regs=row.get("raw_regs", ""),
                                  entry_id=entry_id, region=region_num(row), row_index=m.index)
        pending.append((m.index, ctx, row))

    manifest_batches: list[dict] = []
    for b in range(0, len(pending), batch_size):
        chunk = pending[b:b + batch_size]
        bid = b // batch_size
        items = [_item_payload(idx, row, ctx) for (idx, ctx, row) in chunk]
        (batches_dir / f"batch_{bid:03d}.json").write_text(
            json.dumps({"batch": bid, "rows_digest": digest, "items": items}, ensure_ascii=False, indent=2),
            encoding="utf-8")
        (batches_dir / f"batch_{bid:03d}.prompt.txt").write_text(
            render_batch_prompt([ctx for (_, ctx, _) in chunk]), encoding="utf-8")
        manifest_batches.append({"id": bid, "count": len(chunk), "indices": [i for (i, _, _) in chunk]})

    manifest = {
        "created_at": datetime.now().isoformat(),
        "total_rows": len(rows),
        "rows_digest": digest,
        "batch_size": batch_size,
        "pending_count": len(pending),
        "no_registry_count": no_registry_count,
        "unmatched": unmatched,
        "skipped_existing": skipped_existing,
        "excluded_empty": excluded_empty,
        "batches": manifest_batches,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def main() -> None:
    ap = argparse.ArgumentParser(description="Export matched synopsis rows into agent-parse batches.")
    ap.add_argument("--batch-size", type=int, default=30,
                    help="rows per batch (default 30 — balances per-call prompt overhead against resume "
                    "granularity if a run dies mid-batch)")
    ap.add_argument("--registry", help="registry.json (default: build output).")
    ap.add_argument("--overrides", help="matcher overrides JSON.")
    ap.add_argument("--entries-dir", help="checked-in EntryFiles dir (resume skip).")
    ap.add_argument("--out-dir", help="working dir (default: <out>/parse).")
    ap.add_argument("--force", action="store_true", help="re-export rows already present in EntryFiles.")
    ap.add_argument("--skip-existing", action="store_true",
                    help="drop rows already in EntryFiles (CHANGES the batch layout — breaks resume of a "
                    "run in flight; only for a deliberate fresh export after curation)")
    args = ap.parse_args()

    rows = load_synopsis_rows()
    registry = load_registry(Path(args.registry) if args.registry else default_registry_path())
    default_ov = Path(__file__).resolve().parents[1] / "matching" / "overrides.json"
    overrides = load_overrides(args.overrides or (default_ov if default_ov.exists() else None))
    out_dir = Path(args.out_dir) if args.out_dir else default_work_dir()
    entries_dir = Path(args.entries_dir) if args.entries_dir else (Path(__file__).resolve().parent / "entries")
    existing = load_existing_entry_ids(entries_dir)

    manifest = export(rows, registry, out_dir, args.batch_size, overrides, existing, args.force,
                      skip_existing=args.skip_existing)
    print(f"Exported {manifest['pending_count']} rows into {len(manifest['batches'])} batch(es) -> {out_dir/'batches'}")
    print(f"  of those, no-registry (content-only, flagged): {manifest['no_registry_count']}")
    print(f"  held-back detail: {len(manifest['unmatched'])}  skipped-existing: {len(manifest['skipped_existing'])}  "
          f"empty-regs: {len(manifest['excluded_empty'])}")
    print(f"  manifest: {out_dir/'manifest.json'}")


if __name__ == "__main__":
    main()
