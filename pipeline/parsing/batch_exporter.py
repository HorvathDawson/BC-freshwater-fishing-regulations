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
    return set(load_existing_entry_regs(entries_dir))


def load_existing_entry_regs(entries_dir: Path) -> dict[str, str]:
    """entry_id -> regs_verbatim for every entry already in the checked-in EntryFiles. Used by
    ``--only-changed`` to re-export only the entries whose combined regs differ from what was parsed."""
    out: dict[str, str] = {}
    if not entries_dir.exists():
        return out
    for p in entries_dir.glob("region-*.json"):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        for e in data.get("entries", []):
            if e.get("entry_id"):
                out[e["entry_id"]] = e.get("regs_verbatim", "")
    return out


def _item_payload(index: int, ctx) -> dict:
    """Batch payload for one synopsis row (one entry). `entry_id`, `registry_status`, and `registry_note`
    are injected into the Entry at ingest (authoritative — never trusted from the model), same as
    `raw_regs`."""
    return {
        "index": index,
        "entry_id": ctx.entry_id,
        "item_id": ctx.item_id or None,
        "name": ctx.name,
        "region": ctx.region,
        "mus": list(ctx.mus),
        "raw_regs": ctx.raw_regs,
        "bindable_ids": sorted(ctx.bindable_ids),
        "no_registry": ctx.no_registry,
        "registry_note": ctx.registry_note,
        "symbols": list(ctx.symbols),
    }


def export(rows, registry, out_dir: Path, batch_size: int, overrides, existing_ids, force: bool,
           skip_existing: bool = False, only_changed: bool = False,
           existing_regs: dict | None = None) -> dict:
    """Export matchable rows into stable batches, ONE batch item per synopsis ROW (each row is its own
    entry). Rows that share a registry item get a reach-qualified `entry_id` (`item_id#<reach-slug>`) so
    they no longer collide and overwrite at ingest (the bug); single-row waterbodies keep
    `entry_id == item_id`. Combining reach entries into one waterbody is a later aggregation step, so the
    entries stay faithful to the synopsis format.

    The batch layout is a PURE FUNCTION of (rows, registry): it must NOT depend on what has been ingested
    or a re-run would renumber batches and desync resume. `skip_existing` (opt-in) drops entries already
    in EntryFiles. `only_changed` (for a targeted re-parse that KEEPS existing progress) drops entries
    whose regs are byte-identical to the already-parsed entry, so only the collisions (new per-row ids) +
    genuinely-new/changed rows are exported."""
    batches_dir = out_dir / "batches"
    batches_dir.mkdir(parents=True, exist_ok=True)
    for stale in batches_dir.glob("batch_*"):
        stale.unlink()

    digest = compute_rows_digest(rows)
    matches = match_rows(rows, registry, overrides)
    existing_regs = existing_regs or {}

    # A synopsis row that shares its registry item with another row (reach splits like "Elk River
    # (upstream/downstream of Elko Dam)", multi-part lakes, a mainstem + its "X's TRIBUTARIES" row)
    # becomes its OWN entry with a reach-qualified id + name, instead of colliding on the item id and
    # overwriting at ingest (the bug). Single-row waterbodies keep entry_id == item_id (no churn).
    from collections import Counter
    item_rowcount = Counter(m.item_id for m in matches if m.item_id)

    pending: list[tuple[int, object]] = []              # (row_index, ParseContext)
    unmatched: list[dict] = []
    excluded_empty: list[int] = []
    skipped_existing: list[int] = []
    no_registry_count = 0
    used_ids: dict[str, int] = {}                        # entry_id -> dup counter (distinct regs, same slug)
    seen_content: set[tuple[str, str]] = set()           # (item_id, raw_regs) — drop exact-duplicate rows

    for m in matches:
        row = rows[m.index]
        raw = row.get("raw_regs", "")
        if not raw.strip():
            excluded_empty.append(m.index)              # nothing to split — a pointer/blank row
            continue
        if m.item_id is None:
            unmatched.append({"index": m.index, "water": m.water, "status": m.status, "reason": m.reason})
            eid = f"noreg_{_slug(m.water)}_{m.index}"
            note = f"{m.status}: {m.reason}" if m.reason else m.status
            ctx = build_no_registry_context(
                entry_id=eid, name=m.water, raw_regs=raw, registry_note=note,
                region=region_num(row), mus=tuple(sorted(parse_reg_mus(row))), row_index=m.index,
                symbols=tuple(row.get("symbols", [])))
            is_noreg = True
        else:
            if item_rowcount[m.item_id] > 1:            # multi-row waterbody -> per-row entry
                key = (m.item_id, raw)
                if key in seen_content:                 # exact-duplicate listing -> one entry
                    skipped_existing.append(m.index)
                    continue
                seen_content.add(key)
                eid = f"{m.item_id}#{_slug(m.water)[:48].strip('_')}"
                if eid in used_ids:                     # same slug (or truncated), different regs -> disambiguate
                    used_ids[eid] += 1
                    eid = f"{eid}_{used_ids[eid]}"
                else:
                    used_ids[eid] = 0
                name = m.water                          # keep the reach-qualified name
            else:
                eid = m.item_id
                name = ""                               # -> item.name (unchanged single-row entries)
            ctx = build_parse_context(registry[m.item_id], raw_regs=raw, entry_id=eid,
                                      region=region_num(row), row_index=m.index, name=name,
                                      symbols=tuple(row.get("symbols", [])))
            is_noreg = False

        if skip_existing and eid in existing_ids and not force:
            skipped_existing.append(m.index)
            continue
        if only_changed and not force and existing_regs.get(eid, None) == raw:
            skipped_existing.append(m.index)            # already parsed, identical regs -> keep progress
            continue
        pending.append((m.index, ctx))
        if is_noreg:
            no_registry_count += 1

    manifest_batches: list[dict] = []
    for b in range(0, len(pending), batch_size):
        chunk = pending[b:b + batch_size]
        bid = b // batch_size
        items = [_item_payload(idx, ctx) for (idx, ctx) in chunk]
        (batches_dir / f"batch_{bid:03d}.json").write_text(
            json.dumps({"batch": bid, "rows_digest": digest, "items": items}, ensure_ascii=False, indent=2),
            encoding="utf-8")
        (batches_dir / f"batch_{bid:03d}.prompt.txt").write_text(
            render_batch_prompt([ctx for (_, ctx) in chunk]), encoding="utf-8")
        manifest_batches.append({"id": bid, "count": len(chunk), "indices": [i for (i, _) in chunk]})

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
    ap.add_argument("--only-changed", action="store_true",
                    help="export ONLY entries whose combined regs differ from the already-parsed entry "
                    "(i.e. multi-row collisions + genuinely new/changed rows). The minimal re-parse set "
                    "after the per-row grouping fix — pair with `dispatch --force` on a fresh --out-dir.")
    args = ap.parse_args()

    rows = load_synopsis_rows()
    registry = load_registry(Path(args.registry) if args.registry else default_registry_path())
    default_ov = Path(__file__).resolve().parents[1] / "matching" / "overrides.json"
    overrides = load_overrides(args.overrides or (default_ov if default_ov.exists() else None))
    out_dir = Path(args.out_dir) if args.out_dir else default_work_dir()
    entries_dir = Path(args.entries_dir) if args.entries_dir else (Path(__file__).resolve().parent / "entries")
    existing_regs = load_existing_entry_regs(entries_dir)
    existing = set(existing_regs)

    manifest = export(rows, registry, out_dir, args.batch_size, overrides, existing, args.force,
                      skip_existing=args.skip_existing, only_changed=args.only_changed,
                      existing_regs=existing_regs)
    print(f"Exported {manifest['pending_count']} rows into {len(manifest['batches'])} batch(es) -> {out_dir/'batches'}")
    print(f"  of those, no-registry (content-only, flagged): {manifest['no_registry_count']}")
    print(f"  held-back rows also parsed (no-registry): {len(manifest['unmatched'])}  "
          f"empty-regs skipped: {len(manifest['excluded_empty'])}")
    if manifest["skipped_existing"]:
        why = "unchanged (already parsed)" if args.only_changed else "already in EntryFiles"
        print(f"  dropped {len(manifest['skipped_existing'])} row(s) — {why}")
    print("  (already-parsed batches are skipped at the parse step, not here)")
    print(f"  manifest: {out_dir/'manifest.json'}")


if __name__ == "__main__":
    main()
