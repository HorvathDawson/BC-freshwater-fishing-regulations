"""Export matched synopsis rows into self-contained batch prompts for a coding-agent (Opus) parse.

For each pending row: match it to a registry item, build the constrained parse context (bindable
boundaries + reachable areas + species menu), and write a batch prompt an agent can parse directly.
Unmatched/ambiguous rows are reported and EXCLUDED (hand-curated later) — never guessed.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.batch_exporter --batch-size 40

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

from pipeline.regs.parsing import io
from pipeline.regs.matching.matcher import load_overrides, match_rows, parse_reg_mus, region_num
from pipeline.regs.parsing.parse_context import (
    build_no_registry_context, build_parse_context, render_batch_prompt,
)
from pipeline.atlas.registry import default_registry_path, load_registry
from pipeline.regs.parsing.rows import load_synopsis_rows
from pipeline.regs.parsing import io as _io
from pipeline.common.curated import CURATED, SOURCE


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (text or "").lower()).strip("_") or "row"


def compute_rows_digest(rows: list[dict]) -> str:
    """Stable digest over (water, raw_regs) so ingest can detect data drift since export."""
    h = hashlib.sha256()
    for r in rows:
        h.update((r.get("water", "") + "\x1f" + r.get("raw_regs", "") + "\x1e").encode("utf-8"))
    return h.hexdigest()


# Shared helpers now live in io (the single home); kept as names here for existing importers.
default_work_dir = io.default_work_dir
load_existing_entry_ids = io.load_existing_entry_ids
load_existing_entry_regs = io.load_existing_entry_regs


def _row_entry_id(row: dict, m) -> str:
    """`r{region}:{name}@{mus}` — derived from the SYNOPSIS ROW ALONE.

    The id used to be the matched registry item (`gnis:4927`), or that item plus a reach
    slug when several rows shared it (`gnis:4927#…`), or `noreg_…` when nothing matched.
    All three encode *match state*, so an entry's id moved whenever matching moved — a
    new override, a registry rebuild, a second row starting to match the same item. That
    is what left 42 entries under ids the exporter no longer produced, holding real
    regulations (Trout Lake's tributaries among them) that a prune would have deleted.

    A synopsis row's region, name and MUs are what the BOOK says; they do not change when
    we rebind it. Verified unique across all 1,393 rows with no suffix needed.
    """
    from pipeline.regs.matching.matcher import parse_reg_mus, region_num

    mus = "+".join(sorted(parse_reg_mus(row)))
    base = f"r{region_num(row) or '?'}:{_slug(m.water)[:60].strip('_')}"
    return f"{base}@{mus}" if mus else base


def _item_payload(index: int, ctx) -> dict:
    """Batch payload for one synopsis row (one entry). `entry_id`, `registry_status`, and `registry_note`
    are injected into the Entry at ingest (authoritative — never trusted from the model), same as
    `raw_regs`."""
    return {
        "index": index,
        "entry_id": ctx.entry_id,
        "item_id": ctx.item_id or None,
        "also_item_ids": list(ctx.also_item_ids),      # combined override -> extra registry items
        "name": ctx.name,
        "display_name": ctx.display_name,
        "region": ctx.region,
        "mus": list(ctx.mus),                             # the ITEM's MUs — parser orientation
        "row_mus": list(ctx.row_mus),                     # the ROW's MUs — what identity.mus must be
        "raw_regs": ctx.raw_regs,
        "bindable_ids": sorted(ctx.bindable_ids),
        "boundaries": [list(b) for b in ctx.boundaries],   # (id,label,kind) — the review prompt's menu
        "bindable_by_item": {i: list(ids) for i, ids in ctx.boundaries_by_item},  # for item_id scoping
        "no_registry": ctx.no_registry,
        "registry_note": ctx.registry_note,
        "symbols": list(ctx.symbols),
    }


def export(rows, registry, out_dir: Path, batch_size: int, overrides, existing_ids, force: bool,
           skip_existing: bool = False, only_changed: bool = False,
           existing_regs: dict | None = None, only_ids: set[str] | None = None,
           review_hints: dict[str, list] | None = None) -> dict:
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

    # EVERY synopsis row becomes its own entry. Rows that share a registry item — reach splits
    # ("Elk River upstream/downstream of Elko Dam"), a mainstem plus its "X's TRIBUTARIES" row, the
    # same water printed in two regional books, a lake listed under both its names — are kept apart
    # by `_row_entry_id`, which keys on the ROW (region + verbatim name + MUs), never on the item.
    # Measured: 1,393 rows -> 1,393 distinct entry_ids, zero collisions.
    pending: list[tuple[int, object]] = []              # (row_index, ParseContext)
    unmatched: list[dict] = []
    excluded_empty: list[int] = []
    skipped_existing: list[int] = []
    no_registry_count = 0

    for m in matches:
        row = rows[m.index]
        raw = row.get("raw_regs", "")
        if not raw.strip():
            excluded_empty.append(m.index)              # nothing to split — a pointer/blank row
            continue
        if m.item_id is None:
            unmatched.append({"index": m.index, "water": m.water, "status": m.status, "reason": m.reason})
            eid = _row_entry_id(row, m)
            note = f"{m.status}: {m.reason}" if m.reason else m.status
            ctx = build_no_registry_context(
                entry_id=eid, name=m.water, raw_regs=raw, registry_note=note,
                region=region_num(row), mus=tuple(sorted(parse_reg_mus(row))), row_index=m.index,
                symbols=tuple(row.get("symbols", [])))
            is_noreg = True
        else:
            # NO content dedupe here. Two rows resolving to one registry item with byte-identical
            # regs are still two rows, and the exporter is not the place to decide one does not
            # count: PECKHAMS LAKE and NORBURY LAKE are different waters that merely share
            # "No powered boats"; BLACKWATER RIVER's "See West Road River" pointer is printed in
            # BOTH region 5 and region 7. Dropping either lost a real row silently. A row that is
            # genuinely redundant is marked `reference_only` by the CURATOR, who can see it.
            eid = _row_entry_id(row, m)
            # EVERY entry keeps the synopsis's own wording. This used to fall back to the
            # registry item's name for single-row entries, which is wrong twice over: for a
            # combined row it labelled the whole regulation "Chilliwack River"; and where
            # several differently-named rows resolve to items sharing one collective name it
            # erased the distinction entirely — INDATA, TCHENTLO, TSAYTA and CHUCHI LAKE all
            # became "Nation Lakes", HAYNES/HYDRAULIC/MINNOW became "McCulloch Reservoir".
            # The registry's name is carried alongside as `display_name`.
            name = m.water
            ctx = build_parse_context(registry[m.item_id], raw_regs=raw, entry_id=eid,
                                      region=region_num(row), row_index=m.index, name=name,
                                      symbols=tuple(row.get("symbols", [])),
                                      review_hints=tuple((review_hints or {}).get(eid, ())),
                                      also_items=tuple(registry[i] for i in m.also if i in registry),
                                      row_mus=tuple(sorted(parse_reg_mus(row))))
            is_noreg = False

        if only_ids is not None and eid not in only_ids:
            skipped_existing.append(m.index)            # a targeted subset (repass / review slice)
            continue
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

    # Drop ORPHANED responses/reviews from a previous (larger) export: a batch id no longer produced
    # here would otherwise be glob-ingested against the new layout -> "unknown index" + duplicate noise.
    n = len(manifest_batches)
    for sub, pat in (("responses", "batch_*.json"), ("reviews", "batch_*")):
        d = out_dir / sub
        if not d.exists():
            continue
        for f in d.glob(pat):
            mm = re.search(r"batch_(\d+)", f.name)
            if mm and int(mm.group(1)) >= n:
                f.unlink()

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
    ap.add_argument("--unreviewed", action="store_true",
                    help="REVIEW: export only entries with NO parse_review verdict yet. Resume is by "
                    "ENTRY, not by batch — verdicts live on the entry, so a half-finished review run "
                    "picks up exactly where it stopped even though the batch layout changed.")
    ap.add_argument("--hardest", type=float, default=0.0, metavar="FRAC_OR_N",
                    help="REVIEW: keep only the hardest entries by RULE COUNT — a fraction (0.5 = the "
                    "worst half) or a count (>1). Combine with --unreviewed to review the hard half "
                    "now and the rest later.")
    ap.add_argument("--flagged", action="store_true",
                    help="REPASS: export ONLY entries the review flagged (parse_review.verdict == "
                    "'changes_requested'), skipping locked ones; the reviewer's issues are passed to the "
                    "re-parse as hints. Pair with `dispatch --force` on a fresh --out-dir.")
    args = ap.parse_args()

    rows = load_synopsis_rows()
    registry = load_registry(Path(args.registry) if args.registry else default_registry_path())
    default_ov = CURATED.regulations.overrides
    overrides = load_overrides(args.overrides or (default_ov if default_ov.exists() else None))
    out_dir = Path(args.out_dir) if args.out_dir else default_work_dir()
    entries_dir = Path(args.entries_dir) if args.entries_dir else _io.entries_dir()
    existing_regs = load_existing_entry_regs(entries_dir)
    existing = set(existing_regs)

    only_ids = review_hints = None
    if args.flagged:
        only_ids, review_hints = set(), {}
        for eid, e in io.read_entries_dir(entries_dir).items():
            pr = e.get("parse_review") or {}
            if pr.get("verdict") == "changes_requested" and not e.get("locked"):
                only_ids.add(eid)
                review_hints[eid] = [
                    f"[{i.get('severity','?')}] {i.get('problem','')}"
                    + (f" -> fix: {i.get('fix')}" if i.get("fix") else "")
                    for i in (pr.get("issues") or [])]
    elif args.unreviewed or args.hardest:
        entries = io.read_entries_dir(entries_dir)
        pool = {eid: e for eid, e in entries.items()
                if not (args.unreviewed and ((e.get("parse_review") or {}).get("verdict")))}
        if args.hardest:
            # Rule count is the proxy for difficulty: a 1-rule lake is a sentence, an 8-rule river
            # is several reaches, seasons and species. Ties broken by entry_id so the slice is
            # deterministic and two runs never disagree about where the half is.
            ranked = sorted(pool, key=lambda i: (-len(entries[i].get("rules") or []), i))
            n = int(args.hardest) if args.hardest > 1 else round(len(ranked) * args.hardest)
            pool = {eid: entries[eid] for eid in ranked[:max(0, n)]}
        only_ids = set(pool)
        print(f"  selector: {len(only_ids)} entr(y/ies)"
              + (" unreviewed" if args.unreviewed else "")
              + (f", hardest {args.hardest}" if args.hardest else ""))

    manifest = export(rows, registry, out_dir, args.batch_size, overrides, existing, args.force,
                      skip_existing=args.skip_existing, only_changed=args.only_changed,
                      existing_regs=existing_regs, only_ids=only_ids, review_hints=review_hints)
    print(f"Exported {manifest['pending_count']} rows into {len(manifest['batches'])} batch(es) -> {out_dir/'batches'}")
    print(f"  of those, no-registry (content-only, flagged): {manifest['no_registry_count']}")
    print(f"  held-back rows also parsed (no-registry): {len(manifest['unmatched'])}  "
          f"empty-regs skipped: {len(manifest['excluded_empty'])}")
    if manifest["skipped_existing"]:
        why = ("review-flagged only" if args.flagged else
               "unchanged (already parsed)" if args.only_changed else "already in EntryFiles")
        print(f"  dropped {len(manifest['skipped_existing'])} row(s) — {why}")
    if args.flagged:
        print(f"  repass: {len(only_ids)} flagged entr(y/ies) requested; {manifest['pending_count']} exported")
        if manifest["pending_count"] < len(only_ids):
            print("    ⚠ some flagged entries did not map to a current row (id drift / superseded / "
                  "locked) — not re-parsed")
    print("  (already-parsed batches are skipped at the parse step, not here)")
    print(f"  manifest: {out_dir/'manifest.json'}")


if __name__ == "__main__":
    main()
