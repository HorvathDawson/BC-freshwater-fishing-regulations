# T322 — Fold the owner-confirmed slough/canal polygons into their streams (own side build + binding diff)

Status: CODE DONE, fold OFF until its side build (2026-10-30, 762586d4d). Was OPEN (owner 2026-10-30: "If no stream flows through then it can be ignored, it is not a stream then … do it in
its own build and ticket"). Split out of T55 (measurements in T55 "Fold: what is needed", 675265492).

## Rule
- Input: `data/curated/decided/sloughs.json` — 811 owner-confirmed calls (`stream`, `P: y`); pre-marks never folded.
- A confirmed polygon with a stream flowing in AND out (graph) becomes part of THAT stream (its sections take the
  stream's identity and rules). Several streams through one polygon: follow the existing braid/merge rules.
- NO stream through it → IGNORED (owner: then it is not a stream). Never create a new unnamed stream item. List these.
- 33 are registry items (29 lake, 3 wetland, 1 already stream); 778 are unnamed polygons outside the registry.

## Work
1. An apply step that fits T266 (decided → curated input the registry/atlas reads); no `decided/` in stage keys.
2. Tests: confirmed + through-stream folds; confirmed + no stream ignored; unconfirmed / `not stream` untouched.
3. Its OWN side build (`--atlas`), after the 10-31 batch: atlas parity minus the old-vs-old baseline, and a BINDING
   diff (every water whose rules change, lake → stream zone rules, named stream rules extended) → owner review
   before promote.

Depends: T55, T266. Size: M–L. Owner: review the binding diff.

## Done 2026-10-30 (code; no atlas build run)
- Files moved under data/ (owner 10-30, ac7f3fd32, `git mv`): page verdicts -> `data/curated/sloughs/page-marks-2026-10-28.json`;
  finder + pre-marks -> `data/generated/sloughs/{candidates_v2.json.gz,slough-automark.json}` (committed, `!generated/sloughs/`).
  The running review app (:8787) keeps the old paths in memory until its next reload (it shows 0 sloughs until then).
- Apply step: `python -m pipeline.tools.slough_calls [--check]` -> `data/curated/waters/slough_calls.json` (CURATED.waters.slough_calls):
  verified + call stream + P y, not stale. 811 written. Rerun after `review.export` when the owner marks more.
- Fold: `registry.flowing.fold_confirmed_sloughs`, run after `merge_flowing_polygons` in the atlas build and the sidecars,
  behind `flowing.FOLD_CONFIRMED = False`. Confirmed polygons joined by an edge are one run; ONE stream item in and out
  -> joins it (a lake item fully confirmed is absorbed as an alias); else ignored with why. F6 boundary handling as the
  flowing fold. Report: `slough_fold_report.json` beside the registry.
- Tests: `pipeline/tests/test_slough_fold.py` (10).
- DRY RUN on the live atlas (`pipeline.tools.audit.slough_fold_preview`, report `data/generated/scratch/slough_fold_preview.json`):
  **745 fold into 234 streams** (top: Cormier 25, Gunnell 24, Moss 24, Utahn 24, Kah 23, Trepanier 21); 1 registry
  item absorbed (an Okanagan oxbow polygon into Park Rill); **65 ignored**: 42 no line touches it, 19 only unnamed lines,
  3 a stream only leaves, 1 a stream only ends in it; 1 already stream (Cayoosh Creek). Of the 33 registry items, 31
  are OKANAGAN RIVER OXBOWS that are ignored (cut off from the river). 0 two-stream / item-not-confirmed / cut-on-edge.

## Side build (after the 10-31 batch is promoted)
```sh
# 1. turn the fold on: pipeline/atlas/registry/flowing.py  FOLD_CONFIRMED = True
scripts/heavy.sh t322-side 70 -- .venv/bin/python -m pipeline build --side data/generated/side/t322 --atlas --jobs 3 \
  > data/generated/side/t322.log 2>&1
.venv/bin/python -m pipeline.tools.audit.build_parity data/generated/atlas/full data/generated/side/t322/atlas/full_next
.venv/bin/python -m pipeline.atlas.reach.binding_diff data/generated/reaches/full "$(ls -d data/generated/side/t322/reaches/* | head -1)" \
  --json handoff/PLAN/audit/binding-diff-t322.json
# owner reviews the binding diff, then promote as in rebuild-batch-2026-10-31.md step 3
```

## Owner 2026-10-30: no separate review
"I don't think we need to review the slough stuff, we did the work in the review app already." The side build + binding diff stay as a MECHANICAL gate run by Claude (only the folded sloughs/oxbows and the waters they join may change; anything else stops the promote); no owner review step. Oxbows (31) re-kind to general streams; only polygons no line touches are skipped.
