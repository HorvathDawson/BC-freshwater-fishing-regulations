# T322 — Fold the owner-confirmed slough/canal polygons into their streams (own side build + mechanical binding gate)

Status: PARTIAL — code done; fold ON since 9ee36a5f1 (2026-11-03), 1st side build (data/generated/side/t322) STOPPED at the gate: an Okanagan River Oxbow (wbk 329460316) folded into Park Rill and spread the oxbows' spring-closure exemption (8 sections). Owner 11-03: keep it its own water -> efe94fb (a water of another name is never absorbed). 2nd side build RUNNING into data/generated/side/t322b (log data/generated/side/t322b.log; t322/ refused a rebuild because live tiles share its handle digest). (762586d4d, c31772b77, a4ce0f996). Split out of T55
(measurements in T55 "Fold: what is needed", 675265492).

## Rule (owner 2026-10-30, corrected twice)
- Input: `data/curated/decided/sloughs.json` — 811 owner-confirmed calls (`stream`, `P: y`); pre-marks never folded.
- ONLY AN ISOLATED polygon (no line touches it, no registry item holds it) is skipped. Everything else is a stream:
  - one named stream in AND out, or a named stream only entering / only leaving it -> it joins that stream;
  - only unnamed lines touch it -> the lake / wetland item holding it is re-kinded `stream` in place (it joins the
    unnamed line; the line stays unnamed, nothing new is made);
  - the OKANAGAN RIVER OXBOWS (31 items) are general streams even cut off from the river: re-kinded `stream` in place,
    never joined to the Okanagan River, no new item.
- Two different named streams (in vs out, or two on one side) -> refused and listed (never picked).
- Never a new item, named or unnamed.

## Work
1. An apply step that fits T266 (decided -> curated input the registry/atlas reads); no `decided/` in stage keys.
2. Tests.
3. Its OWN side build (`--atlas`), after the 10-31 batch: atlas parity minus the old-vs-old baseline, and the
   MECHANICAL binding gate (below) before promote. No owner review step.

Depends: T55, T266. Size: M–L.

## Done 2026-10-30 (code; no atlas build run)
- Files moved under data/ (owner 10-30, ac7f3fd32, `git mv`): page verdicts -> `data/curated/sloughs/page-marks-2026-10-28.json`;
  finder + pre-marks -> `data/generated/sloughs/{candidates_v2.json.gz,slough-automark.json}` (committed, `!generated/sloughs/`).
  The running review app (:8787) keeps the old paths in memory until its next reload (it shows 0 sloughs until then).
- Apply step: `python -m pipeline.tools.slough_calls [--check]` -> `data/curated/waters/slough_calls.json`
  (CURATED.waters.slough_calls): verified + call stream + P y, not stale. 811 written. Rerun after `review.export`.
- Fold: `registry.flowing.fold_confirmed_sloughs`, after `merge_flowing_polygons` in the atlas build and the sidecars,
  behind `flowing.FOLD_CONFIRMED = False`. Confirmed polygons joined by an edge are one run. A lake item fully
  confirmed is absorbed into the stream it joins (alias); a lake item only partly confirmed is never split. F6 boundary
  handling as the flowing fold. Report: `slough_fold_report.json` beside the registry.
- Gate: `python -m pipeline.tools.audit.slough_fold_gate BEFORE AFTER SIDE_ATLAS` — streamed; exit 1 when any rule or
  licensing row changes on a section outside {folded polygons, re-kinded items, the streams they join}, or any rule
  changes unresolved state. ALLOWED and listed as `via_fold` (coordinator 10-30): a creek whose path to the stream
  passes through a folded / re-kinded polygon (it is that stream's tributary now) — the side graph's upstream walk from
  each polygon, never into the joined stream's own sections. Reads the side `graph.pkl`: run it under heavy.sh.
- Tests: `pipeline/tests/test_slough_fold.py` (15).
- DRY RUN on the live atlas (`pipeline.tools.audit.slough_fold_preview`, report
  `data/generated/scratch/slough_fold_preview.json`), 811 calls:
  - **780 become stream**: 749 join 236 named streams (through 745, a stream only leaves 3, only ends in it 1; top
    Cormier 25, Gunnell 24, Moss 24, Utahn 24, Kah 23, Trepanier 21; 1 oxbow absorbed into Park Rill, which runs
    through it) + **31 OKANAGAN RIVER OXBOWS re-kinded stream in place** (18 on an unnamed line, 13 isolated).
  - **29 skipped**: isolated, no item.
  - **1 HELD (stays held, coordinator 10-30)**: wbk 329708509 — only unnamed lines touch it and no item holds it;
    making it a stream needs either a new item (forbidden) or a section-level kind the registry does not have. It
    stays a lake; listed in `slough_fold_report.json` as "unnamed, no item".
  - 1 already stream (Cayoosh Creek). 0 two-stream refusals, 0 partly confirmed items, 0 cuts on an edge.

## Side build (after the 10-31 batch is promoted)
```sh
# 1. turn the fold on: pipeline/atlas/registry/flowing.py  FOLD_CONFIRMED = True
scripts/heavy.sh t322-side 70 -- .venv/bin/python -m pipeline build --side data/generated/side/t322 --atlas --jobs 3 \
  > data/generated/side/t322.log 2>&1
.venv/bin/python -m pipeline.tools.audit.build_parity data/generated/atlas/full data/generated/side/t322/atlas/full_next
SIDE_REACH=$(ls -d data/generated/side/t322/reaches/* | head -1)
.venv/bin/python -m pipeline.atlas.reach.binding_diff data/generated/reaches/full "$SIDE_REACH" \
  --json handoff/PLAN/audit/binding-diff-t322.json
# 2. THE GATE: exit 0 = promote (rebuild-batch-2026-10-31.md step 3); exit 1 = stop, explain what moved
scripts/heavy.sh t322-gate 40 -- .venv/bin/python -m pipeline.tools.audit.slough_fold_gate data/generated/reaches/full "$SIDE_REACH" \
  data/generated/side/t322/atlas/full_next --json handoff/PLAN/audit/slough-fold-gate-t322.json
```

## Owner 2026-10-30: no separate review
"I don't think we need to review the slough stuff, we did the work in the review app already." The side build + binding diff stay as a MECHANICAL gate run by Claude (only the folded sloughs/oxbows and the waters they join may change; anything else stops the promote); no owner review step. Oxbows (31) re-kind to general streams; only polygons no line touches are skipped.

## RESUME NOW (2026-11-03, after the t322b side build)
Use SIDE=data/generated/side/t322b. When the log ends `done rc=0`:
  SIDE_REACH=$SIDE/reaches/full_next
  .venv/bin/python -m pipeline.tools.audit.build_parity data/generated/atlas/full $SIDE/atlas/full_next
  .venv/bin/python -m pipeline.atlas.reach.binding_diff data/generated/reaches/full $SIDE_REACH --json handoff/PLAN/audit/binding-diff-t322.json
  scripts/heavy.sh t322-gate 60 -- .venv/bin/python -m pipeline.tools.audit.slough_fold_gate data/generated/reaches/full $SIDE_REACH $SIDE/atlas/full_next --json handoff/PLAN/audit/slough-fold-gate-t322.json   # must exit 0; oxbow rows must be gone
  DFO re-match into a scratch copy of data/curated/regulations/entries/dfo_salmon (match apply --registry $SIDE/atlas/full_next/registry.json --entries-dir <copy>), diff -r: stop on any change
  .venv/bin/python -m pipeline.atlas.promote $SIDE/atlas/full_next
  scripts/heavy.sh t322-live 60 -- .venv/bin/python -m pipeline build --jobs 3 > data/generated/side/t322-live.log 2>&1
No staging upload (SALMON_NEEDS until 0.5.0). Then delete data/generated/side/t322 (failed side). Then batch 2.

## RESUME (2026-10-31, stopped on low credits — nothing of T322 started)
State: cleanup/t327-t331 merged (3f28d0e80); data/release pruned to r-4e709418ebd8 (LATEST, on staging; prod has no
release) + r-038e6818e80f. FOLD_CONFIRMED still False. dfo/t332-shared-quota NOT merged.
```sh
# 1. flowing.py FOLD_CONFIRMED = True; commit -- pipeline/atlas/registry/flowing.py
scripts/heavy.sh t322-side 60 -- .venv/bin/python -m pipeline build --side data/generated/side/t322 --atlas --jobs 3 \
  > data/generated/side/t322.log 2>&1
.venv/bin/python -m pipeline.tools.audit.build_parity data/generated/atlas/full data/generated/side/t322/atlas/full_next
SIDE_REACH=$(find data/generated/side/t322/reaches -mindepth 1 -maxdepth 1 -type d | head -1)
.venv/bin/python -m pipeline.atlas.reach.binding_diff data/generated/reaches/full "$SIDE_REACH" --json handoff/PLAN/audit/binding-diff-t322.json
scripts/heavy.sh t322-gate 60 -- .venv/bin/python -m pipeline.tools.audit.slough_fold_gate data/generated/reaches/full "$SIDE_REACH" \
  data/generated/side/t322/atlas/full_next --json handoff/PLAN/audit/slough-fold-gate-t322.json   # must exit 0
# 2. DFO re-match on the SIDE registry (T337: item_ids is a cache no stage refreshes; Park Rill oxbow absorbed).
#    apply never rewrites existing item_ids, so ALSO check every existing item_id still resolves in the side registry.
cp -R data/curated/regulations/entries/dfo_salmon /tmp/dfo-t322 && .venv/bin/python -m pipeline.regs.dfo_salmon.match apply \
  --registry data/generated/side/t322/atlas/full_next/registry.json --entries-dir /tmp/dfo-t322
diff -r data/curated/regulations/entries/dfo_salmon /tmp/dfo-t322   # any changed/unresolved binding: STOP before staging
# 3. promote + live build + staging
.venv/bin/python -m pipeline.atlas.promote data/generated/side/t322/atlas/full_next
scripts/heavy.sh t322-live 60 -- .venv/bin/python -m pipeline build --jobs 3 > data/generated/side/t322-live.log 2>&1
MIN_APP_VERSION=0.3.0 deploy/staging/upload.sh
scripts/heavy.sh rt-base 60 -- .venv/bin/python -m pipeline.runtiming.feed.base --upload --yes
.venv/bin/python -m pipeline.runtiming.parse        # DFO counts re-parse
.venv/bin/python -m pipeline.runtiming.feed.build --timing data/source/runtiming/timing.json \
  --polygons data/generated/runtiming/cu_polygons.geojson --base data/generated/runtiming/feed/base-<handles>.json.gz --out /tmp/rt-out
.venv/bin/python -m pipeline.runtiming.feed.publish --dir /tmp/rt-out/runtiming --via wrangler --yes   # staging only
node app/tools/smoke-staging.mjs      # 18/18; then STATUS.md
```
Then T332: merge dfo/t332-shared-quota (no release); add an upload.sh guard SALMON_NEEDS = {3: "0.5.0"} beside
PAINT_NEEDS (app version lives in app/apps/mobile/app.json + package.json, now 0.4.0; T332 app = 0.5.0); note in
OWNER-TODO + T332: next release MIN_APP_VERSION=0.5.0, feeds worker deploy + sync-inputs only AFTER that app ships.
