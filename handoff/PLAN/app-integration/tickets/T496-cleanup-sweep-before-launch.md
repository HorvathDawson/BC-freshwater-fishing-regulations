# T496 — Cleanup sweep before launch

Status: PARTIAL — pass 1 2026-11-08, 678bf986e (see below); owner 2026-11-07 (part of T495).

Code, config and docs that the launch should not carry. Owner rule: delete freely, git is the safety net; never drop or
rename DATA content without asking.

## Do
- v1 leftovers: old production website config/scripts/docs and references to the v1 site that the T404 switch
  replaces (keep the T198 crosswalk/301s — they ARE the v1 transition).
- Dead pipeline code (no knip there): modules, CLIs and flags nothing calls; stale `smoke_pending` entries.
- Stale docs: MAP.md files vs the code, runbooks naming removed scripts, handoff pages no longer cited.
- Release folders / generated outputs past live + previous; scratch tools in app/tools that were one-offs.
- Each removal: one line in the commit saying why it is dead (grep / graphify evidence). Full suite once at the end.

## Pass 1 (2026-11-08 night) — what went
- Local outputs (gitignored, no commit): 7 release folders past live + previous (`pipeline.deliver.release.prune --yes`,
  kept r-6dce77a24e6a LATEST + r-25a89ce9f8e6), 21 tile versions and 3 basemaps no kept manifest names (~5.6 GB;
  data/release 680 MB now).
- Smoke ratchet: 142 of 145 `smoke_pending.txt` files moved into `smoke.txt` (fastest passing data-free test each, timed
  run, all < 0.13 s); PENDING_MAX 148 → 3. The 3 left are files another lane has open (test_part_line,
  test_release_slices, test_t207_split_confidence).
- Dead code: `app/tools/spot-visits-shots.mjs` (T294 one-off, nothing runs it); the `--places` flag of
  `python -m pipeline.deliver.tiles` (never read, nothing passes it).
- Stale docs: app/MAP.md `tools/card_glyphs.py` → `workers/feeds/tools/card_glyphs.py`; data/generated/README.md
  `pipeline.tools.reparse_candidates` → `pipeline.tools.audit.reparse_candidates`.
- v1: nothing left on this branch — the v1 workflows (`update-hydro/in-season/stocking.yml`) live only on `main` and the
  `bc-fishing-cron` worker is not in the repo; both go at PRODUCTION.md step 11 (cutover). T198 crosswalk kept.

## Live silent fallbacks found (behaviour NOT changed — owner call)
- `app/tools/consistency.ts:22` — `readdirSync` failure returns no files, so a moved UI root makes the consistency
  auditors pass vacuously.
- `app/tools/check-boundaries.mjs:101` — a layer dir that cannot be stat'ed falls back to the bare path; a typo'd layer
  walks nothing and its rules never apply.
- `pipeline/tools/audit/audit_splits.py:48` (`except Exception: return None`, bbox) and `:80` (a chain-build crash
  marks every split on the water `resolved: False` — a crash reads as an audit finding).
- Checked, not silent: scripts/update-hydat.sh:54-55 and scripts/hydat-fetch.sh:31 (`|| true` followed by an explicit
  empty/size/CRC check that exits 1); emit-fish.mjs:180/188 (null → stale → exit 1).

## Deferred (other lanes had them open, or owner call)
- app/packages/core/src/regs/*, waterNav*, inseason*, parts/paint/naming code, release orchestration + heavy.sh,
  workers/feeds/src/dfo/*, app icons, app.config.js — all files in `git status` tonight.
- Dangling `archive/webapp/...` prior-art comments (app/packages/map controls.css:10, hatch.ts:8, runtime-style.ts:485,
  ui-native FishSpinner.native.tsx:6, tools/build-spinner.mjs:6, build-style.mjs:303/583/647, workers/feeds/src/index.ts:2)
  — harmless, several in files other lanes touch.
- README.md:10-12 "main still runs v1" banner and deploy/prod/data.wrangler.toml:3-5 v1-keys comment: reword at cutover.
- Kept on purpose (still cited / open tickets): slough_fold_preview + slough_fold_gate (T322 PARTIAL), tidal_preview
  (T166 runbook step), audit_splits / audit_split_binding / unnamed_trib_sweep (re-runnable audits), dfo_auto (DFO
  labelling), polish-shots.mjs (T410 proof method), label-shots / testbed tap-glide + lab-shots (T493/T420 in flight).
- Handoff pages no longer cited: not swept this pass.

## Suites (pass 1, once at the end, unfiltered)
- `pytest -m smoke`: 377 pass. Full fast pytest: 45 failed / 609 errors, all in the answers/export/part/store lane's
  in-flight schema (`no such column: p.grp`, `part has 17 columns but 18 values`, `KeyError: 'ends'`) against the live
  bundle — none in files this pass touched; test_smoke_coverage passes.
- App vitest: 44 files fail — regs fixture/store (the deleted fixture release r-fc17c4373686 is another lane's), paint/4
  vs paint/5 release boot, WaterMapView; check-tickets.test passed on its own rerun (15/15).

Fallbacks fixed (dev tools): consistency.ts sources() now throws on an unreadable root; audit_splits.py _target_bounds and chain-build now raise naming the water; check-boundaries.mjs:101 kept (a not-yet-created layer is the tested contract for scratch trees).
