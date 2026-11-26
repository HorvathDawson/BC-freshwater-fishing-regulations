# T496 — Cleanup sweep before launch

Status: PARTIAL — pass 1 2026-11-08, 678bf986e, and the dev-tool fallbacks fixed in f135c4284 (see below); left (Claude): pass 2 over the "Deferred" list (files other lanes had open, the archive/webapp comments, the v1 banners now that production is live, handoff pages no longer cited). Owner 2026-11-07 (part of T495).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T496 | Cleanup sweep before launch: v1 leftovers, dead pipeline code, stale docs, old outputs | PARTIAL — pass 1 done; deferred list in ticket | 678bf986e, f135c4284 | left (Claude): pass 2 over the deferred list |

Code, config and docs that the launch should not carry. Owner rule: delete freely, git is the safety net; never drop or
rename DATA content without asking.

## Do
- v1 leftovers: old production website config/scripts/docs and references to the v1 site that the T404 switch
  replaces (keep the T198 crosswalk/301s — they ARE the v1 transition).
- Dead pipeline code (no knip there): modules, CLIs and flags nothing calls; stale `smoke_pending` entries.
- Stale docs: MAP.md files vs the code, runbooks naming removed scripts, handoff pages no longer cited.
- Release folders / generated outputs past live + previous; scratch tools in app/tools that were one-offs.
- Each removal: one line in the commit saying why it is dead (grep / graphify evidence). Full suite once at the end.

Done, pass 1 (2026-11-08, 678bf986e): 7 local release folders past live + previous and ~5.6 GB of old tiles pruned;
142 of 145 `smoke_pending.txt` files moved into `smoke.txt` (PENDING_MAX 148 → 3; the 3 left: test_part_line,
test_release_slices, test_t207_split_confidence); `app/tools/spot-visits-shots.mjs` and the tiles `--places` flag
deleted; two stale doc paths fixed. v1: the v1 workflows (`update-hydro/in-season/stocking.yml`) live only on `main`
and the `bc-fishing-cron` worker is not in the repo; both go at PRODUCTION.md step 11 (cutover). T198 crosswalk kept.

Silent fallbacks found (f135c4284): `app/tools/consistency.ts:22`, `app/tools/check-boundaries.mjs:101`,
`pipeline/tools/audit/audit_splits.py:48` and `:80`.

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


Fallbacks fixed (dev tools): consistency.ts sources() now throws on an unreadable root; audit_splits.py _target_bounds and chain-build now raise naming the water; check-boundaries.mjs:101 kept (a not-yet-created layer is the tested contract for scratch trees).
