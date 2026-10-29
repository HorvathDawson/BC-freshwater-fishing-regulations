# T87 — Owner sign-off walk, then the final clean-up phase

Status: PARTIAL — clean-up done 2026-10-28 (owner: "delete the old design prototype now — it's in git"): 1267d358
(re-cut tooling to app/tools/regs-recut/, page v36 to app/fixtures/reference/) + the deletion commit. Left: the owner's
sign-off walk (web + phone), recorded in decisions.md.

Source: implement_progress "FINAL CLEAN-UP PHASE (owner, 2026-10-20) — ONLY after the app is done AND the owner has …
confirmed it looks like the prototype": remove app/design, then audit handoff/ in the repo and in memory. T63 has no
owner gate.

## Work
The owner walks the app (web + phone). After sign-off: delete app/design/ and dead handoff/ material in dated commits;
prune stale memory notes.

## Acceptance
Sign-off recorded in decisions.md; nothing references app/design; the CI dead-link check passes.

Depends: T63, T77, T82. Size: M. Owner: yes. Default: hold until the owner walks it.

## Clean-up (2026-10-28)
- Moved: `port/{sync.mjs,taps.py,verify_map.py,hydrate.mjs}` -> `app/tools/regs-recut/` (sync = taps + verify_map +
  hydrate); `regs-v11/reference/` -> `app/fixtures/reference/`; `regs-v11/watermap.js` -> `pipeline/tests/fixtures/`;
  `fish-sprite.{png,html}` -> `app/tools/fish-sprite/`. Proof: old vs new taps/hydrate on r-a871447c48ba identical
  except `meta.generator`.
- Frozen (no generator left): `app/fixtures/golden/{golden,pins,tokens,inventory}.json`.
- Deleted: `app/design/` (prototype, snapshots, screens, check.mjs and the port generators); `app/tools/compare-regs.mjs`,
  `regs-heights.mjs`, `regs-web/{proto,compose,tall,sheets}.mjs` (+ tests): side-by-sides with the prototype; the CI and
  test-full/test-smoke prototype check; `test_the_design_mock_carries_the_shipped_map…` (the mock is gone; the map is
  still proved against the PDF and the export); `handoff/PLAN/r20*.md`, `ur5_data.md` (prototype round notes).
- Not deleted: handoff shot folders. Git dates them 2026-10-20 or later, so none is older than the two-week bar.
