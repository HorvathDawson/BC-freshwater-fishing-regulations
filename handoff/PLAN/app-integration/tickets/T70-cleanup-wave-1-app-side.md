# T70 — Cleanup wave 1, app side: safe deletions, stale worktrees, `pnpm clean`

Status: DONE — 52ec4cb5, 799614a5, 4615ee0f, 1e8cbdb5, ac95e09f, a7947f99, de3cb79d, review ef11d8e4 (lane wf_63a2801d-ce3-3). All code
items done; left: three filesystem steps outside a worktree lane (below) and the strict `pnpm check` run at merge.

Source: handoff/PLAN/audit/PLAN.md §2 "Wave 1" items 1.1–1.7, 1.13. The pipeline half landed (7ea5300b, 8f459d98,
974359b2, 15a32b5c); the app half did not. T61 is wave 2 only. Checked on disk 2026-10-22:
- 1.1 `app/tools/render-setup.js` + `.d.ts` still tracked.
- 1.2 `dist/` in core, data, map, ui, ui-native, apps/mobile.
- 1.3 unused exports: `clearBasinCache` (ui/panel.ts), `NO_GAUGE`, `NO_READING_HERE` (core/flow.ts), `paletteFaults`,
  `DEFAULT_VIEW`, `ATLAS_MIN_Z`, `ALL_REGIONS`.
- 1.5 `TabPending` (WaterScreen.tsx, RegsScreen) and `WORDS.TAB_PENDING` (words.ts).
- 1.6 `app/apps/web/` and `formFactorFor` (ui/index.ts) — HANDOFF-ui: "Desktop is scaffolding".
- 1.7 `useWaterGauge`, `gaugeForItem`, `itemExists`, `lakeInfo`, `stockingHistory` (31 references).
- `pipeline/hack/` holds only `__pycache__`; stale worktrees `.claude/worktrees/agent-a8261f149d43eead6` and
  `wf_e2463c2d-be5-1` (remove the latter only after T71 has taken the 485677bc text).
- 1.4 BrowseBar: already gone.

## Work
One grep-verified delete per commit; before each, prove nothing needs it (outputs, consumers, docs, CI — owner rule).
Amend AGENTS rule 26 and app/README to "desktop = layout class". Add a `pnpm clean` script. Remove `pipeline/hack/`.

## Acceptance
A grep finds 0 hits for every listed name; typecheck clean; strict `REGS_STORE_REQUIRED=1 pnpm -C app check` green.

Depends: T71 (for the wf_e2463c2d worktree). Do before T63 (knip starts clean). Size: M. Owner: no.

## Outcome (lane wf_63a2801d-ce3-3, 2026-10-22)
- 1.1 52ec4cb5: `tools/render-setup.js` + `.d.ts` removed (vitest and every render test import the `.ts`).
- 1.2 799614a5: `tsconfig.base.json` sets `emitDeclarationOnly` (no JS in any dist/ again); `pnpm clean`
  (`tools/clean.mjs`) deletes packages/*/dist, apps/*/dist, conformance/dist. Pinned by `tools/clean.test.ts`
  (mutation: dropping conformance/dist from the cleaner fails it).
- 1.3 4615ee0f: clearBasinCache, NO_GAUGE, NO_READING_HERE, DEFAULT_VIEW, ATLAS_MIN_Z, ALL_REGIONS, paletteFaults,
  plus the audit's ui-native `mapChrome` wrapper, STEELHEAD_STAMP_ITEM, YOUTH_ALT_LINES.
- 1.5 1e8cbdb5: TabPending + WORDS.TAB_PENDING (gear/methods/licence are null only when `edgeKind` is "outside").
- 1.6 ac95e09f: `app/apps/web` (READMEs + package.json only), its lock importer and layers.json layer, formFactorFor;
  AGENTS rules 25–26, app/README, apps/mobile/README, HANDOFF-ui, deps.md, root README → "desktop = layout class".
  Pinned by `tools/one-app.test.ts`.
- 1.7 a7947f99: useWaterGauge, gaugeForItem, itemExists (+ GAUGE_FOR_ITEM, SECTION_GAUGES, byGaugeForItem).
  **lakeInfo and stockingHistory KEPT** — the owner's 2026-10-22 wave-1 correction (decisions.md "the audit plan":
  stocking/bathymetry code is prepared, not dead) overrides this ticket's list; the interface now marks them
  "PREPARED, NOT FED". So the acceptance grep finds them by design.
- de3cb79d: `tsc -b --force` on the branch head failed TS2352 in two map tests (an incremental build hid it); cast
  through `unknown`. `tsc -b --force` is now clean.
- 1.4 BrowseBar: already gone. The 485677bc text is on the branch (decisions.md "D2 / D24 against the view strip").

## Left (for the merger, in the main checkout — a worktree lane may not touch it)
- `rm -rf pipeline/hack` (untracked: only `__pycache__`; nothing tracked references it).
- `git worktree remove .claude/worktrees/agent-a8261f149d43eead6` and `.claude/worktrees/wf_e2463c2d-be5-1` (check
  `git status` in each first; keep their branches: agent-a8261… holds the N1 spike code commits marked "do not
  merge", whose results — N1-SPIKE.md + shots — are already on the branch; 485677bc's text is already merged).
- `pnpm -C app clean` once, to delete the old orphan JS in the main checkout's dist/ dirs; then
  `REGS_STORE_REQUIRED=1 pnpm -C app check` (the lane ran every gate but the python ones and targeted tests only).
- Not in this ticket: `stationsFor` has no non-test caller (audit 1.7 named it; this ticket did not) — T63's knip
  pass decides. Several pre-existing unused imports/locals (`--noUnusedLocals`: core regs/banner.ts, plainrow.ts,
  data panel.ts, ui hooks.ts LakeInfo/PlaceId, ui panel.ts donorKeyLabel, …) — also T63.

## Review (adversarial, 2026-10-22)
Reproduced: listed names grep to 0 (lakeInfo/stockingHistory kept per decisions.md 2026-10-22 correction; formFactorFor
and apps/web only in prose/the pin test); `tsc -b --force` exit 0 with 0 JS in any dist/; tickets/boundaries/platform/deps
gates green; frozen offline install green; targeted vitest (tools, data, ui, core regs+flow, ui-native WaterScreen+regs,
two map tests, conformance) all green with .venv linked (without it 14 python-spawning tests fail ENOENT — env only).
TabPending was truly unreachable: `licence` is null iff `inp.key` is null iff `edgeKind` is "outside". One fix:
ef11d8e4 layers.json ui-native "why" still said "the desktop component set". Nothing else left beyond the merger steps above.

## Merger steps (2026-10-24)
`pipeline/hack` and both stale worktrees were already gone; `pnpm -C app clean` ran; strict
`REGS_STORE_REQUIRED=1 pnpm -C app check` green: 365 files / 8212 tests passed, 22 skipped.
