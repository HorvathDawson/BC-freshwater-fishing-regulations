# T70 — Cleanup wave 1, app side: safe deletions, stale worktrees, `pnpm clean`

Status: OPEN.

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
