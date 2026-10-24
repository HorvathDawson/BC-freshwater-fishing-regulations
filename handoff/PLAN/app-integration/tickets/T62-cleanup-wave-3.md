# T62 — Cleanup wave 3: split long files, make it easy for a human to follow

Status: PARTIAL — app lane b619ca7b…bb31281e merged; pipeline lane running; 3.2 + 3.9 after the braid and names lanes. Source: handoff/PLAN/audit/PLAN.md §2 "Wave 3". Includes Shell.tsx (re-spec P14/P17 against today's
shell first, N13), export_ui_rules, other files over the size line. Behaviour unchanged: tests green before/after.

## Outcome (app lane)

Wave items 3.4, 3.5, 3.6, 3.7, 3.8 and 3.10 are done. Each split is its own commit on redesign/stream-sections (from
9253f72f) and none changes behaviour. Strict check: `REGS_STORE_REQUIRED=1 pnpm -C app check`, run through heavy.sh,
ends `Test Files 366 passed | 4 skipped (370) · Tests 8178 passed | 21 skipped (8199)` with rc=0. No snapshot was
written or updated.

| Item | File | Lines before → after |
|---|---|---|
| 3.4 | `ui-native/src/Shell.tsx` | 981 → 239. The spec of today's Shell (N13) heads the file. The parts are in `shell/`: useShellNav, useMapColouring, useShellSearch, MapLegend (+PairBanner), WaterModeHost, BrowseHost, SpotHost, ShellSheets, props |
| 3.5 | `map/src/Map.web.tsx` · `runtime-style.ts` · `adapters/contract.ts` | 1214 → 135 (16 hooks/helpers in `map/src/web/`) · 728 → 495 (+ endpoints.ts, gaugeDots.ts) · 703 → 348 (paint → paint.ts) |
| 3.6 | `ui/src/panel.ts` · `ui/src/hooks.ts` · `data/src/index.ts` | 1141 → panel/{feed,standings,horizon,donor,conditions,change} · 383 → hooks/{water,gauge,facts} · 637 → 39 (barrel) + types.ts + source.ts |
| 3.7 | `RegsSource` | Renamed `WaterSource` in one pass over app/ and the live integration docs, with no alias kept. Ticket files and archives keep the old name as history |
| 3.8 | SpotScreen · SearchSheet · core/search.ts · core/menu.ts | 581 → 276 (+spot/) · 423 → 180 (+search/) · 518 → 287 (+searchGeometry.ts) · 566 → menu/ (one file per page) |
| 3.10 | ConditionsLayout.before.ts + `P16_RECORD` | Deleted. The test now names every drawn line directly |

Gates that read a moved file by path now follow the code, and no copy was added. KNOWN_COPY entries moved with
SpotScreen's and SearchSheet's parts. back-layers no longer lists `Shell#Shell: takes onBack`, because the route
layer's `onBack` is now in a hook; the Shell now shows up through the day sheet it opens. The Layers and Day sheets are
still drawn by Shell itself, next to their open state, so the gate can see that each open state reaches a sheet that
registers with back.

Not done:
- `core/conditions.ts`: two gates pin that exact file, and moving HORIZONS out would make one of them pass vacuously.
- 3.10's second half, "pnpm check prints one line of skipped tiers": the brief did not include it.
- No dead code turned up in any of the split files.
