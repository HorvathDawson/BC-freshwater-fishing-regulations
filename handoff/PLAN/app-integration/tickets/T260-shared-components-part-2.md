# T260 — shared components, part 2 (the rest of the T257 sweep)

Status: OPEN

Source: owner 2026-10-29 polish pass (T257 add-on: "extract generic reusable components, migrate call sites, guard").
T257 extracted Button (small / link / busy / icon), LoadingState + useDelayedShow, ErrorState and round(); these are
what its audit found and left, each to be extracted from the best existing instance with no look change (pixel-diff
proof per migrated screen, as T257 did with `app/tools/polish-shots.mjs`).

## Work
1. Chip / Badge: the selection chips (ChartControls, Sheet chip variant, SpotCapture quick days), the env badge
   (InfoPages), count badges (TopBar bell, MapRail, SpotsScreen "no photo") — one component with variants.
2. SegmentedControl: CornerStrip generalised; candidates More → low data (Auto/On/Off), InfoPages env choice, Sheet
   `segmented` (ink-filled, the Licence tab) — decide one selected-state language with the owner first (Q).
3. EmptyState (SpotsScreen's title + body) and the "none" lines (ReportPage, UpdatesLogPage, ConditionsPanel, RunsSheet).
4. Banner / Notice (NetBanner, PartPending, SyncErrorSheet rows, SpotPickBanner, InSeasonBanner) and a SectionHeader
   (TYPE.section kickers with ad-hoc overrides in LayersSheet, SpotCapture, spot/parts).
5. Type: a `caption` token for the ~18 `fontSize: 11.5` overrides; drop `fontWeight` literals (the face is the weight;
   widen tools/regs-tokens' no-weight check to all of ui-native); App.tsx boot lines and MapLegend's raw sizes.
6. Icon sizes: a ladder (14 / 16 / 18 / 20); close icons are 20 and 22 for one job, chevrons 14 / 16 / 18.
7. IconButton: Hydrograph ±, DayCalendar Btn, SpotCapture Step (three looks for one job).
8. RunsControls.tsx / RunsSheet.tsx (run-timing v2): literal radii → round() / palette.r.*, then drop them from
   tools/one-button.test.ts RADIUS_PENDING.
## Acceptance
Each component extracted unchanged from its best instance; call sites moved; a guard per component; before/after
shots + pixel diff per migrated screen (desktop + phone, light + dark), any pair over 0.5 % reviewed.
Depends: T257. Size: M. Owner: Q on item 2 only.
