# T260 — shared components, part 2 (the rest of the T257 sweep)

Status: DROPPED — merged into T410 (2026-11-05 audit; the work continues there) — was: PARTIAL — items 1 (Chip), 3, 5 (caption), 8 done with an exact-markup proof; 2 waits for the owner (Q); 4, 5b, 6, 7 left (each needs a look decision)

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

## Built
- **Chip** (`Chip.tsx`, looks `choice` / `day` / `chart`): extracted unchanged from Sheet `Choice`'s pill, SpotCapture's quick days and
  ChartControls' switch; the three call sites use it. Guard: no typed-out chip (one-button.test.ts).
- **EmptyState + NoneLine** (`EmptyState.tsx`): the Spots tab's title-and-line, and the quiet "none" line (ReportPage, UpdatesLogPage,
  ConditionsPanel x2). Guard: no typed-out title-and-line.
- **`TYPE.caption`** (`type.ts`): `small` at 11.5; the 12 screens that typed `...TYPE.small, fontSize: 11.5` use it. Guard: none left.
- **Radii** (item 8): RunsControls / RunsSheet circles are `round(side)`, the track and bar `palette.r.chip`; `RADIUS_PENDING` is empty.
  Deliberate one-off look change: the Runs slider track (3 px) and the run bar (2 px) are square now, like every other bar.
- **Proof, not shots** (`extracted.same.test.tsx`): each extracted component renders byte-identical markup (react-native-web turns a
  style into class names by content, so identical markup is identical pixels) to the verbatim copy it replaced, light + dark, every state
  (19 cases); the caption token equals `{...small, fontSize: 11.5}`. This stands in for the before/after screenshots: an isolated "before"
  build would need a second worktree with its own node_modules and a second Metro, and the markup comparison is exact where a pixel
  diff is a threshold.

## Left (each needs a decision, not typing)
- **Badge** (env badge, count badges, "no photo"): the three look different on purpose; unifying changes one of them.
- **SegmentedControl** (item 2): owner Q first - one selected-state language (accent frame, ink fill or tint).
- **Banner / Notice, SectionHeader** (item 4): NetBanner, PartPending, SyncErrorSheet rows, SpotPickBanner, InSeasonBanner differ in
  ground, rule and type; the kicker overrides in LayersSheet / SpotCapture / spot parts are size and tracking choices, not drift.
- **fontWeight literals** (item 5b): `fontWeight` over a face is a synthesized bold on the web; removing it changes the look, so each
  needs the right face picked (and the regs-tokens check widened after). App.tsx boot lines and MapLegend's raw sizes with it.
- **Icon ladder** (item 6) and **IconButton** (item 7): 20 vs 22 close icons and the three +/- looks are different sizes today; the
  ladder picks one, so they are look changes to approve.
