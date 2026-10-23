# T150 — Drop the regulations header's "Parts" button; the part list lives in the Map view

Status: DONE — 410dbc63 (with T151: header, Map view list, golden exclusion, decisions, shots), 08b842d0 (fixtures, text-scale snapshot); adversarial review 56c68c19 (focus after a pick, back pinned, one copy of the words, source pins out of core).

Source: owner 2026-10-23: "dont we want to remove 'parts' from reg page since map is now at the top tabs?" Follows owner
call 3 (decisions.md 2026-10-22, T71: the view strip is the one Map entry; the header lost its Map button but kept
Parts when the picker has a choice — core/src/regs/header.ts).

## Work
1. The regs header offers no Parts action (core header.ts, ui + ui-native); it keeps the part's NAME, so the reader
   always knows which part's rules are shown.
2. The "Parts of this water" list moves into the Map view (compact, under/over the map when the water has > 1 part):
   picks a part by name — tiny parts are hard to tap and a screen reader cannot tap a map. Tapping a part on the map
   still works.
3. Golden/conformance: the golden header's Parts action becomes an exclusion with this decision as its reason (like
   GOLDEN_MAP_ACTION_IDS for the Map button); header.parity asserts the new rule.
4. decisions.md: record it under owner call 3.

## Acceptance
No Parts button on any regs page; every part reachable by name from the Map view (web + native, light + dark, screen
reader label); shots of a many-part water (Fraser, Kootenay Lake). Size: S–M.

Owner question: the list's placement in the Map view (default: a collapsible list under the map).

## As built
- core `HeaderVM` has NO `action` field any more (`headerOf` returns name, day, date, aria, dot, place, tabs); words
  `PARTS` / `PARTS_ARIA` removed. The place label (the part's name) is unchanged.
- ui-native `RegsHeader`: no action slot (the 44 px slot is gone — the name row is now the frame's `WaterTitle` + the
  date pill, the same box the Conditions / Map views have); `RegsScreen` loses `onParts`.
- The Map view's list (Q122 as decided): `@app/ui` `waterMapView(...).list` — `{title, toggle, open, rows}` for a water
  of more than one part (null for one part), rows in the group's mouth-first order (a split lake's closure areas
  too), `here` on the part shown, `aria` from core `partRowAria` ("<name>, part 2 of 4 · Chilliwack River"). State:
  `WaterMapState.list` + action `{type: "list"}` (closed at the start and after a pick; opening it closes an open mark
  or chooser; a one-part water ignores it). A pick is the existing `{type: "part", ix}` step, so the frame's part,
  the Regulations view and Conditions follow (one water screen). A row names its water only when the list holds more
  than one water.
- ui-native `WaterMapView` `PartsList`, placed directly under the map at the top of the bar: one 44 px toggle (list
  glyph, "Show all 15 parts" / "Hide the list of parts", `aria-expanded`), and open a bounded (232 px) scroll of
  44 px rows as a `radiogroup` "Parts of this water" with `radio` rows (`aria-checked` on the part shown, ✓ drawn),
  normal weight; back closes the open list first (`useBackLayer`). Tapping a part on the map still works.
- Conformance: `GOLDEN_PARTS_ACTION_IDS` + its EXCLUDE entry (reason: owner call 3, 2026-10-23, T150, Q122) beside
  `GOLDEN_MAP_ACTION_IDS`; header.parity asserts on EVERY state that the golden's action (if any) is excluded by its
  kind's entry (T71 for Map, T150 for Parts) and the app's header has no `action` key. exclusions.test proves the two
  lists exactly cover the golden states with an action and do not overlap. The T115 grown-part exclusion is narrowed
  to `^place\.(full|short)$` (its `action.aria` half quoted the gone button).
- The Parts SHEET (core `partsSheet`, sheet key `parts`, still golden-compared by sheets.parity) is no longer opened
  by the app: only the web start link `sheet=parts` reaches it. Left in place (see Left / follow-ups).
- decisions.md: "EXTENDED by the owner 2026-10-23 (T150, Q122)" under owner call 3 (D2 / D24 section).
- Tests: core header.test (whole VM, no action key, mutation: re-adding `action: null` fails it and 130+ header.parity
  states); ui waterMap.test (the Chilliwack's whole list VM, toggle / pick / mark-closing, a split lake, one part);
  WaterMapView.test (toggle words, radios checked, regular face only, a pick hands the frame `part gnis:8634 0`);
  WaterMode.test through the real Shell on the fixture release (list → pick part 2 → Regulations reads part 2);
  RegsHeader / RegsScreen / RegsWide / Edges render tests (no slot, no button). Mutation: removing the `PartsList`
  mount fails WaterMode.test and WaterMapView.test.
- Shots (`T150-shots/`, own Expo port 39561, live release via :39217): `before|after--{fraser,kootenay,chilliwack}--
  {phone,desktop}--{light,dark}--regs.jpg` (the header with / without Parts) and `after--*--map-parts.jpg` (the list
  open, full page). Kootenay Lake's areas never had a Parts button (one choice per area item); its list is the 3 areas.

## Checks
See T151 (run together): full `pnpm -C app check` green except the 4 known x4p_1010 regs parity failures (T100).

## Ships with
App build only. No pipeline, bundle or release change.

## Left / follow-ups
- The Parts sheet (core `sheets/parts.ts`, its golden comparison, the "choice" block in `SheetBody`, the `onPart`
  plumbing in RegsScreen / WaterScreen) is reachable only through the start link now. Follow-up: delete it and add its
  golden `parts` sheets to the exclusions with this decision as reason — or keep it as the start-link test affordance.
- The list shows names only, not each part's open / closed word on the day (the sheet had one, per picker choice of one
  item; the Map view's group spans items and its parts are not picker choices). A per-part status word would need a
  per-part day read for every item of the group.

## Adversarial review (2026-10-23)
Held against the owner's words ("remove 'parts' from reg page since map is now at the top tabs"; keep the part's NAME;
the list in the Map view, collapsible under the map, Q122). Verdict: as built is right; three defects fixed, one false
pin replaced.
- **Screen reader, live (Expo web, own port 39591, the release through :39217):** the regs header of the Chilliwack has
  no `regs-action` and keeps the part's name ("From the Vedder Crossing Bridge up to the Tamihi Rapids Bridge"); in the
  Map view the toggle is a `<button>` "Show all 4 parts", `aria-expanded` false → true; the open list is
  `role=radiogroup` "Parts of this water" with 4 `role=radio` rows, each named whole ("Tamihi Rapids Bridge → 100 m below
  the Slesse Creek confluence, part 2 of 4 · Chilliwack River"), `aria-checked` true only on the part shown, every row
  focusable and ≥ 44 px. Every part of the Fraser (15, including "Within Landstrom Bar sign zone" and "2
  stretches") and Kootenay Lake's 3 areas is reachable by name.
- **Fixed — focus dropped on a pick:** a pick closes the list, so the focused row unmounted and keyboard / screen-reader
  focus fell to the page top. Focus now goes back to the toggle (`WaterMapView` `PartsList`); verified live
  (`document.activeElement` = the toggle after picking part 2) and pinned in WaterMapView.test.
- **Fixed — Android back unpinned:** the list's `useBackLayer` had no test. New WaterMapView.test drives the real
  `useBack` + `useRoute`: back closes the open list (route stays `water`), the next back leaves the water.
- **Fixed — a second copy of the words:** `WATER_MAP_WORDS.parts` spelled "Parts of this water" again beside core
  `WORDS.PARTS_TITLE`; it now reads that one value.
- **Fixed — the earlier review's source pins broke the layer gate:** they read files with `node:fs` from
  `packages/core` and `packages/map` tests, which `check-boundaries` refuses. Moved to `app/tools/one-parts-list.test.ts`
  (the words spelled once; no app surface opens the Parts sheet or names `onParts` / `PARTS_ARIA`; the mark-label face
  read by both maps). Core / map tests keep the value assertions only.
- Golden: `GOLDEN_PARTS_ACTION_IDS` carries the reason "owner call 3 (2026-10-23, T150, Q122)"; header.parity asserts
  on every state that a golden action is excluded by its kind's entry and the app's header has no `action` key;
  exclusions.test proves the two id lists exactly cover the golden states with an action. Read, not mutated again (the
  builder's mutation of `action: null` stands).
- Mutations run (each failed its named test, then reverted): the second literal back → one-parts-list "spelled once";
  `openSheet("parts" as SheetKey)` back in RegsScreen → one-parts-list "no app surface opens the Parts sheet"; the
  focus line removed → WaterMapView.test "T150: the parts list…"; `useBackLayer` removed → WaterMapView.test "Android's
  back closes the open parts list".
- Shots: `T150-shots/review--{fraser,kootenay,chilliwack}--{phone,desktop}--{light,dark}--{regs,map-parts}.jpg`
  (`T151-shots/shots.mjs`, OPEN_PARTS=1): no Parts button on any regs page; the list open in every combination.
- Seen, not fixed (follow-up / owner): opening the list shortens the map but does not refit it, so on a phone a long
  part (the Fraser's mouth → Mission) is partly cut off until the reader pans or closes the list. A refit on open would
  move a map the reader may have panned.
- Checks: `REGS_STORE_REQUIRED=1 pnpm -C app check` via scripts/heavy.sh (on the review change): every gate before `test`
  green (boundaries now clean again); vitest 8039 passed, 4 failed, 24 skipped — the same 4 known x4p_1010 failures from
  T100 (caveats.parity ×2, keep.parity, sheets.parity), nothing else.
- Commit: 56c68c19.
