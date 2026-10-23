# T150 — Drop the regulations header's "Parts" button; the part list lives in the Map view

Status: DONE — 167c3744 (with T151: header, Map view list, golden exclusion, decisions, shots), 2e92a987 (fixtures, text-scale snapshot).

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
