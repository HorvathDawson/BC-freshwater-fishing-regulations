# T731 — A step on a water's map closes the bar, resizes the map, then draws it all again

Status: IN PROGRESS (in build 32 as first built; one behaviour changed after the owner's 2026-11-16 answer "we should fix anything that doesnt seem desirable": 8e1791c7e on branch `app-fix/small-looks-1116`, merged onto `redesign/stream-sections` 2026-11-16 (265fae08a; benches 572a6957a); the steps' placement later reworked by T875 (DONE, 1c0911f91, 93efeda60); left (Claude): the emulator look and the test bed scenario `step-in-place`)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T731 | A step on a water's map closes the bar under it, resizes the map, then draws it all again | IN PROGRESS (in build 32 as first built; the 2026-11-16 change 8e1791c7e merged onto redesign/stream-sections 2026-11-16 via `app-fix/small-looks-1116`; left (Claude): emulator look, test bed `step-in-place`) | b40336850, 8e1791c7e | owner 2026-11-14 phone ("menus briefly close at bottom and cause a flash of a resize then rerender"); code + tests done; left (Claude): emulator look on the next APK, test bed `step-in-place` owed |

Source: owner 2026-11-14, phone, verbatim: "When you are in the navigate section view and you click upstream or downstream it
is like menus briefly close at bottom and cause a flash of a resize then rerender that is a bit abraisive"

## What
On a water's Map view, a step into ANOTHER water (▲ / ▼ "Upstream into …" / "Downstream into …", `onCross` → route
`cross`; a split lake's other area) blanked the bar under the map to a spinner (`WaterMapView.tsx` drew
`status === WATER_MAP_WORDS.loading` as a `LoadingState` in place of every control while `useWaterMap`,
`app/packages/ui/src/waterMap.ts:532`, read the new group), so the map grew, then shrank back. A step inside one water
was already in place.

Done: `@app/ui` `useKeptWhilePending` (`app/packages/ui/src/async.ts`): the last answer stays drawn (inert, `aria-busy`)
while the next is read, never past `PANE_HOLD_MS`; the new part is drawn into the same nodes (b40336850). After the
owner's 2026-11-16 12:08 answer, verbatim: "we should fix anything that doesnt seem desirable": the bottom bar keeps its
height stepping into a one-part water (was 122 px shorter); a water opened fresh carries no empty row (8e1791c7e,
`app-fix/small-looks-1116`, merged 265fae08a; benches 572a6957a: regs-open's two slow first taps, 160 / 154 ms, were
the first run after an install, A/B passed). Placement later reworked by T875.

## Acceptance
- `app/packages/ui-native/src/StepInPlace.shell.test.tsx` (real Shell, regs fixture release): ▼ Chilliwack → Vedder and ▲
  back, and ▲/▼ inside the Coquitlam — no bar node (bar, where, name, both steps, Show regulations) and no page node
  (`water-map`, `water-back`) is ever removed, the same DOM nodes before and after, the loading line never drawn, the map
  never remounted, never handed an empty part, its insets constant. The cross case FAILS without the fix (every control
  lost, loading drawn); the in-water case passes either way (it was already in place).
- `app/packages/ui/src/async.render.test.tsx` `useKeptWhilePending`: waiting state before a first answer, kept while the
  next is read, never past the limit.
- Left (Claude), emulator on the next APK (T717 holds the emulator now): test bed scenario `step-in-place` owed — open the
  Chilliwack's Map view at its mouth part, ▼ into the Vedder, ▲ back, ▲ / ▼ inside the Chilliwack; record frames
  (`pane-frames.py`) and FAIL on any frame where the bar under the map is missing or shorter, or the map's size changes;
  then a look at the same on the phone.


## Emulator look 2026-11-20 (L4, `small-looks`, build of 124d79efa)
Chilliwack River's Map view, part 1: down into the Vedder River and up back. The bar under the map measured 857 px before, after
the step down and after the step up. A recording of the step down, cut into 10 frames a second (`handoff/PLAN/app-integration/L4-android-shots-2026-11-20/T731/down-step-frames-10fps.png`),
shows the old bar and map for the whole read and then one frame where the title, map and bar change together: no spinner, no blank bar,
no resize. Shots `1-chilliwack-part-1.png`, `2-after-down-into-vedder.png` (the one-part water keeps the bar's two buttons: "Lowest
part" dashed, "Upstream into Chilliwack River"). The `step-in-place` scenario was written as the T731 step of `small-looks`.
