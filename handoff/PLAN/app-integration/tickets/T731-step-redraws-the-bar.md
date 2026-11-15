# T731 — A step on a water's map closes the bar, resizes the map, then draws it all again

Status: IN REVIEW (in build 32 = 0.7.10, Play internal 2026-11-14; emulator looks passed; owner: OWNER-TODO "Phone check, build 32 = 0.7.10")

Source: owner 2026-11-14, phone, verbatim: "When you are in the navigate section view and you click upstream or downstream it
is like menus briefly close at bottom and cause a flash of a resize then rerender that is a bit abraisive"

## Problem
On a water's Map view (the part-by-part view: ▼ Downstream / ▲ Upstream under the map), pressing a step made the bar
under the map go blank for a moment (the steps, the name and "Show regulations" replaced by a spinner), the map grew into
the room it left, then the bar came back and the map shrank again. Wanted: the step changes the part in place; the bar
stays mounted at its height; no resize, no blank frame; the map glides to the new part.

## Cause
`app/packages/ui-native/src/water/WaterMapView.tsx` (at 203254660: line 107, the status, and line 218, the spinner drawn in the bar's place): the bar draws
`status === WATER_MAP_WORDS.loading` as a `LoadingState` in place of every control. A step that goes on into ANOTHER water
(▲ / ▼ "Upstream into …" / "Downstream into …", `onCross` → route `cross`; a split lake's other area, `partAction` with an
item) moves the frame to a water whose group and frame rows are not read yet, so `useWaterMap`
(`app/packages/ui/src/waterMap.ts:532` `useWaterMap`, `group.state === "loading" || waiting`) returns loading for that read.
The bar was replaced by the spinner (shorter), the map (`flex: 1` above the bar) was laid out taller (on a phone the native
map refits on a size change, `Map.native.tsx` `onLayout` → `refit`), then the new part's bar came back at full height.
Measured in jsdom through the real Shell (Chilliwack ▼ into the Vedder): every bar control removed, `water-map-status`
added, then all re-added. A step inside one water reads nothing new and was already drawn in place (same probe: no
node lost), so the owner's flash is the step into the next water; a phone look on the next APK confirms whether the
in-water step shows anything on the device (the native map's glide only, expected).

Not the T716/T717 hold: a step does not change whether a water is open, so `usePaneHold` / `useBrowseKept` are not
involved (checked against T717's uncommitted Shell.tsx / paneReveal.ts; no change needed there).

## Fix
`@app/ui` `useKeptWhilePending` (`app/packages/ui/src/async.ts`): the last answer drawn stays on screen while the next is
read, marked `kept`; nothing kept before a first answer, and never past a limit (then the caller's own waiting state, so a
slow read is said, never hidden). `WaterMapView` draws the bar and everything the map is handed (lit sections, water line,
marks, fit, receiving water, in-season outline) from it with `PANE_HOLD_MS` as the limit: through a step's read the bar is
the part stepped from, inert (`pointerEvents="none"`, map presses ignored), `aria-busy` to a screen reader, the mark pop and
chooser not redrawn; then the new part is drawn into the same nodes and the map's fit moves to it. A first open is
unchanged (the waiting state, held over the browse map, T508). One load path: the final frame is the same as before for
every way in; only the in-between frame changed.

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
