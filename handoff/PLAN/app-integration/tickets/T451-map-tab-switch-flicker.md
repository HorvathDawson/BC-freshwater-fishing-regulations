# T451 — Switching to the Map tab, the streams flicker (Pixel 9, 0.6.6)

Status: IN REVIEW — e63c921c3, 507de9433 (test bed: 0 uncoloured frames entering Map, was 12/12); owner phone check left

Owner: "switching between Conditions and Runs has no flicker, but sometimes going to Map it flickers a bit (the streams
etc flicker, not the base map) … maybe the colours when switching should get ready before it switches to avoid flicker."

## Work
1. Reproduce on the emulator test bed: switch Conditions / Runs / Map repeatedly; count style swaps per switch and the
   frames drawn between a style load and its labels landing (pan probe), plus a screen recording's frames.
2. Find the cause (suspects: a tab switch hands the binding TWO styles — the new modes first, the channel values one
   render later through `useNativeColouring`'s state; each swap drops feature-state until the relabel lands).
3. Fix so the new tab's colours are ready before it is shown (or the old ones stay until the new ones land): no
   grey/blank frame. Web too if it reproduces there.
Related: T420, T287 (style swap by content). Size: S–M. Owner: a phone check after.

## Found and fixed (2026-11-06, e63c921c3)
**Cause:** a tab switch handed the binding TWO styles. `useNativeColouring` carried each channel's style-borne values
(the day's profiles, salmon/method days, the run field) through React state set by the engine's `setChannel` in an
effect, so the render that changed the tab's modes handed a style with the new modes and the old (or no) values, and
the values arrived one render later as a second style. MapLibre drew the first: the water in plain grey/no colours for
a frame or more (also Conditions → Runs, where the field went grey). Feature-state was NOT the cause: it survives a style
swap (probe: 29/29 and 35/35 kept).
**Fix:** each channel's input is memoised once and its values read from it in the same render (`values` memo); the
engine's adapter no longer writes state. One style per switch.
**Measured** (test bed `switch`: 4 rounds × 8 switches Map/Conditions/Runs, normal emulator, pan probe `[swap]` +
screen recording; runs `out/runs/t451-a` before, `t451-b` / `t452-c` after):
| | before | after |
|---|---|---|
| style loads for 32 switches | 42 | 32 |
| loads that followed 2+ hand-overs | 30 | 0 |
| switches with a recorded frame off the before→after path (detour > 0.3) | 21 / 32 (max 2.04) | 0 / 32 (max 0.10) |
| load → labels acknowledged | p50 54 / max 96 ms | p50 39 / max 50 ms |
Picture of a before frame: `out/runs/t451-a/T451-flicker-before.png` (Conditions → grey frame → Runs).
The relabel on every style load (`swapped()`) is redundant now that states are known to survive; left in (harmless).
Web not changed: GL JS paints channel values imperatively, no style swap. The run's camera sat at the zoomed-out field,
not stream zoom; streams use the same values path.

## Owner phone check (2026-11-06, 0.6.7): not fixed
"The map switch still flickers, mostly between Conditions/Run timing into Map, not so much between Run timing and
Conditions. Flicker is maybe better." So the one-style-per-switch fix helped but is not the whole cause. What is left
happens when entering MAP from a colour mode (Conditions = flow colours, Runs = run-timing colours) and not between
those two modes. Next: on the emulator, record the frames of a Conditions→Map switch (map surface presents + style
events + feature-state writes). Find what draws a frame in the wrong colours: map mode's own style landing after a
render of the old paint, feature-state cleared before the new values, or the stream layer's visibility/filter changing
before its colours. Fix that, and before/after use the same 32-switch script with Conditions→Map and Runs→Map
counted separately.

## Found and fixed, round 2 (2026-11-06, 507de9433)
**Measured first** (the 32-switch recording of e63c921c3, `out/runs/t452-c`, counted per direction by
`app/tools/testbed/switch-frames.py`: a frame with the Map style's ground and under 30 % of the settled view's blue
lines): every switch INTO Map drew the streams uncoloured — Conditions→Map 8/8 (one frame each), Runs→Map 4/4 (12
frames, up to 67 ms); Map→Conditions, Map→Runs, Conditions↔Runs 0. The owner's words exactly.
**Cause:** not feature-state (it survives the swap: MapLibre keeps `SourceFeatureState`, and pushes it to every tile
every frame). A swap that changes a line layer's data-driven paint makes every tile lay out again, and until that lands
MapLibre 13.6.1 draws the NEW layer from the OLD bucket (`GeometryTile::LayoutResult::getLayerRenderData`, by layer id),
whose feature-state binders were built for the old paint: grey streams for 1–4 frames. Conditions and Runs at that
zoom draw the field (style-borne values, no binders), so switching between them never showed it.
**Fix (binding patch, Android):** `setReactMapStyle` copies the frame on screen (`PixelCopy`, 6–20 ms) over the map,
sets the style, and takes the copy away at the first fully rendered frame after the new style loaded (or the first
touch, or 1.5 s — logged `CifSwap`). And the native colouring queue merges a section's writes in one burst (a clear's
null write + its relabel are one write), so no frame between them draws it bare (Runs→Map drops the runs channel).
iOS is not changed (needs its own snapshot path; the owner's phone is Android).
**After** (`out/runs/t451-t459`, same script, same place):
| direction | before (t452-c) | after |
|---|---|---|
| Conditions → Map | 8/8 switches, 8 frames | 0/8, 0 |
| Runs → Map | 4/4, 12 frames (max 67 ms) | 0/4, 0 |
| Map → Conditions / Map → Runs / Conditions ↔ Runs | 0 | 0 |
Holds: 32 swaps, 32 ended by the drawn frame (none by the cap), hold p50 11 / max 159 ms.
Left: the owner's phone check (Pixel 9: Conditions/Runs → Map). Unverified: the hold on a real device (its first full
frame comes ~10 ms after the load on the emulator).

