# T451 — Switching to the Map tab, the streams flicker (Pixel 9, 0.6.6)

Status: PARTIAL — e63c921c3 (owner on 0.6.7: still flickers, "maybe better")

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
