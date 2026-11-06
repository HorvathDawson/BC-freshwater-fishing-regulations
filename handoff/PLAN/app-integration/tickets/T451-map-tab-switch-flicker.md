# T451 — Switching to the Map tab, the streams flicker (Pixel 9, 0.6.6)

Status: OPEN (owner 2026-11-06, 0.6.6 on Play internal, Pixel 9).

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
