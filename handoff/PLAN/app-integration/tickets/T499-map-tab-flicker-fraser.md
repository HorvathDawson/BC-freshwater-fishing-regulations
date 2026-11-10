# T499 — Map tab in the rules pane flickers, left open on the lowest Fraser stretch

Status: DONE — 0fee60f85, c23affc58, 3d807829d; owner 2026-11-09 on build 24: fine

Was: IN REVIEW — 0fee60f85 (+ testbed c23affc58), in build 22 (3d807829d); owner: OWNER-TODO "Phone check with build 22" (Map tab on the lowest Fraser stretch).
Earlier: IN REVIEW — 0fee60f85 (+ testbed c23affc58); owner sees it on the next APK. Owner 2026-11-08 (phone, 0.6.9). Part of T495 (smooth map).

With the pane's Map tab left open on the lowest Fraser stretch, the map flickered now and then with no input.

## Do
- Reproduce on the emulator (leave it open several minutes; log renders, camera and style/filter/feature-state writes).
- Find what rewrites the map while idle (timer, feed refresh, tile reload, re-render loop) and stop it.

## Done (2026-11-08)
- **Reproduced** (emulator, testbed `map-idle`: Fraser → Map → Part 1, the lowest stretch, hands off 6 min, recorded):
  ~4.5 min in, the whole lit part vanished for one frame. Pan probe at that second: map renders 2, highlight re-sent 2,
  no camera / style / tile change.
- **Cause**: the 5-minute feed poll re-rendered the water map's view model, which handed the map the same highlight
  ids in new arrays; the highlight effect was keyed on the arrays, so it cleared every lit section and lit it again —
  separate feature-state writes on a phone, one frame drawn unlit.
- **Fix**: both maps key the highlight on what is lit (`sections.ts` `litKey`). Contract test (web + native): equal
  highlight in new arrays writes nothing (fails without the fix).
- **After**: same poll at ~4.8 min, renders 2, writes none; no map-body change in 6 min. Shots:
  `shots/T499-before.png` (frame before / the blink), `shots/T499-after.png` (the seconds around the poll).
