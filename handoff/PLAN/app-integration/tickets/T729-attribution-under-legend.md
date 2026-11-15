# T729 — The opened credits card is under the legend and cannot be read

Status: IN REVIEW (in build 32 = 0.7.10, Play internal 2026-11-14; emulator looks passed; owner: OWNER-TODO "Phone check, build 32 = 0.7.10")

Source: owner 2026-11-14, verbatim: "attribution on app goes under the legends on the different map tabs."
Clarified by the owner 2026-11-14 (dialog), verbatim: "i meant when you open the atrribtuion clicking on it it opens under the legend on the map so you cant read it"

## Problem
Tap the © chip on a map tab (Map, Conditions, Run timing): the credits card opens, but the legend (and the Runs / gauge
keys) are drawn over it, so the credit text cannot be read. The licence text must be readable when asked for.

## Cause
`map/src/MapCredits.tsx` drew the card inside the map component. The Shell's foot block (`ui-native/src/Shell.tsx` `map-foot`,
zIndex 1) holding the legend, `RunsKey` (right edge, exactly where the card opens) and `GaugeOnlyKey` is a SIBLING of the map
painted over it, so anything inside the map is under them whatever its coordinates. (The chip itself was correctly lifted
above the foot by `insetBottom`; the opened card was the one covered.)

## Fix
The open card is a `Modal` (one rule, native and web through the same component): above every overlay whatever the legend's
height or state, centred, width and height capped to the window (the credit lines scroll if the window is short), square
card, our type, light/dark chrome, Esri's wording included (it is in `creditLines`). Closing: Close (unchanged), plus a tap on
the dim and the system back. The chip, its place, the startup credit and `creditLines` are unchanged. No Shell.tsx change.

## Acceptance
- `MapCredits.render.test.tsx` "the open card is above the app's overlays": the card is in a modal layer, not inside the
  map's corner block; satellite's "Powered by Esri" line is in it; the dim closes it. Fails before the fix.
- Left (Claude), emulator look on the next APK: Map, Conditions, Run timing x legend open / folded x light / dark x satellite
  on (card whole, readable, links open the licences); the water page's map.
