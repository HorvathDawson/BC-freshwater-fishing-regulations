# T420 — Panning the map feels laggy on web AND app (even a Pixel 9): streams load in slowly, then the pan stutters

Status: OPEN (owner 2026-11-05, on 0.6.1 from Play internal, Pixel 9: "the panning on the map still feels very laggy even on a pixel 9 phone … it is
like the streams load in slow then lag").

Two symptoms, maybe one cause: (1) the stream lines arrive late after a pan (tiles or their layers decoding/painting
slowly), and (2) the pan itself stutters once they arrive (frame drops while the new tiles are styled — main-thread or
render-thread work on tile load: paint-index lookups, feature-state, filter expressions, a re-render from React).

**On the web too** (owner 2026-11-05): the same lag on the web map, so the cause is likely shared — the tiles
(size, layer count, data-driven paint over stream features) or our style/JS — rather than MapLibre Native alone.
Measure both: the web with the Performance panel / MapLibre's `map.showTileBoundaries` + frame timing on the same pan
script, the phone as below.

## Work
1. Measure on a real device class (the owner's Pixel 9 report; emulator numbers are not enough): a scripted pan over a
   dense area (Fraser Valley at z10–z12, then Coquitlam/Harrison at z13) with frame timing (`adb shell dumpsys gfxinfo
   <pkg> framestats`, MapLibre's render stats) and time from camera stop to streams drawn. Record before numbers.
2. Find where the time goes: tile fetch/decode (local pmtiles read), style evaluation (data-driven expressions over
   stream features, the paint index / feature-state per section), JS work on `onMoved`/region change (T417 added a
   bounds report on settle; check it costs nothing mid-pan), React re-renders of the map tree during a gesture.
3. Fix what the measurement names; after numbers on the same script. Target: no dropped-frame bursts during a pan,
   streams drawn within ~0.5 s of the camera stopping.
Related: T401 (streams pop in at the edges — the pan buffer), T360 (performance pass), T245 (no lighter z8: decided).
Size: M. Owner: a phone check after (and a screen recording of the lag would help).
