# T417 — Watershed conditions: no flicker between watersheds; pick the watershed most on screen

Status: OPEN (owner report 2026-11-05 20:21 on 0.6.0 build 9, report dbc65e804d2692ba: More screen, regulations map,
z8.6 at 49.134,-122.776).

"When moving between watersheds it flickers when switching the schedule view for the watershed conditions. Instead it
should load the new one in the background and only move over after it is ready. Also it should use which watershed is
most in screen, so it won't show none when you are centered next to the watershed on an ocean for example."

## Work
1. Keep the current watershed's schedule on screen until the next one has loaded, then swap (no empty/loading frame).
2. Choose the watershed with the largest share of the visible map (not the one under the centre point); "none" only when
   no watershed is on screen. One load path (memory one-load-path-per-screen): the same result from pan, search or tap.
3. Measure on the emulator: pan from the Fraser lowland onto the Strait; no blank frame, the watershed stays.
Size: S–M. Owner: phone check on the next APK.
