# T566 — Switching between the maps causes issues sometimes (owner, Pixel 9, build 24)

Status: IN REVIEW — f856e42f2 (key follows the map; reproduced on web and the emulator); owner phone check on the next build

Source: Owner 2026-11-09 (owner checks page, T512): "it is okay but not perfect. it is good to pan around, lags a tiny bit rarely but switching between the maps causes issues sometimes."

## Work
1. Ask the owner what "issues" is (blank map, stale layer, lag, crash) and between which maps (main map ↔ a water's Map tab, Regulations ↔ Conditions view).
2. Reproduce on the emulator (owner rule: simulate, not on his phone): a scenario that switches maps 20 times and records frame gaps and blank frames.
3. Fix what it shows. Related: T401 (edge pop-in, minor, later), T360 (memory).

## Acceptance
The emulator scenario runs clean; owner says fine on the next build.

Depends: —. Size: M. Owner: no.

## Owner answer 2026-11-11
It is the Trout / Salmon / Runs buttons on the main map: a blank or grey map, lag, and layers "stay wrong for half a second or so". Reproduce in the emulator (20 switches, record frames), then fix.

## Found and fixed (2026-11-11, f856e42f2)
**Reproduced** on both. Web (own headless Chrome, 390 px, 4x CPU throttle, 48 switches Trout/Salmon/Runs/Map in two
orders): every switch drew 2-3 frames (up to ~235 ms) of the PREVIOUS colouring while the key already showed the new
one (pixel histograms of the frames: trout colours under "Keep salmon", and back). Emulator (test bed `regs-switch`, new
scenario, 48 switches, screen recording cut by `app/tools/testbed/regs-switch-frames.py`; APK b26): the new key showed
over the old picture 66-101 ms on 36 of 48 switches; Runs → Map showed the Runs map's grey streams under the Map key for
4 frames every time — the owner's "grey map". No blank frames and no hold that ran out (48 holds, all ended by a drawn
frame, p50 91 / max 197 ms).
**Mechanism:** a Trout / Salmon / Runs switch is a data-driven paint change on the stream layers, so every tile on
screen is laid out again (GL JS reloads the atlas; MapLibre Native is handed a new style and holds the old picture,
T451). Until each tile's new layout lands, the renderer draws it from its OLD bucket, in the previous colouring. The key
under the map and the buttons changed at once, so for that time they described colours the map was not showing. Not a
fetch, not feature-state loss, not a style reload of the basemap.
**Fix:** the map says when it has drawn the look it was handed (`MapProps.lookKey` / `onLookDrawn`, `look-drawn.ts`:
web at the first frame or tile with every atlas tile laid out after the paint; phone at the first full frame after the
new style loaded — the frame the held picture lifts at). The key (MapLegend, RunsKey, RunsLegend) is held at the look
the map last said it drew (`@app/ui` `HeldLookStore`, re-rendering only the key, `HeldKey`), and the pressed rail
button shows busy meanwhile. No fallback: a look the map never says it drew keeps the key on the colours on screen.
**Tests:** `packages/map/src/look-drawn.test.ts` (frame-by-frame sequences, no clocks), `packages/ui/src/heldLook.test.ts`;
mutation-checked: the web watch without the atlas check, the phone tracker without the style-load wait, and a key that
moves on the ask each fail their tests.
**Before / after:**
| | before | after |
|---|---|---|
| web 4x CPU: frames with the new key over the old colours | 2-3 on every switch (24 of 24) | 0 (48 switches) |
| web: key on screen after the press | ~120-170 ms (before the map) | ~220-340 ms (with the map; the map itself unchanged) |
| emulator: switches with the new key over the old picture | 36 / 48, up to 101 ms | 16 / 48, up to 33 ms (Runs tab switches only: 1-2 frames) |
| emulator: Trout ⇄ Salmon | 66-84 ms stale every time | 0 |
| emulator: key behind the map (old key over new colours) | — | p50 24 ms, max 82 ms |
Runs: `app/tools/testbed/out/runs/t566-before` (b26), `t566-after3` (patched lean MapLibre, this commit).
**Left / not verified:** the owner's Pixel 9 (the emulator is a fast Mac; the owner saw ~0.5 s); the key now changes up
to ~80 ms AFTER the map on the emulator (a React render behind the native frame) — the old colouring is never under the
new key, but for a frame or two the new colours are under the old one; the residual 1-2 frames on Runs switches are
other foot chrome moving with the tab; iOS not run; the b26 APK's MapLibre (stock or lean) not checked against the
after APK (lean).
