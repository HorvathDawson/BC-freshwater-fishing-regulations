# Conditions map: how the gauges reach the place you are reading

Owner, 2026-10-21, about the map in a section's Conditions view:

- "it should show downstream and upstream separately"
- "the icon for where you are should be different than gauges"
- "the map should say how much of the contribution the gauge gives"
- "if it is zoomed out too much the stream ... dont even draw ... copy geometry on click"
- "the gauge map should have the ability to zoom" (for both the chart and this map)

## What changed

1. **Upstream and downstream are drawn differently.** If the gauge is downstream of you, its route is a **solid** line.
   If the gauge is upstream, its route is a **dashed** line. Each route uses its gauge's own colour, which is the same
   colour as that gauge's row in the panel. Small arrows on every route point in the direction the water flows. The
   key under the map explains solid and dashed, but only for the kinds of route actually on the map.
2. **"You are here" is a pin; gauges are dots.** The spot is a pin in the accent colour with a hollow centre and the
   words "you are here" under it. Every gauge is a round dot. The difference is in the shape, not only the colour,
   so it holds in both themes. Each gauge dot also has a faint ink ring. Without the ring, a gauge that is not
   reporting (dark grey) could not be seen on the dark map. A gauge at the spot used to be merged into the spot
   marker. Now both are drawn.
3. **Every gauge shows its share of the estimate**, for example "35%", or "not reporting". This uses `donorShare`,
   the same words the panel rows use, so the map and the rows always agree. Labels move to whichever side of the dot
   is free, and the gauge with the largest share gets its label placed first. A label is never dropped: when no side
   is free (a gauge at the spot, or a zoomed-out map), it is drawn on its first side anyway, overlapping. The key
   lists every gauge as `station · share`.
4. **The route still draws when you zoom out.** When a section opens, the app reads the route's section ids from the
   same atlas `.pmtiles` file the map draws. It reads them at a zoom level where those sections exist, decodes the
   vector tiles, and keeps only the features whose MVT id matches a section id. The result is drawn as a GeoJSON
   overlay that shows at every zoom. The geometry comes only from the tiles. There is no second geometry store.
   - **Work is capped.** First the app reads the route's box at the deepest zoom, from z8 to z12, that needs at most
     16 tiles. If a small creek is still missing, it reads one zoom level deeper in a 3×3 block of tiles around the
     end of the route that creek belongs to. It stops at z14 or 40 tiles in total.
   - **Results are cached** by section id (up to 4000 sections) and by tile (up to 64 tiles). Opening the same water
     again reads no tiles.
   - **Measured cost** in the live web app against the release atlas:

     | Section | Sections on the routes | Tiles read | Time |
     |---|---|---|---|
     | Chilliwack R. (4 gauges) | 5 | 5 | 80–116 ms |
     | Driftwood Cr. to the Skeena at Usk (2 gauges) | 18 | 16 | 93–410 ms (410 ms on a cold cache) |

     No sections were missing in either case.
5. **You can zoom and pan the map freely.** It has zoom buttons at the top right, plus pinch and the mouse wheel.
   There is no compass, so north stays up. **Fit** is at the top left and returns the camera to the spot and all the
   gauges. It moves the camera only when pressed, so it never fights your own panning. **Larger map** makes the map
   about square. The Fit and Larger buttons are at the top left because at the bottom left they covered a gauge far
   down the river (the Skeena at Usk, in `driftwood-*`).

## Screenshots

`before` is `redesign/stream-sections` at 36deb502 and `after` is this branch. Both ran against the owner's data
server, read-only. The zoom was set exactly, keeping the map's centre (`z9`, `z8`). `opened` is the camera the panel
opens with. Phone is 390×844, desktop is 1440×900.

- **Chilliwack River** (a section between Vedder Crossing and Slesse):
  - `chilliwack-before-{desktop,phone}-light-{opened,z8}.png`: at z8 the route is gone and only the teardrop markers
    remain.
  - `chilliwack-after-{desktop,phone}-{light,dark}-{opened,z8}.png`, `chilliwack-after-desktop-light-z9.png`: the
    downstream route to Vedder Crossing is solid, and the three upstream routes are dashed.
  - `chilliwack-after-phone-light-fit-pressed.png`: taken after zooming out to z7 and pressing Fit (z7 → z8.7).
  - `chilliwack-after-phone-light-larger.png`: Larger map.
- **Driftwood Creek** (north, near Smithers). Both gauges are downstream: the Skeena at Usk (100%) and 08EE001 (not
  reporting).
  - `driftwood-before-{desktop,phone}-light-{opened,z8}.png`: the panel opens at about z7.4, and **no route is drawn
    at all**.
  - `driftwood-after-{desktop,phone}-{light,dark}-{opened,z8}.png`: the whole route from the creek down the Bulkley
    to the Skeena is drawn.

My own Metro servers ran on :39301 (after) and :39302 (before) and have been stopped. I never touched the owner's
:39217 or :39218.

## Not done / not verified

- **Native.** The contract has `MapAdapter.setTrace`, and `baseAdapter` implements it for both platforms through an
  optional `MapHandle.setSourceData`. The overlay's sources and layers are in `runtimeStyle`, which both renderers
  share. `Map.native.tsx` (owned by N2) does not yet pass `trace` to `setTrace`. That wiring is N2's, and it has not
  been checked on a device.
- **Where "not reporting" appears.** The words "not reporting" appear on the map and in the key. When the gauge last
  reported is shown in the panel rows (conditions-layout), not on the map.
- **Share labels after the review fix.** The change that stops labels being dropped (overlap allowed after a
  free-side pass) is tested at the style level only. The screenshots above were taken before it. No new screenshots
  were taken, because that would mean running a Metro server again.
- **The flow chart's zoom** belongs to conditions-layout (Hydrograph / ChartControls) and is not part of this change.
