# T401 — Streams pop in at the screen edges while panning (phone)

Status: OPEN (owner 2026-11-04, 0.5.0 Play internal: "panning doesn't load enough of a buffer of streams outside the
view").

**What the code says:** nothing in the app can widen the area loaded. MapLibre Native loads only the tiles that cover
the viewport; its only prefetch is parent tiles (`prefetchZoomDelta`, default 4), and the binding exposes no knob
(T273's finding). The parent tile drawn while a new one loads holds only the bigger streams (`MAGNITUDE_LADDER`: a
magnitude-5 creek starts at z12), so when the real tile lands the small streams appear — the pop. The atlas has no
tippecanoe `--buffer` set (default 5/256): the buffer clips lines at tile edges, it does not load neighbours, so a
bigger buffer would NOT fix this — no tiles rebuild needed for it.

**Options (owner pick after measuring):**
1. Measure tile arrival on the emulator (`run.mjs pan`, pan probe): time from tile request (`MLRNTileIds`) to drawn.
   If the local archive read/parse is slow, the z13 tiles' size is the lever (rides with the T389 rebuild).
2. Patch the binding to expose `setPrefetchZoomDelta` (smaller delta = parent placeholders closer to the real tile,
   with more streams) — small native patch in `patches/`.
3. Render margin: the native map view drawn larger than the screen (tiles beyond the edge load early) — costs GPU/memory.

Depends: T312 (map feel). Size: S–M. Owner: choose after step 1.
