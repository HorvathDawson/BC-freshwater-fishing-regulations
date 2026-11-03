# T360 — App memory footprint

Status: PARTIAL — code read done (findings + corrections below); measurement on the emulator is next (RESUME)

Owner 2026-11-03: "could we look into the memory footprint of the app". Evidence: the two 0.4.0 foreground native
crashes (T351) happened at 406 MB and 435 MB in use (one SIGSEGV); background LOW_MEMORY kills at 169 MB (0.3.0, 0.4.0).

## Work
1. Read-only first: where memory goes (map tiles / MapLibre caches, the bundle sqlite, salmon/regs stores held in JS,
   images, the synopsis PDF), from code and from an emulator run (`adb shell dumpsys meminfo`) across map pan, a water
   sheet, Conditions, Salmon, the PDF.
2. Report the top contributors with numbers, then propose fixes as follow-up tickets.

## Read-only findings (2026-11-03, code only; sizes are estimates until measured)
1. MapLibre native GPU/tile memory, est. 150–250 MB: `nativeVariant: "vulkan"` (apps/mobile/app.json:29, fits the
   SIGSEGV); no ambient/tile cache limit set anywhere; 49 layers over 4 sources, atlas overzoomed to z18; feature-state
   on atlas + a copy source (Map.native.tsx:141-170, 340-342); Esri imagery tiles while satellite is on.
2. Synopsis PDF spike, est. 100–200 MB per rule-book tap: the 24 MB file read into a JS Uint8Array
   (synopsisPdf.native.ts:98-100), parsed by pdf-lib (highlightPdf.ts:17), a second 24 MB copy written.
3. Decoded release data cached in JS for good, ~10–30 MB: paint (2.2 MB), flowArea, method, access, lakes, every
   salmon shard; the salmon slice Map only grows (data/src/release/loader.ts ~115-190).
4. ~30 SQLite files each held open (`useNewConnection: true`, never closed), ~20–60 MB page cache/mmap.
5. JS tile bookkeeping is capped (TileHold 512) — not a leak.

Quick wins to try after a measurement: cap MapLibre's ambient cache + tile count; try the OpenGL variant against the
SIGSEGV; drop imagery sources when the basemap is "map"; open the PDF by streaming (or skip highlighting above a size);
free paint/method decodes and close unused SQLite handles on `onTrimMemory`.

Measurement (next): `adb shell dumpsys meminfo <pkg>` per step (boot, pan ×10, zoom, water sheet, Conditions, Salmon,
rule-book PDF, then `am send-trim-memory <pkg> RUNNING_CRITICAL`), satellite on and off; compare Graphics + Native Heap
(map) with Java/Hermes heap.

## Checked by the main session (2026-11-03) — corrections and the plan
- **Vulkan:** `["@maplibre/maplibre-react-native", { "android": { "nativeVariant": "vulkan" } }]` (apps/mobile/app.json).
  It was PROVISIONAL from the N1 spike ("Vulkan provisional pending a real phone", app-data/n2/README.md, N1-SPIKE.md)
  and was never judged on a phone. The SIGSEGV (status 11) makes the OpenGL variant the first thing to try.
- **Correction:** MapLibre's "ambient cache" (`OfflineManager.setMaximumAmbientCacheSize`, `setTileCountLimit`) is the
  DISK tile cache. Capping it does not lower RAM. RAM is driven by the tiles in view, overzoom depth, layer count,
  feature-state and imagery; measure before tuning.
- **PDF (confirmed):** `openHighlighted` (ui-native/src/regs/synopsisPdf.native.ts) reads the whole 24 MB book with
  `cached.bytes()` and `highlightPdf` parses it with pdf-lib (`PDFDocument.load`), then writes a second copy. A
  plain open (`pdf <= 1`, no boxes) streams the file. Same root cause as **T354** (slow open): fix them together.
  Options to put to the owner: (a) highlight a small extract (the cited pages only, with a "whole book" link);
  (b) pre-build the highlighted pages at release time (pipeline), the app opens a small file; (c) skip the highlight
  above a heap budget. Each one changes what opens, so the owner decides.
- **Release data / SQLite (not yet verified):** loader caches (data/src/release/loader.ts) and ~30 open sqlite handles
  — confirm with the measurement before changing anything.

## RESUME (next chat)
1. Build a debug/release APK of the current tree and install it on the `n1_pixel` AVD
   (`~/Library/Android/sdk/emulator/emulator -avd n1_pixel`, adb at `~/Library/Android/sdk/platform-tools/adb`;
   T255 test bed: `app/tools/testbed/README.md`). Heavy: through `scripts/heavy.sh <label> 60 -- …` in the background.
2. Measure with `dumpsys meminfo ca.canifishthis.app` per step: boot, 10 pans, zoom in to z16, satellite on, a water
   sheet, Conditions, Salmon, a rule-book tap (highlighted PDF), `am send-trim-memory … RUNNING_CRITICAL`. Record
   TOTAL PSS / Graphics / Native Heap per step in this ticket.
3. Repeat with `nativeVariant: "opengl"` (one prebuild + build). Compare memory and N1's pan budget (p95 ≤ ~20 ms).
4. Put the PDF options (a/b/c above) and the Vulkan→OpenGL result to the owner; then fix what the numbers show.
