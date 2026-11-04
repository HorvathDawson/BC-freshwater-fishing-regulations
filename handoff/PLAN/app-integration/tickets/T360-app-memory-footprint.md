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

## Measured 2026-11-03 (emulator n1_pixel, Vulkan as shipped, local release r-4df5bccc9360; `run.mjs memory`)
MB, `dumpsys meminfo` App Summary. Graphics reads 0 on the emulator (host GPU): a real phone adds its GPU memory on top.

| step | PSS | Java | Native | Code | Other |
|---|---|---|---|---|---|
| boot (map drawn) | 255 | 13 | 155 | 40 | 35 |
| 10 pans | 287 | 15 | 181 | 40 | 41 |
| zoom +4 | 208 | 15 | 97 | 41 | 44 |
| water page (Cultus) | 292 | 23 | 137 | 37 | 83 |
| rule book open (in-app viewer) | 332 | 14 | 170 | 40 | 83 |
| Conditions | **392** | 18 | **229** | 37 | 83 |
| after trim RUNNING_CRITICAL | 375 | 18 | 214 | 34 | 80 |

- Native heap (map + SQLite + Hermes) is the whole story; Java is small. Conditions is the peak, ~400 MB before any
  GPU memory: the phone crashed at 406/435 MB. `onTrimMemory` frees ~17 MB only (nothing listens for it).
- The rule book costs ~40 MB in the viewer now (was the 100–200 MB JS spike, T354): no longer the top item.
- Next: the same with `nativeVariant: "opengl"`; then where Conditions' +100 MB native goes (its map + chart + feeds).

## Measured 2026-11-03, round 2 (heap probe + Vulkan vs OpenGL; runs t360-vk / t360-gl)
| step | Vulkan PSS | OpenGL PSS | JS heap |
|---|---|---|---|
| boot | 257 | 233 | 28 |
| water page (Cultus) | 280 | 264 | 72 |
| rule book open | 346 | 364 | 72 |
| Conditions | 454 | 436 | 76 |
| after trim | 414 | 411 | 72 |
- OpenGL vs Vulkan: 5–25 MB apart on the emulator (host GPU, Graphics 0): the variant/SIGSEGV question needs a REAL phone.
- JS: 28 MB boot → ~75 MB once a water's regs are decoded (held for good). Not the main cost.
- Conditions adds ~150 MB NATIVE over a water page (its map + gauge layers + chart): the biggest single item. NEXT: split it.
- The PDF viewer leaves ~60–75 MB native after it closes (react-native-pdf / pdfium not released on close). Fix: free on close.
- RUNNING_CRITICAL frees 20–40 MB: nothing listens for trim. Fix: drop decoded caches + close idle SQLite on trim.
- Rule-book open IN THE APP: book 3 ms (held), copy 6–39 ms, drawn in 390–680 ms. The 10 s the test bed reads is
  Maestro's own overhead.
Probe: `[heap]` every 2 s in test-bed builds (apps/mobile/index.js); `session.sh --opengl` builds the other variant.
- Correction (run t360-pdf): the viewer costs ~70 MB while OPEN and keeps ~35–40 MB after it closes (273 → 345 open →
  312 closed; a RUNNING_MODERATE trim frees nothing). Not 70 retained.

## Round 3 (2026-11-03, chat 5; run t360-cond)
- **Earlier rows were over satellite:** the `memory` scenario's second Satellite tap never turned it off (the ground
  is a choice, not a toggle), so water page / rule book / Conditions in rounds 1–2 had Esri imagery under them. Fixed:
  the Map base is chosen by its id after the satellite step.
- Over the drawn map: before the rule book 278 PSS / 103 native; **Conditions 422 / 245** (+140 native); Gauges off
  421 / 239; Gauges + arrows off 418 / 238; back on the Map tab 429 / 245 (nothing given back); Conditions again
  411 / 198; trim 394 / 188.
- So the cost is not the Conditions layers. JS is ~17 MB of it: the `[heap]` probe's `size` left out Hermes'
  EXTERNAL bytes (typed arrays, 37–68 MB through the run; they count as "Native"); JS total 116 → 133 MB. The rest
  (~120 MB) is MapLibre's. Next: heapprofd (`run.mjs heapprof`, `heapprof.py`) names it by library and function.
- **Bug found and fixed (02433a339):** Gauges off left the dots drawn (`useAsync` keeps its last answer for an
  unchanged key when disabled).
- **PDF:** the app holds none of the book in JS and react-native-pdf disposes the document on detach. The scenario now
  measures after a second open + close: not growing = a one-time pdfium cost, not a leak.
- **Trim (item 3), in code:** `modules/memory-trim` (Android) answers `onTrimMemory` (RUNNING_MODERATE and up):
  `onLowMemory()` on every MapLibre MapView (maplibre-react-native never forwarded it) + a `trim` event; JS
  (`src/memoryTrim.ts`) shrinks every open SQLite connection (`ReleaseLoader.trim`, `PRAGMA shrink_memory`, through
  the one SQL queue). iOS: the same shrink on `memoryWarning` (MLNMapView handles its own). Decoded files are not
  dropped (the screens hold them). Emulator check: run t360-trim.

## Round 4 (2026-11-03, t360-fix + t360-pdf runs)
- **Purge works (17d5f0dc3):** a RUNNING_CRITICAL trim: 1 map view told, `purge 2` (M_PURGE_ALL), 14 databases shrunk;
  PSS 394.8 → 378.5 MB (~16 MB). `mallopt` is declared from API 26 only: looked up with `dlsym` (first build failed).
- **PDF leak NOT fixed:** the react-native-pdf patch's `recycle()` runs but finds 0 pages left in `pageMap`; pdfium still
  holds 77.6 MB after two opens + closes, and only ONE recycle line was logged for the two closes — one viewer is
  likely never released (not a page leak). The patch stays uncommitted (`app/patches/react-native-pdf@7.0.5.patch`,
  package.json, lock). Next: log PdfView attach/detach + `PdfFile.dispose` per open; check the Modal unmounts the view.
  (Symbols are stripped in this build, so heapprofd names no pdfium function.)
- The memory scenario's "Salmon" layer tap no longer finds its text (label changed by the UI chat?) — fix the step.

## Round 5 (2026-11-03): the PDF leak's cause
react-native-pdf posts `drawPdf` from `setPath` AND from every props transaction (`PdfManager.onAfterUpdateTransaction`),
and our viewer passed a new `source` object each render: one open loaded the book 2+ times. Each load recycles the view
first; a load still decoding is only flagged cancelled — AndroidPdfViewer 4.0.1's `DecodingAsyncTask.onCancelled` sets
`cancelled` and nothing else, so the `PdfDocument` it opened (`createDocument` → `PdfFile`) is never closed (javap). Fix
(uncommitted until measured): the patch now skips a draw of the same path + page on a view not recycled since
(`drawnKey`; the pageMap closing is gone — it found 0 pages), and `PdfViewer.native.tsx` memoizes `source`. Measuring:
heapprof `t360-pdf2` (queued after the batch-3 rebuild).
**Measured (t360-pdf3, t360-pdf4, same APK):** pdfium held after the rule book opened and closed: 2.5 MB, then 0.9 MB
(was 50–77 MB). Log per open: one `drawPdf load`, two `drawPdf skipped` (the duplicate reloads that leaked), `detached`,
`recycle`. Committed. Open question: logcat shows ONE viewer's life per trace though the step opens twice (the earlier
patch also logged one recycle for two closes) — check the scenario's second open really happens (screens). t360-pdf2
(the same fix, no logs) still held 50 MB — most likely a stale library build in that APK; not seen since.
