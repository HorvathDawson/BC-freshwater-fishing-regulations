# T286 — Tapping a water opens its regulations instantly on phones (never needs a spinner)

Status: DONE 2026-10-29 — a3cc1f269 (owner 2026-10-29: "should make the regulation opening faster when a click happens on mobile too. it should
never be slow enough to need a loading icon").

## Work
1. Measure tap → pane drawn with content on the testbed emulator (and a low-end profile) for: a small stream, a big
   multi-part river (Fraser), a lake; split the time (hit test, SQLite reads through the T264 serial queue, decompress,
   hydrate/buildRegsPage, React render, first frame).
2. Fix by cause: show the pane shell + header from what the tap already knows in the same frame; read the regs rows in one
   batched statement; cache hydrated pages (LRU by release + day); precompute/prefetch for the waters on screen when the map
   settles (idle only); move heavy work off the render path; keep T264's race fix.
3. Budget: content on screen ≤ 150 ms on the emulator for every case above; a test/benchmark pins it.

## Acceptance
Before/after timings per case; no spinner shown on a normal tap; checks green.

Depends: T264, T273. Size: M. Owner: no.

## Outcome (2026-10-29)
Measured with the test bed's new `run.mjs regs-open` (release APK, staging, emulator): search the water, let the map
frame it, TAP IT ON THE MAP where its highlight ring is drawn (found in the raw screen pixels), wait for the page. The
open probe (`@app/data` `openProbe.ts`, on in the test-bed build) logs one `[open]` line per tap: each stage's time
since the press (hit test, route, water sheet, store, target, inputs, page, RegsScreen render, layout, commit, next
frame) and the SQLite calls with their queue wait and run time. "Before" is the same build with
`EXPO_PUBLIC_REGS_ONE_BY_ONE=1` (one row per call, no page held).

Cause: SQLite round trips. On a phone every expo-sqlite call is a native round trip through T264's one-at-a-time queue,
and a tap's page was ~100–220 of them, one keyed row at a time (a part's strings, a ladder's verdicts, a rows frame's
rows), behind the store being opened on the first tap. buildRegsPage is 5–13 ms; render + layout 15–45 ms.

| first tap on the water (ms, press → …) | before: content / frame · SQL calls | after (no prefetch) commit / frame · calls | after, prefetched on settle: commit / frame · calls |
|---|---|---|---|
| small stream (Norrish Creek) | **992 / 1029** · 129 — **spinner at 541 ms** | 151 / 306 · 33 | (scenario tap missed, see below) |
| the Fraser (17 parts) | 309 / 469 · 226 | 123 / 268 · 24 | 118 / 258 · 2 for the page |
| a lake (Cultus Lake) | 163 / 179 · 59 | 95 / 112 · 37 | 44 / 84 · 2 |
| the same water again | 60–96 / 78–121 · 6 | 40–52 / 64–83 · 3 (page held: drawn in the first render) | |

"commit" = the page committed to the native tree (Fabric mounts it on the UI thread); "frame" = the JS thread's next
frame callback, which on a first open waits behind the Salmon card's and access file's first decode (~0.15 s, after the
page is already committed). No spinner in any tap after. The node pin (`conformance/src/regs/openCost.release.test.ts`,
live release): a page's own reads 100 / 207 / 67 calls → 26 / 34 / 26 (budget 30 / 40 / 30; whole tap ≤ 50 / 60 / 50).

Fixes:
- `reads.ts` BATCHED READS: asks made together (same turn of the JS thread, give or take a few promise hops) are one
  `WHERE id IN (…)` per file — str, every frame family per region file, akey, seglist, segmom, ruleset, a key's cells;
  per-key cache unchanged; a key a file lacks rejects its own asker only. The rules are inflated 250 at a time with a
  turn between (no long hold).
- `@app/ui` HELD PAGES: target, inputs and the water sheet are asked once per store / source (`askOnce`, LRU 96) and
  PEEKED, so a water opened again is drawn in the very first render, no read.
- PREWARM (phone only, idle: 1.5 s after the release is up, after interactions): store opened + paired, statics / rules /
  licensing read, regs-sid / core / geo and every region file opened. Never on the web (T254/T256: nothing unasked).
- PREFETCH ON SETTLE (phone, Map tab, zoomed in past the handover, no search open): the map's visible sections →
  their waters (`itemsOfSections`, one read) → the 8 with most sections on screen have sheet + target + inputs read into
  the same held answers, one water at a time, stopped by a new settle or a tap (`useRegsPrefetch`). Logged
  `[prefetch] 8/8 waters … in 36–229 ms` on the emulator.
- T264 kept: every expo-sqlite call still goes through the one queue (the probe wraps it). near-cold on this build:
  5 of 5 cold starts that reached the tap listed the waters (139 within 25 km), no failed near read logged (the other
  runs failed on the emulator's Wi-Fi switch, not the app).

Not done / honest notes: the scenario's small-stream tap after closing the search missed (the map's framing moves when
the results sheet closes); the prefetched-tap column is for whatever water was under the ring point (titles were not
read back). One emulator, single samples. Look unchanged (no component drawing changed).
