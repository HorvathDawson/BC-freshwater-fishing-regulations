# T741 — Faster and lighter on iPhone and Android: a standing optimisation pass

Status: IN PROGRESS (a standing pass; `perf/t741` merged onto `redesign/stream-sections` 2026-11-16 via `merge/held-group1`; left (Claude): the Android measures named in the rounds above).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T741 | Faster and lighter on iPhone and Android: a standing optimisation pass (cold start, first install, tap to page, memory, app size, download size). Next free ticket T742 | IN PROGRESS (standing pass; `perf/t741` merged onto redesign/stream-sections 2026-11-16 via `merge/held-group1`; lossy or visible savings are owner proposals) | — | owner 2026-11-14: "can you make sure to keep working on optimizations? make app faster and lighter for both ios and android". Builds on T360, T686, T716, T717, T719, T720, T722, T725, T737, T738, T740 |

Source: owner 2026-11-14 (evening), verbatim: "can you make sure to keep working on optimizations? make app faster and
lighter for both ios and android".

## What this covers
- **Faster:** cold start, the first install (download, checking, first map), a tap to a water's page, tab and page
  changes, search, map pan and zoom, the rule book.
- **Lighter:** memory in use (per screen and at the peak), the app's size in the store (APK / AAB, IPA), the data
  download (479 MB today), storage on the phone, battery and data used in the background.

## Rules for this ticket
- Measure first, then change. Every item names its number before and after, on the same device and build type, and
  says when a comparison is not like for like.
- One change per commit, on a branch (`perf/...`), never straight on `redesign/stream-sections`. A change to shared code
  runs the Android benches (`regs-open`, `tap-stalls`) and the iOS twins before it is offered for merge.
- No change to what a reader sees or to data content. A saving that drops, simplifies or re-encodes with loss
  anything shown (tile detail, zoom levels, photo quality, text) is a **proposal for the owner**, never done unasked.
- Existing work is built on, not redone: T360 (Android memory), T686 (data savings, ranked; owner pick pending), T716
  and T719 (tap to page), T717 and T740 (map kept under a page: memory), T720 (stalls), T722 (install), T725 (iOS
  memory), T737 (prefetch), T738 (first-install bar), T272 (hash while downloading), T420 and T492 (MapLibre).
- Timed runs only on a quiet machine (no other heavy job), with the load average written down.

## Known starting numbers (2026-11-14)
| measure | iPhone simulator | Android emulator | source |
|---|---|---|---|
| cold start to map | about 0.6 s | not re-read today | T725 |
| tap → page shown | 374 to 553 ms | 331 to 586 ms | T719, T716 |
| worst JS stall | 227 ms (cold start) | under 300 ms | T720 |
| memory on a water's page | 279 MB (was 213 to 230 before T717) | 332 to 411 MB | T725, T740 |
| peak memory | 389 MB (rule book over a page) | 379 MB (11-09, old APK) | T725, T360 |
| data download | 479 MB (basemap 225, atlas 197, rule book 12, rest 45) | same files | T718 |
| after the last byte, checking | 44 s | not read | T718 |
| app size in the store | not measured | not measured | — |

## Done (`perf/t741`, 13 commits, merged 2026-11-16 via `merge/held-group1`)
- d0050b7d2 only the 8 font faces drawn (per-weight subpaths; `App.tsx` had imported family index files, which `require` every face): 41 / 4,778,008 B → 8 / 900,016 B on both platforms (~−1.66 MB download, gzip estimate); iOS `.app` 65,720 → 61,824 KB, measured. Pinned by `tools/font-imports.test.ts`; faces checked by eye on the iOS sim against `docs/ios/shots/baseline/`, no system face.
- 3b2d8bc0e JS SHA-256 rounds unrolled: node `--jitless` (Hermes stand-in), 16 MB: 4.3-4.4 → 5.0-5.4 MB/s; NOT measured on Hermes. Pinned by `tools/sha256-node.test.ts` + the FIPS vectors. The gain is for the small release files (~40 MB raw) on both phones: Android hashes the big files natively (`BgDownloadModule.kt`), so does iOS once `ios/t722-install` lands; est. ~3.3 s → ~2.7 s per install or full update, not measured on a device.
- 47f6e3888 + c26cd9405 the background check reads obs.json, the release manifest and the version file by conditional GET (`notify/conditional.ts` `conditionalJson`): it sends the STRONG ETag form (staging's edge answers a weak `W/"…"`; the data worker compares the strong form against the R2 object's ETag), only a 304 answers the kept copy, no ETag = whole read every run, logged `[obs] … no ETag`. Manifest measured 6,255 B gzip → **304, 0 B** (curl, 2026-11-15). Tests: `packages/data/src/feed/obsHeld.test.ts` + 6 cases.
- Launch paint slice 40 ms (`core/src/sliced.ts` `LAUNCH_SLICE_MS = 40`; `boot.ts` `paintSliceFor(o)`; `SLICE_MS` 8 when `keepReplaced`; a cold start that applies a staged set is a launch too; the web keeps the default). Pinned by `packages/data/src/release/launchSlice.test.ts`. iOS sim (iPhone 17, iOS 26.5), builds d2fd851b7 → 571edb7b7, 6 launches: launch → first `[drawn]` median 1,030 → **786 ms (−244 ms)**, ranges do not overlap; `tap-stalls` PASS, worst 233 ms. Simulator, the Mac's CPU.

## Measured, not kept (do not redo)
- Metro `inlineRequires` (side branch `perf/t741-inline`, b1f4bb080, never on perf/t741): −24 ms median against a 115 ms spread, bytecode +153,139 B, `tap-water` not 3 of 3: NOT kept.
- Fonts embedded at build (expo-font plugin): no log line shows font loading as a visible share of the launch: not done.
- SQLite `PRAGMA cache_size=-256` (side branch `perf/t741-cache`, 0519af399, never to merge): footprint inside one run's noise; the ~28 MB ceiling is not real; closing connections when their screen closes (A5, `release/loader.ts`, up to 17 connections, typical 6-9) is not worth it for memory either.
- Dev-only code (~35 KB): pulled in by Expo's own modules (`expo/build/async-require/loadBundle.js` → `hmr.js` → `pretty-format`; `expo-sqlite`'s `SQLiteDevToolsClient` → `@expo/devtools`); a patch for under 1 % of the bundle: not done.
- JS bundle beyond fonts: no duplicate packages, no web-only or test code; the test-bed probes (~13 K) ship on purpose (switched by `EXPO_PUBLIC_PAN_PROBE` etc. in the SAME bundle); nothing provably dead.
- Memory, iOS sim, base vs branch: 0 to −12 MB, inside the noise (none expected).

## Left
1. **Android measures** for the merged changes: `regs-open`, `tap-stalls`, cold start (the 40 ms slice), and the background alert check (conditional GETs of obs.json, the manifest and the version file). Fonts / install: `node app/tools/testbed/run.mjs install perf walkthrough`, shots of a regs page + the boot screen; pass: every face drawn as before (shots match build 32), install passes, `[download] … followed, N ms to catch up` shorter or equal.
   Like-for-like cold start (PROPOSAL for the emulator lane): the same JS lines reach logcat under `ReactNativeJS` in a `--probes`/test-bed APK (`Running "main"`, `[setup] opened … manifest + paint N ms`, `[work] decode paint sliced N ms`, the first `[drawn]`); the process's start is ActivityManager's `Start proc <pid>:ca.canifishthis.app` line (or `am start -W`'s `TotalTime`). Measure `Start proc` → first `[drawn]` (logcat `-v epoch`), 6 cold starts (`am force-stop` between), base vs branch, the load written down; plus `tap-stalls`. Android's timer floor is the Choreographer frame too, so the same gain is expected, unmeasured.
2. **Native SHA-256 on iOS** for the big files (item 4): lands with `ios/t722-install` (F14, CryptoKit, `BgDownloadModule.swift` `sha256`); pass: `[setup] installed … ms after the last byte` under ~5 s (was 44,158 ms, T718).
3. **R8 with keep rules** (item 5: WorkManager/Room, MapLibre patch, pdfium, bg-download, expo modules): own ticket; dex 30.4 MB, est. 50-70 % off (typical R8, not measured); T250: the first R8 APK died at start. Bench: Android `run.mjs install crash-bed bg-install regs-open tap-stalls memory`, then a Play internal build on a phone.
4. **Longer slices for the slicer's other callers (PROPOSALS, not changed).** Only a longer slice is honest: `setImmediate` / promise / `queueMicrotask` starve the native side (a freeze), `requestAnimationFrame` is frame-aligned like the timer. CPU in node `--jitless`, turns at 8 ms, ~12 ms saved a turn:
   | caller | when | CPU | turns @8 ms | at 16 ms / 40 ms | est. gain | what a longer slice costs a finger |
   |---|---|---|---|---|---|---|
   | `sha256HexAsync` / `ungzipAsync` (`HASH_SLICE` 256 KiB ≈ 20 ms a slice on Hermes) | every release file at install and at each update (~40 MB raw) | ~3.3 s (T272 rate) | ~160 | 512 KiB: ~80 turns | ~1 s per install/update | a 40 ms hold while the map may be on screen (update): under the 300 ms limit, a tap could wait 40 ms |
   | paint, in-session apply (`keepReplaced`) | T746: the apply measured 702-880 ms | 178 ms | ~23 | ~12 / ~5 | ~130 / ~220 ms | 16 ms: nothing a finger feels; 40 ms: a tap or pan frame may wait up to 40 ms (bench: `tap-stalls`, a pan during an apply) |
   | method (`decodeMethodAsync`) | after the first draw at a launch; an apply | 58 ms | ~8 | ~4 / ~2 | ~50-70 ms | as above |
   | salmon shards (`decodeSalmonShardAsync`, 11 files) | after the first draw; an apply | 6-53 ms each (salmon-6 53) | 1-7 each, ~20 in all | ~10 / ~6 | ~120-170 ms over all | as above |
   Ranked: 1) the hash/inflate slice; 2) the apply's paint at 16 ms; 3) salmon shards; 4) method. Each needs `tap-stalls` and a pan during an apply to stay under 300 ms, and T746's apply bench before/after. Also not built: a decoded paint kept on disk beside the release (keyed by release id + the paint's sha256), saves at most the ~190 ms CPU, a new on-disk format.
5. **Spot photo thumbnails (PROPOSAL, owner pick after T732 merges; touches T732's files):** `origin/android/t732-backup` adds `expo-image-manipulator` 58.0.15 (`backupSet.native.ts`: 1600 px long edge, JPEG 70). The same module can write a thumbnail when a photo is added (`photoStore.native.ts`), e.g. 480 px long edge, kept beside the original and deleted with it; rows (`SpotsScreen.tsx` / `SpotPhotos.tsx`, full file today) and the grid draw the thumbnail, the viewer the original. A 12 MP photo decoded whole is 48 MB, a 480 × 360 thumbnail 0.69 MB; what each platform decodes today is NOT verified (somewhere between ~0.7 and 48 MB a row). Bench: `saved-spots` with 10 photos of 12 MP, then `memory` on the Spots tab. Old photos get thumbnails on first draw (or a one-time pass).
6. Other levers, code only, not measured: MapLibre prefetch `Map.native.tsx` `PREFETCH_ZOOM_DELTA = -1` (bench `memory` + `pan`); idle connections close only on a trim (`loader.closeIdle(60 s)`); T686's release-data shrink (owner pick pending) and T699 tiles are not redone here.

## Merged, 2026-11-16 (`merge/held-group1`)
`perf/t741` (13 commits) merged after T746, T738 and the crash-send fix. Conflicts: App.tsx's font imports (the eight
per-weight imports kept, beside the head's core words), `data/release/boot.ts` (the paint call keeps T746's "apply"
stage and asks `paintSliceFor(o)`: a cold start that applies a staged set is a launch too, so it gets the 40 ms slices),
and this file (the branch's rounds were already partly copied here; the branch text is the whole of it). Nothing
superseded. Android benches for it: `regs-open`, `tap-stalls`, cold start (the 40 ms slice), and the background
alert check (conditional GETs of obs.json, the manifest and the version file).

