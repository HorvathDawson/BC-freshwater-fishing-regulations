# T741 — Faster and lighter on iPhone and Android: a standing optimisation pass

Status: IN PROGRESS (iOS chat; survey first, fixes on branches).

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

## Work
1. Survey (no device time): what is in the APK / AAB and the IPA by size; the JS bundle by module; what the cold
   start loads before the first frame; what holds memory per screen (from the existing tables and the code); the
   first-install timeline. Output: a ranked list here, each row with its measured or estimated saving, its risk, and
   whether it changes anything a reader sees.
2. Fixes, best ratio first, each on a branch with its before and after.
3. Proposals for the owner for anything lossy or visible.

## Results

### Phase 1 survey (2026-11-14 night, iOS chat lane, no device time)
What was read: the Android `latest.aab` (0.7.10 build 32, 38,839,464 B) unzipped; the iPhone simulator `.app` of build
081d93f90 (65.7 MB on disk; no device archive was left on disk); a JS-only Metro export of the Android bundle from this
branch (`expo export --no-bytecode --source-maps`, 8.6 s, 3.8 GB peak; 1,874 modules, 3,554,712 B minified), tallied by
source map. `bundletool` is not installed, so per-device download sizes are estimated from gzip, not measured.

**What is in the app.** Android AAB (arm64 only): native libs 39.7 MB (MapLibre 12.8, React Native 7.0, pdfium 4.8 +
0.4, Hermes 2.7, expo-sqlite 1.9), **dex 30.4 MB (R8 is off, T250)**, Hermes bundle 5.0 MB, **fonts 4.78 MB (41
faces)**, images 2.4 MB (fish pictures already WebP), resources 2.8 MB; 20.2 MB of debug symbols that Play does not ship.
iPhone `.app`: MapLibre 16.2 MB, React 12.4, the app binary 9.8, Hermes 6.4, `main.jsbundle` 6.2 (bytecode), Expo
frameworks ~8, assets 6.8 MB of which **fonts 4.78 MB (the same 41 faces)**.
**Why 41 faces:** `App.tsx` imports 8 faces from the packages' index files, and each index `require`s every face of its
family, so Metro ships them all (Archivo 18, JetBrains Mono 16, Bricolage 7). 33 are never drawn: 3,877,992 B raw,
1,664,501 B gzip -9.
**The JS bundle** (minified, before bytecode): react-native 17 %, core/regs 4.7 %, gesture-handler 3.8 %, ui-native/regs
3.5 %, react-native-svg 3.1 %. Dev-only code still in the store bundle: `@expo/devtools`, `pretty-format`, metro HMR
client, ~31 KB (under 1 %). `inlineRequires` is off (Expo's default): every module's top level runs at start.
**First install (iOS, T718):** the 44 s after the last byte is the JS SHA-256 follower falling behind (~12 MB/s on
Hermes per T272, 422 MB of map files). Android hashes natively (`bgDownload` sha256); iOS has no native hash.
**Background checks:** every run (Android ~15 min) GETs `obs.json` with `cache: no-store` (staging: 24,039 B gzip,
185,429 B raw) when an alert is on, the release manifest (25 KB raw) when "new data ready" is on, and the version file;
the foreground update service already uses conditional GETs (`update/check.ts`), the background one does not.
**Storage:** release pruning, the rule book's marked copy (one 12 MB file in the cache, reused) and the web-only slices
(never on a phone, `phoneHolds`) were checked: nothing duplicated found beyond what T686 lists.
**Memory:** nothing new measured; holders per T360/T725/T740: MapLibre native (the largest), decoded release files as JS
external bytes (33-67 MB), salmon runs 6.3 MB (owner: stays decoded at load), PDFKit / pdfium while the book is open,
and the kept map under a page (T740, the Android chat's).

| # | item | platform | saving | risk | reader sees a change | size |
|---|---|---|---|---|---|---|
| 1 | Import only the 8 font faces drawn (per-weight subpaths) | both (+ web export) | 3.88 MB installed, ~1.66 MB download (measured raw; download estimated from gzip -9) | L | no | S |
| 2 | Faster JS SHA-256 (rounds unrolled by 8 with renamed variables, Int32Array words) | iOS install, all small release files, web | ~16-20 % less hash time in an interpreter (measured on node `--jitless` as a Hermes stand-in, not on Hermes); estimated 5-8 s of the iOS 44 s | L (pinned by vectors + random cross-check) | no | S |
| 3 | Conditional GET (ETag / 304) in the background check for obs.json, manifest, version file | Android (iOS runs rarely) | up to ~24 KB per run when unchanged; estimated ≤ 2.3 MB/day at 96 runs if obs.json rarely moves (its change rate not measured) | M | no | M |
| 4 | Native SHA-256 on iOS (CryptoKit) for the big files, as Android has | iOS | estimated 44 s → ~1 s after the last byte | M, native code | no | M — needs a device run; belongs with T722's iOS downloader |
| 5 | R8 with keep rules (WorkManager/Room, MapLibre patch, pdfium, bg-download) | Android | dex 30.4 MB; estimated 50-70 % off dex (typical R8, not measured) → ~6-10 MB download | H (T250: the first R8 APK died at start) | no | M — needs a device run, own ticket |
| 6 | Metro `inlineRequires` | both | cold start: not measured (module top levels deferred) | M (side-effect order) | no | S — needs a device run (`perf` cold start, `tap-stalls`) |
| 7 | Fonts embedded at build (expo-font plugin) instead of `useFonts` at start | both | cold start: the font load before the first frame (not measured) | M | no | S — needs a device run |
| 8 | Drop dev-only modules from the store bundle | both | ~31 KB JS | L | no | S |
| — | T686's release-data shrink (owner pick pending), T699 tiles | both | per T686 | — | — | not redone here |

**First five:** 1 (fonts), 2 (SHA-256), 3 (background conditional GET), then 8, and 6 / 7 / 4 / 5 handed over as
device-run items.

### Phase 2: landed on `perf/t741` (2026-11-14 night; not merged, no device run)
| commit | item | before → after | how measured |
|---|---|---|---|
| d0050b7d2 | 1. only the 8 faces drawn | fonts 41 / 4,778,008 B → 8 / 900,016 B, Android and iOS alike (−3,877,992 B installed; ~−1.66 MB download, gzip estimate) | `expo export` asset map, before (this branch's base) and after; pinned by `tools/font-imports.test.ts` (no family index import; faces named in styles = imported = loaded by `useFonts`, so no text falls back to the system face) |
| 3b2d8bc0e | 2. SHA-256 rounds unrolled | node `--jitless` (Hermes stand-in), 16 MB: 4.3-4.4 → 5.0-5.4 MB/s; JIT: 237 → 264 MB/s | scratch bench, best of 3, alternated twice, load ~15; NOT on Hermes. Pinned by `tools/sha256-node.test.ts` (node's crypto on 5 MB random, odd chunk runs, every length around a block end, the `.part` resume path, a wrong sha refused) + the FIPS vectors |
| 47f6e3888 | 3. background obs.json by conditional GET | every run a whole read (24,039 B gzip on staging) → a 304 when unchanged (estimated; obs.json's ETag on staging was the same over 25 min tonight, its change rate not measured) | `packages/data/src/feed/obsHeld.test.ts`: unchanged (strong validator, 304, kept copy), changed (whole read, new copy), no validator (whole read every run, said), bad kept copy (reported, whole read), no store (as before), stray 304 (a failure) |

**Validators today (item 3):** staging's edge answers `obs.json` with a WEAK ETag (`W/"…"`, it gzips) and
`cache-control: public, max-age=300`; the data worker compares the STRONG form against the R2 object's ETag (measured
2026-10-28 for `update/check.ts`). So the check sends the strong form, as the update service already does: the 304 is the
origin's word that the R2 object has not moved. With no ETag, every run reads whole and logs `[obs] … no ETag`. The
manifest and the version file are still whole reads (manifest 25 KB raw; not changed: smaller, and the release check
needs the body's fields). Web: no background check, nothing changes.

**Items 2 and 4 per platform (restated after T722):**
- Android: the big files (tiles, mask, basemap, rule book) are hashed NATIVELY today (`BgDownloadModule.kt`,
  MessageDigest). Every other release file (the ~40 MB of SQLite and binary files, raw) and the update service's feed
  bodies are hashed by the JS `Sha256` at install and at every update.
- iOS on 0.7.9/0.7.10: everything in JS, the big files included (the 44 s, T718). On `ios/t722-install` the big files are
  hashed natively (CryptoKit, `BgDownloadModule.swift` `sha256`, F14), so item 4 is DONE there for the big files once
  that branch merges; the small files stay JS as on Android.
- Web: release files by WebCrypto (`source.web.ts`); JS `Sha256` only for the update check's feed bodies.
- So item 2's gain is for the small files on both phones: estimated (proxy ratio on T272's ~12 MB/s Hermes figure)
  ~3.3 s → ~2.7 s of JS-thread hashing per install or full update, spread in slices. Not measured on a device.

### Needs a device run (the main lane schedules; nothing here is built)
| item | what | bench | pass line |
|---|---|---|---|
| 1, 2, 3 (this branch) | fonts drawn, install hash, background check | Android: `node app/tools/testbed/run.mjs install perf walkthrough`; iOS: `run-ios.mjs first-open walkthrough perf`; shots of a regs page + the boot screen | every face drawn as before (shots match build 32), install passes, `[download] … followed, N ms to catch up` shorter or equal |
| 4 | native hash on iOS | lands with `ios/t722-install` (F14) | T722's own: `[setup] installed … ms after the last byte` under ~5 s (was 44,158 ms) |
| 5 | R8 + keep rules (WorkManager/Room, MapLibre patch, pdfium, bg-download, expo modules) | own ticket; Android `run.mjs install crash-bed bg-install regs-open tap-stalls memory`, then a Play internal build on a phone | the app starts and every scenario passes; AAB dex 30.4 MB → measured |
| 6 | Metro `inlineRequires: true` | Android `run.mjs perf tap-stalls`, iOS `run-ios.mjs perf tap-stalls`, 3 cold starts each, same build ± the flag | cold start to map faster than ±0.3 s noise, no new stall ≥ 300 ms, walkthrough passes |
| 7 | fonts embedded at build (expo-font plugin), `useFonts` off on native | `perf` on both + shots of the boot screen and a regs page | no system-face frame; cold start not slower |

### Memory: second pass, code only (2026-11-14 night)
| holder | where set | today | a lower value would cost | bench |
|---|---|---|---|---|
| MapLibre tile prefetch | `Map.native.tsx` `PREFETCH_ZOOM_DELTA = -1` | 1 lower zoom prefetched | blurrier tiles longer while zooming out | `memory` + `pan` (frames) |
| MapLibre ambient/tile cache | not set by the app (MapLibre defaults; T360: the ambient cache is disk) | default | — (no RAM win found in T360) | — |
| SQLite page cache | `open.native.ts`; no `PRAGMA cache_size`, so SQLite's default 2,000 KiB per connection | ~14 connections: up to ~28 MB if all fill (estimated ceiling, not measured) | slower repeat reads on a page | `memory` with `cache_size=-500` vs default, + `regs-open` timings |
| idle connections | `loader.closeIdle(60 s)`, only on a trim | open for the app's life otherwise | a 0.1-0.3 ms reopen | `memory` (PSS after 2 min idle on the map) |
| spot photos | `pickPhoto.native.ts` keeps full pixels (quality 0.8); thumbnails `SpotsScreen.tsx` / `SpotPhotos.tsx` draw the full file | decode at the platform's choice (iOS scales to the view; Android Fresco downsamples large JPEGs: not verified) | none if a thumbnail file is made; needs an image module (new dependency: proposal) | `saved-spots` with 10 photos, `memory` |
| rule book viewer | react-native-pdf (Android, pdfium), PDFKit (iOS) | ~70 MB while open; T740: only ~10 MB freed on close with a page up (iOS) | — | T740's pair (Android chat) |
| decoded release files | JS external bytes, 33-67 MB (T360) | held for the session | slower first open of what was dropped | `memory` |
| lists | `FlatList` in search, regs wide, glossary; other lists are short | — | — | nothing to do found |

### Round 2 (2026-11-15, same branch)
**A2 landed (c26cd9405):** the background check's release manifest and version file by conditional GET too
(`notify/conditional.ts` `conditionalJson`; the obs.json rule: strong validator, only a 304 answers the kept copy, a
200 is checked by the reader before it is kept, no ETag = whole read every run and said, a bad kept copy reported and
read whole). Kept whole because the release check needs the manifest's fields. Saving per unchanged run: the manifest's
body: staging's `/release/manifest.json` is 6,255 B on the wire (gzip; 25,131 B raw), weak ETag, `no-cache`; the same
URL with the strong If-None-Match answered **304, 0 B** (measured 2026-11-15 with curl). The version file is small. Tests: 6
cases.

**A1 (item 8), no change, and why:** the ~35 KB of dev-only code in the store bundle is pulled in by Expo's own modules,
not by the app: `expo/build/async-require/loadBundle.js` → `hmr.js` → `pretty-format` (12.4 KB with its plugins) +
metro's `HMRClient` (2.6 KB); `expo-sqlite`'s `SQLiteDevToolsClient` → `expo/devtools` → `@expo/devtools` (12.6 KB).
The app's own import (`@expo/metro-runtime` in `apps/mobile/index.js`) is 3.8 KB and installs the `location`
polyfill on native. Removing the rest needs a patch to expo and expo-sqlite for under 1 % of a 3.55 MB bundle (0.04 %
of the app). Not worth a patch to keep; recorded, not done.

**A3, the JS bundle beyond fonts** (minified JS before bytecode, Android, from the source map):
| what | bytes | verdict |
|---|---:|---|
| duplicate packages (two versions of one package) | 0 | none: no nested `node_modules` path in the bundle |
| web-only app code (`*.web.*`, `src/web/`, react-dom, maplibre-gl, sql.js) | 0 | none reaches the native bundle (rule 34a pairs hold) |
| test files, fixtures, `dev/` | 0 | none |
| test-bed probes and the map lab (`panProbe` 2.4 K, `openProbe` 2.0 K, `labels`, `MapLab`, `lab`, `camProbe`, `imageryProbe`, `testbedFlag`) | ~13 K | shipped on purpose: switched by `EXPO_PUBLIC_PAN_PROBE` etc. in a test-bed build of the SAME bundle; dropping them would make the test bed measure another bundle |
| generated tables: `fish-sprite.generated` 24.3 K (the loading fish, base64), `pack.generated` 23.4 K (the release codec's types/specs), `fish.generated` 16.2 K, `help.ts` 18.2 K, `changelog.ts` 8.8 K | ~91 K | all read on a phone (boot screen, decoder, fish sheet, Help, What's new); help and changelog could come from the release but are app words (core sentences), not data: no move |
| `whatwg-url-minimum` 30.2 K, `whatwg-fetch` 10.0 K | 40 K | the URL / fetch polyfills Expo and RN install: needed |
Nothing provably dead to remove; no change.

**A4, spot photo thumbnails (PROPOSAL, not built; touches T732's files):** `origin/android/t732-backup` adds
`expo-image-manipulator` 58.0.15 (`backupSet.native.ts`: 1600 px long edge, JPEG 70). The same module can write a
thumbnail when a photo is added (`photoStore.native.ts`), e.g. 480 px long edge (2× the largest cell the Spots list
and the spot's grid draw), kept beside the original and deleted with it; rows and the grid draw the thumbnail, the
photo viewer the original. Memory per row, estimated (4 bytes a pixel): a 12 MP photo decoded whole is 48 MB; a
480 × 360 thumbnail 0.69 MB. What each platform decodes today is NOT verified: iOS's image loader scales to the view and
Fresco may downsample a large JPEG on Android, so today's real cost per row is somewhere between ~0.7 and 48 MB.
Bench: `saved-spots` with 10 photos of 12 MP, then `memory` on the Spots tab, before/after. Also saves decode time on
scroll. Old photos get thumbnails on first draw (or a one-time pass). Owner pick, after T732 merges.

**A5, the SQLite connections on a phone** (`release/loader.ts`: one connection per file, opened on its FIRST read, held
until a memory trim closes it idle 60 s or an update retires the set; each may grow to SQLite's default 2,000 KiB page
cache):
| file | first opened by | could close when its screen closes? |
|---|---|---|
| `core` | boot / first search / first tap | no: every screen |
| `water` | a water's page | no: the map and pages |
| `geo` | first tap (part boxes) | no: every tap |
| `regs-<r>` (1-8, one per region visited) | a page in that region | yes after the page closes, all but the map's region (a reopen 0.1-0.3 ms, T360) |
| `regs-sid`, `down` | a tap on an unnamed stream | rarely used: candidates |
| `near` | town search | yes, when search closes |
| `climate` | a gauge chart | yes, when Conditions closes |
| `conditions` | the Conditions tab | yes, when Conditions closes |
| `lines` | making / opening a link | yes, after the link |
Up to 17 connections; a typical session 6-9 (core, water, geo, 1-3 regs regions, conditions, climate, near). Sharing one
connection (ATTACH) would change the SQL queue and every query's schema name: not proposed. Closing on screen close
changes nothing a reader sees but needs the device measure (B3) to show the page cache is real memory first.

### Round 2, device (iPhone 17 simulator `t741-17`, iOS 26.5, `--probes` builds from this worktree, lanes 2/3)
Builds, same script and signing: base 410564271 (the branch point), branch d2fd851b7, inline b1f4bb080 (side branch
`perf/t741-inline`, not on perf/t741), cache 0519af399 (side branch `perf/t741-cache`, never to merge).
**Size, measured on the built apps:** `.app` 65,720 KB (base) → 61,824 KB (branch), −3,896 KB; fonts 41 → 8 (900,016 B);
`main.jsbundle` 6,161,085 → 6,144,737 B. Inline requires makes the bytecode bigger: 6,297,876 B (+153,139 B on the branch).

**B1 functional (FUNCTIONAL ONLY: the machine was loaded, load 7.5-21; no number below is a timed result):**
- branch, fresh install: first-open PASS (513 s to the map); search Cultus → regulations PASS; edge-back PASS; tap on the
  lake → page PASS; walkthrough PASS to step 27 of 41 when the 28-min session cap stopped it (Maestro ~45 s a step);
  `perf` not reached. ONE FAIL: "the data is staging (More → About)": the More tab was hidden by the first-run notice.
  Cause, from the app log: step 1 passed at ~00:28:56 while the install was still verifying (installed 00:29:37); the
  notice then came up after the map opened. A test-bed race on a slow download, not the app: base, fresh install, same
  device, same steps: all 5 PASS (478 s; there the install had ended before step 1 finished).
- **Faces drawn as before:** compared by eye, side by side, the branch shots (`runs/b1-branch/002…`, `003…`) with
  `handoff/PLAN/ios/shots/baseline/tap-water.jpg` / `first-open.jpg`: Bricolage on the headings ("Before you fish",
  "Closed", "✓Open", "Let's find a water"), Archivo on the body and rows, JetBrains Mono on the month letters J F M …;
  no system face anywhere. Shots in the session scratchpad `t741/runs/`.
- Install hash after the last byte (from the app log, NOT like for like: different download speeds and load): branch
  25,546 ms, base 23,253 ms (T718's 44,158 ms was another day's line). No claim on item 2 from this.
- **B2 functional (inline build, not timed):** `tap-stalls` 5 of 5 PASS (numbers not counted: load 6-28). `tap-water`:
  first run, 2 PASS + 1 FAIL — after a cold start the first-run notice ("Before you fish") was up over the map and the
  tap opened nothing; the control re-run (same device, base then inline, back to back) passed all 3 on BOTH builds. So 1
  failure in 2 inline runs, 0 in 3 base runs: not reproduced, not explained, and a reason to read the timed B2 runs'
  shots for the notice too before keeping inline requires (it changes the order module top levels run).
- **B3 functional (cache build, `PRAGMA cache_size=-256`):** `memory` 10 of 10 steps PASS; the rule book opens. Its
  numbers wait for the timed pair against the branch build (load 6-20 during this run).

### Round 2, TIMED (2026-11-15 03:00-04:39, the quiet slot; t741-17, iPhone 17 sim, iOS 26.5; one heavy job, lane 2)
Load average (1 min) written at each session start: 4.6-7.2 (the script waited up to 5 min for it to fall under 6; one
editor process of the owner's uses about one core all night). At the very end of the last memory run the load jumped to
35.9 (04:39): the cache run's last step only. Simulator numbers: the Mac's CPU, not a phone's.
Cold start is read two ways: the scenario's screenshot poll (±0.3 s, too coarse to split these builds) and, finer, the
app's own log per launch (ms from the process's first log line to `Running "main"`, `[setup] opened`, the first
`[drawn]`; scratchpad `t741/scripts/launches.py`), 6 launches per build (3 `perf` cold starts + the other cold starts of
that session).

| measure | base 410564271 | branch d2fd851b7 | inline b1f4bb080 | verdict |
|---|---|---|---|---|
| `.app` size | 65,720 KB | 61,824 KB | — | −3,896 KB (fonts), measured |
| cold start, screenshot poll (3 runs) | 712 / 583 / 607 ms | 714 / 603 / 586 ms | 708 / 597 / 612 ms | no difference at ±0.3 s |
| launch → first `[drawn]`, median (min-max), 6 launches | 1,086 (1,049-1,137) ms | 1,074 (1,021-1,136) ms | 1,050 (1,006-1,072) ms | inline −24 ms against a 115 ms spread: NOT a win |
| launch → `[setup] opened`, median | 860 ms | 843 ms | 817 ms | inside the spread |
| `tap-stalls` (limit 300 ms) | PASS, worst 225 ms | PASS, worst 240 ms | PASS, worst 244 ms | all pass |
| map tap → first frame (cold / warm) | 142 / 54 / 71 ms | 148 / 54 / 66 ms | — | same |
| `tap-water` ×3 | 2 of 3 (run 2: edge-back) | — | 2 of 3 (run 2: edge-back) | the edge drag failed on BOTH builds in the same round: test bed; the notice failure seen once before did not come back |

**B2 decision: inline requires NOT kept.** The cold start gain (−24 ms median) is inside the run-to-run spread, and
tap-water was not 3 of 3 (the failure was on both builds, but the rule is the rule). The commit stays on the local side
branch `perf/t741-inline` only; `perf/t741` never had it. It also makes the bytecode 153 KB bigger.
**B4 (fonts embedded at build): not done.** No log line marks font loading; between `Running "main"` (~140 ms) and
the first draw (~1,070 ms) the logged work is the release open and the paint decode (`[work] decode paint sliced`
~440-490 ms). Nothing shows the 8 faces as a visible share, so the rule for B4 is not met.

**B3 memory, footprint MB** (malloc in brackets; one run each; T725 measured ±30 MB between two runs of one build):
| step | base | branch | cache_size 256 KiB |
|---|---|---|---|
| boot | 135 (46) | 129 (42) | 134 (44) |
| 8 pans | 161 (51) | 149 (45) | 158 (49) |
| satellite, 4 pans | 172 (52) | 159 (47) | 173 (53) |
| a water's page | 240 (59) | 241 (55) | 235 (55) |
| rule book open / closed | 307 / 251 | 309 / 252 | 299 / 245 |
| second open / closed | 249 / 252 | 249 / 252 | 241 / 245 |
| Conditions | 250 (66) | 241 (60) | 244 (61) |
| Map after Conditions | 243 (63) | 236 (58) | 236 (58) |
| Salmon | 245 (63) | 238 (58) | 237 (58) |
| memory warning | 243 (63) | 235 (57) | 235 (58) |
- Branch vs base: 0 to −12 MB, inside the noise; the fonts were never loaded beyond the 8, so no RAM saving was expected
  (the saving is the app's size, above).
- **SQLite cache_size: the ~28 MB ceiling is not real on this path.** 256 KiB per connection instead of 2,000 KiB moved
  the footprint by +1 to −8 MB against base and by −10 to +14 MB against branch: inside one run's noise. So no
  proposal to lower it, and closing connections when their screen closes (A5) is not worth a change for memory either:
  the page caches never fill on these screens. Recorded; nothing changes.

### Round 3 A: where the launch's ~1.07 s goes (code + the app's own log; branch build, 6 launches, iPhone 17 sim)
From one launch's log (49299, typical of the six; ms from the process's first log line):
| step | at | takes | what (code) | skippable on an unchanged release? |
|---|---|---|---|---|
| native start, Expo modules made | 0 → 141 | ~140 ms | `Running "main"` at 131-198 ms in all 18 launches | no (native) |
| JS start: App module top levels, stores opened, React's first mount, boot: held pointer + `heldManifest` (JSON parse), `readsRelease`, tiles compat check, loader made | 141 → ~347 | ~200 ms | `boot.ts` `openSet`; `App.tsx` | checks are cheap and must stay (they refuse a set this app cannot read) |
| **paint read + decode, SLICED** | ~347 → 848 | **501 ms wall** (`[work] decode paint sliced 501 ms`; 437-501 ms over the 18 launches) | `loader.paint()` → `source.bytes("paint")` (2,311,087 B read from disk, no hash) → `decodePaintAsync` → `runSliced` with an **8 ms** budget, each turn a `setTimeout(0)` (React Native has no `MessageChannel`, so each turn waits for the next timer tick) | see below |
| core opened | 848 → 851 | 3 ms | `loader.db("core")` | no |
| Shell mount, map made, first frame | 851 → 1,093 | ~240 ms | the salmon slices 4/8/x decoded before the draw (12 ms), MapLibre's first frame | out of scope here |
**The paint decode's CPU** (measured on the live release's paint, node `--jitless` as the Hermes stand-in): **~190 ms**
(JIT: 25 ms), with 883 yield points. Sliced at 8 ms that is ~25 slices; the 501 ms wall is the CPU plus ~25 turns of
~12 ms waiting (estimated from wall − CPU) plus whatever else the JS thread runs between them. Nothing is hashed at a start
(the files were hashed when installed); the release is NOT re-verified at a start, and nothing here proposes to.

Ranked options (gain estimated from the numbers above):
1. **Bigger slices for the launch's paint decode only (40 ms instead of 8 ms) while the boot screen is the only thing
   on screen** — est. −200 to −250 ms (25 turns → ~5). Nothing a reader sees changes: the boot screen's fish is a native-
   driver animation (`FishSpinner.native.tsx`, `useNativeDriver: true`), so a busy JS thread does not stutter it, and a
   40 ms hold stays under the 50 ms of a long task and far under the 300 ms stall limit. An update applied while the map
   is on screen (`keepReplaced`) keeps the 8 ms slices. **Pick.**
2. A decoded paint kept on disk beside the release (keyed by release id + the paint's sha256, its own checksum; a stale or
   corrupt one detected, reported, rebuilt) — saves at most the CPU (~190 ms proxy) minus the cost of reading and
   rebuilding the members' objects from it; more code and a new on-disk format to keep honest. Second; not built.
3. Decoding paint without the slices at launch (`runNow`): ~−300 ms but one ~190 ms hold, and the boot line's own
   updates wait for it; option 1 gets most of that without the hold.
