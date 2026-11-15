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
