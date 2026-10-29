# T262 — Android first install crashes (OutOfMemoryError in expo fetch) part way through the ~500 MB download

Status: OPEN

Source: T255 test bed, 2026-10-29 (release APK, staging, AVD n1_pixel API 35, Wi-Fi). A clean app's first run died
55 s in: `FATAL EXCEPTION: expo.modules.AsyncFunctionQueue … java.lang.OutOfMemoryError: Failed to allocate a 8388627
byte allocation with 982256 free bytes … growth limit 201326592 … at expo.modules.fetch.ResponseSink.finalize`. A
relaunch resumed from the `.part` files and finished (492.8 of 498.8 MB), so a phone recovers — after a crash on the
very first run. T172's first run (debug build, from the Mac's :39217) did not crash.

## Reproduced again (T255 final run, 06:00–06:30)
Deterministic: every clean first run dies of the same OutOfMemoryError (3 crashes in one run, each ~8 MB allocation
with < 3 MB free of the 192 MB growth limit); each relaunch resumes the `.part` files and dies again ~190 MB later.
One relaunch landed on the failed-boot screen instead: "The map data could not be loaded. fetch failed:
java.net.UnknownHostException … Run `pnpm tiles` to serve the release." (`../T255-shots/first-run-failed-boot-dns.png`)
— a transient DNS miss on a first run is a dead end with no Retry, it shows a raw Java error, and the developer hint
(`INSTALL_WORDS.serve`, App.tsx) is shown in a staging/production build.

## Why (reading, not yet proven)
`rangedDownload` (data/release/install.ts) fetches the tiles (185 MB) and the basemap (274 MB) as 8 MB Range requests
through the global fetch (Expo's `expo.modules.fetch`). Each body is copied into a Java-heap direct buffer
(`ResponseSink.finalize` → `allocateDirect`) and handed to JS; Hermes does not feel that Java memory, so the buffers
outlive their use until a JS GC, and the 192 MB Java heap fills.

## Work
1. Reproduce with the test bed: `run.mjs install` (fails on the crash: the runner reads the crash buffer per step).
2. Fix: stream the big files to disk natively — expo-file-system's download (`File.downloadFileAsync` / a resumable
   download with a Range header) instead of JS-held 8 MB bodies; keep the `.part` resume and the off-disk sha256.
   Interim alternatives, measured: a smaller chunk (1–2 MB) and `android:largeHeap` (a config plugin).
3. Fold in T256 item 4 (resilience of the same path) if done together.
4. The failed first-run screen: a Retry; the reader's words (not the exception); `INSTALL_WORDS.serve` only in a
   local build; a network failure on a first run retried before it is said.
## Acceptance
`run.mjs install` passes twice in a row on a clean app (no crash line); peak Java heap noted (`dumpsys meminfo`).
Depends: T255. Size: M. Owner: no.
