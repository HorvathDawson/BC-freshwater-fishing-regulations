# T272 — First install: a second progress bar after the download, and make that step fast

Status: DONE 2026-10-29 — abaf19343, e8db24c51 (+ App.tsx in f0357560b) (owner 2026-10-29 on Android: "it downloaded everything then at 100% it took a very long
time to open. it needs a second loading bar so the user doesnt get confused").

## Work
1. Measure on the APK (testbed emulator + timings in the Updates log) what runs between download 100% and the map: unzip /
   verify / SQLite import / index build / first queries. Suspect T264's serialized SQLite queue and the T262 streaming path.
2. Loading screen gets a second stage with its own bar and words (e.g. "Downloading the regulations" → "Setting up on this
   device"), real progress (bytes/rows/steps), never a stuck 100%. Same component/look as the first bar (FishSpinner rules).
3. Cut the time where cheap (batch inserts in one transaction, defer non-map work until after first paint).

## Acceptance
No moment where a bar sits at 100% with nothing moving; before/after timings for the post-download step on the emulator;
screenshots of both stages.

Depends: T262, T264. Size: S–M. Owner: no.

## Outcome (2026-10-29)
Measured with the test bed (`run.mjs install`, release APK, staging, emulator on Wi-Fi; the scenario now records when the
download line first says 100 %, when "Setting up" is first seen and when the map is up, and keeps the app's
`[download]`/`[setup]` logcat lines as `T272-install-timeline.txt`).

| | before | after |
|---|---|---|
| launch → map | 180 s | 62 s |
| download line at 100 % → map | **112 s** (nothing moving) | **11 s** (second bar moving) |

What the 112 s was: (1) the basemap's sha256 off disk AFTER its download, 23 s (JS SHA-256 on Hermes, ~12 MB/s);
(2) the MAP ASSETS (glyphs + sprites, 776 files, 13 MB) fetched ONE AT A TIME after the download and counted by no bar,
~80 s — and the data host's per-IP limit (T249: 300 / 10 s) answered some with HTTP 429, which failed the whole boot;
the automatic retry (T262) then re-hashed both map files (~40 s) before trying again. Opening the set (manifest, paint,
core) is ~0.2 s. There is no SQLite import on a first run (files open in place) and T264's queue is not on this path:
left as it is.

Fixes:
- The big files are hashed WHILE they download (`partDownload`'s follower, `PartFs.readAt`): a megabyte behind the
  native writer, trusted only for one clean fill (a retry or a whole-body answer says `onRewrite` → hashed off disk as
  before; a follower hash that does not match is checked off disk before anything is refused). What is left after the
  last byte is the hash's catch-up (13.6 s on the emulator's fast line, ~0 where the line is slower than ~10 MB/s).
- The map assets start BESIDE the big downloads, 6 at a time, paced at 22 starts/s (under the host's limit), each file
  tried 4 times (a 429 waits 3 s × try); 35 s of fetching now hidden under the download, 0 failures.
- The second stage: "Setting up on this device" with its own bar (`StageBar`, @app/ui-native) under the download's
  (kept full): "Checking the downloaded files · n %" → "Fetching the map's labels and symbols" (only if still going)
  → "Opening the map" (`SETUP_WORDS`, `setupFraction`, `setupLine` in @app/core). Real progress: bytes hashed, files,
  open steps. A file already in place (a resumed install) is re-checked with progress too.
- The loading screen redraws at most every 200 ms (`PROGRESS_MS`), leaving the JS thread to the hash; a failed start
  is logged (`[boot] start failed: …`).

Shots: `../T272-shots/stage1-download.png`, `../T272-shots/stage2-setup.png`.
Follow-up (owner, not done here): the assets would be one request if `deliver` packed the folder (or the data worker
exempted the immutable `map-assets-*` paths from the rate limit).
