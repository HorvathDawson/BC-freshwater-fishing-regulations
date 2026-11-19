# T912 — A regulations data update stops downloading when the app is closed

Status: IN PROGRESS (main chat; reproduce on the emulator, then fix; 0.8.3 batch).

Owner 2026-11-19 ~10:50 (main chat), verbatim: "also iff i close app it doesnt continue to downlaod the regulation data
updates." (He is on Android, Play 0.8.2 build 37; promoting it to closed testing for himself the same minute.)

## Work
T298 meant the system's downloader to carry the big files in the background (`background (T298): the system's
downloader carries the big files`, tests in packages/data). Reproduce on the emulator with the Play-like build (patched
MapLibre): start an update, swipe the app away / press Home, and watch whether the download job keeps going (adb,
the downloader's logs) and whether the next open picks up the finished files or starts over. Find where the job is
tied to the app process (JS-side fetch instead of the system downloader, a foreground-only task, a job cancelled on
destroy) and fix it so a closed app keeps downloading and the next open applies it. Tests + an emulator walk.
