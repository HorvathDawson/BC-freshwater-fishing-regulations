# T298 — First download: keeps going in the background (notification), asks before using mobile data, fish sayings while it runs

Status: DONE b75860825…e3dd669ed (2026-10-30) — merged into redesign/stream-sections; next APK.

Source: owner: "for the initial download of the offline regs can we make it so it can work in background if user closes
app? also it should ask user for permission to download off wifi if they are not connected. finally we want to add cute
fish sayings that cycle as the download is happening. for downloading in the background it could become a notification
bar download?"

## Work
1. **Background download (Android):** the first install (release files, basemap, tiles, map assets, synopsis PDF) keeps
   downloading when the app is closed or the screen is off, with a progress notification in the notification bar
   ("Downloading BC fishing rules · 45%"); tapping it opens the app. Use the platform's own mechanism (Android
   DownloadManager or a foreground service / WorkManager via an Expo module or config plugin) — resumable, survives app
   close; on the next open the app picks up finished files, verifies sha256 (T262/T272 path) and finishes setup. iOS: a
   background URLSession download where expo-file-system supports it; note what's possible. Notification permission asked
   only when needed (Android 13+), with a plain reason.
2. **Mobile data:** before a large download (first install, a big update), if the device is on cellular (or metered), ask:
   "Download about N MB using mobile data?" — **Download now** / **Wait for Wi-Fi** (resumes automatically on Wi-Fi). Small
   updates (< ~5 MB) don't ask. (Owner change 2026-10-29) The answer is SAVED as a setting, not per download: "Download
   over mobile data" — **Always / Ask / Wi-Fi only** (default Ask until answered), shown in More → Data where it can be
   changed any time; on answering, one line says so ("Saved. Change this any time in More → Data."). Words in core tables.
3. **Fish sayings:** short, cute fishing lines that cycle every few seconds under the progress bars during the download
   (in the spirit of the search placeholders; never promise fish; e.g. "Untangling the leader…", "Checking the tackle box…",
   "Waiting for the bite… of data"). Words in core tables; cycle respects reduced-motion (no animation, just swap).

## Acceptance
Emulator: start the first install, close the app → notification progresses → reopen → setup finishes; airplane/cellular
emulation shows the prompt and "Wait for Wi-Fi" resumes on Wi-Fi; sayings cycle; screenshots.

Depends: T262, T272. Size: M–L. Owner: no.

## Outcome (2026-10-30)
**Choice: Android's own DownloadManager**, through a local Expo module (`apps/mobile/modules/bg-download`, Kotlin, no
package). Over a foreground service / WorkManager (`dataSync`): no `FOREGROUND_SERVICE_DATA_SYNC` + Play Console
declaration, no 6-hour cap (Android 15), and the system does the resume and the "Wi-Fi only" wait itself. Over
`@kesha-antonov/react-native-background-downloader` (same DownloadManager underneath): no third-party package. Why in
`app/deps.md`.
- **Background:** before the install starts, every big file (tiles, mask, basemap, rule book: ~95 % of 475 MB) is
  queued with the system at once (`release/bgDownload.ts`, platform-free + tested); they download in the system's
  process with its notification, the app open or closed. The install waits on each job, moves its file into the
  install root and checks the sha256 natively (~1 s for the basemap; JS took 23 s), then renames it into place (the
  T262 `.part` path; an older app's `.part` with no job resumes the old way). A finished job whose file is gone (the
  app's data cleared) is queued again. The small data files and map assets stay in-app (they finish on the next open).
- **Notification:** the system's ("Downloading BC fishing rules" · "Base map · 274.5 MB", grouped as "2 files
  downloading"). Android 13+ shows it only for an app allowed to notify: asked once before the first download, with
  the reason on the boot screen (`DOWNLOAD_NOTE_WORDS.why`). One notification per file, not one combined bar (the
  system's). A tap on it does not open the app on Android 15 (the system's background-start rule); the launcher does.
- **Mobile data (owner change):** one saved setting "Download over mobile data — Always / Ask / Wi-Fi only" (update
  service, migrates the old "large updates on mobile data" switch; More → Data replaces that switch with the three
  choices; the bell's "Download now" and Offline maps' Wi-Fi-only switch drive the same setting). On Ask the boot screen
  asks "Download about 475.2 MB using mobile data?" — Download now / Wait for Wi-Fi — saves the answer and says "Saved.
  Change this any time in More → Data." Wait: the system jobs are queued Wi-Fi-only (they start on Wi-Fi by themselves,
  app open or not), the screen says it is waiting and offers Download now. Below 5 MB never asks.
- **Sayings:** `DOWNLOAD_SAYINGS` (core), one every 4 s under the bars (`SayingLine`): fade with motion, plain swap
  with reduce-motion; none promises fish.
- **Low-bandwidth mode (owner 2026-10-30):** hidden on a phone (More → Data); the offline banner stays; the slow-line
  banner shows on a phone only while live data (gauges, forecasts, weather) is loading. Web unchanged.
- iOS: no module (not built); expo-file-system's `DownloadTask` already runs on a background URLSession.

Emulator (test bed `bg-install`, release APK, staging, Android 15): notification permission asked → on mobile data the
prompt → Wait for Wi-Fi (saved + said, sayings turning, nothing downloaded) → Wi-Fi: starts by itself → app process
killed at ~40 MB: "2 files downloading" in the notification bar → relaunch: setup finishes, map up → More shows Wi-Fi
only. Shots: `../T298-shots/`. The emulator's Wi-Fi is not throttled (the set lands in ~1 min), so "closed" was seconds.
Also in this branch (coordinator): test bed `regs-open` taps the water at the app's own `[target]` (test-build hook in
Map.native) and never sends Back at the map root; `satellite` puts the Map base back (by id); `install`/`appUp` allow
the notification dialog. Note (not blocking): regs-open's stream cases (Norrish Creek, Fraser) still miss — the `[target]` point is off the
highlighted line on screen (the lake case passes 3/3); cause not found yet.
