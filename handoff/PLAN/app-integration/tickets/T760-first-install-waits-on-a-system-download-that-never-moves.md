# T760 — First install waits for ever, without a word, on a system download that is paused and never moves; and the bench does not clear the downloader

Status: OPEN (iOS chat found it; first install on Android is the Android chat's area, so the fix needs its word; the bench half can start at once).

Source: 2026-11-15, the R8 bench (T749). Traced on the emulator, not inferred.

## What happened
- The six-bench run of 15:17 failed at `install`: 20 minutes at "0.6 of 479.3 MB downloaded", then every later scenario
  failed "the app is not up". The Mac's line was fine at the time (8 MB in 5 s from the same host).
- The app's own log, three relaunches in a row (14:48, 14:49, 14:51 emulator time):
  `[download] …/atlas-….pmtiles: paused (reason 1), 18436761 of 197196706 bytes` and
  `…/basemap-….pmtiles: paused (reason 1), 17413559 of 225356297 bytes`: the same bytes each time.
- The system downloader's log for that whole run has "Starting" lines only for the two small files (jobs 453, 454).
  Nothing for the atlas or the basemap, and `dumpsys jobscheduler` holds no download job for them. Their rows were
  left by the morning's cut-short runs (a run stopped by its time cap while downloading).
- `pm clear` keeps the app's uid, so the system downloader keeps its rows. The fresh app finds a row for the same
  address, sees "paused" (not failed, not stale), keeps it (`bgDownload.ts` `queue`), and `fill` then polls it for
  ever: only a FAILED job, or after T756 a Wi-Fi-only one while mobile data is allowed, is ever queued again.
- After an uninstall (which drops the uid's rows) and an install of the same APK, the download ran at once.
- So the bg-install failures of the morning ("468.2 of 479.3 MB": one 11.7 MB file short) were very likely this,
  not R8 and not the internet line, which the iOS chat blamed (wrongly from 13:04 on; see the handoff).

## Why it matters on a real phone
A row the system has parked and does not run again is not only an emulator thing: a battery saver that defers the
system downloader's jobs, a reboot at the wrong moment, or the downloader's own back-off after errors all leave a job
"paused" for a long time. The setup screen then shows a bar that does not move and the line "You can close the app:
the download carries on", for ever. Nothing is shown and nothing is reported (rule W46).

## Work
1. **Bench (can start now):** a scenario's "clean app" also clears the system downloader's rows for the app (uninstall
   + install, or the test-bed build's own way to remove every job it owns), and `session.sh` does it once before a
   batch. A run cut by its cap must not poison the next one.
2. **App, `packages/data/src/release/bgDownload.ts` `fill`:** a job that is paused or pending with NO new bytes for N
   minutes while the network its rule needs is up is removed and queued again, with a `[download]` line that says so
   (which file, how long, the reason code, the bytes it had). A few times, then the install fails with words on the
   setup screen and a report, instead of waiting. N and the count to be agreed with the Android chat; a job that is
   honestly waiting for Wi-Fi is not stuck.
3. **Setup screen:** when nothing has moved for a while it says so in its own words (not the fish sayings alone) and
   offers the retry.
4. Tests in `bgDownload.test.ts` (the fake downloader can hold a job paused), each failing with its part removed; then
   the Android `install` and `bg-install` benches, and the iPhone twin once T722 has merged.
5. **Fix the T756 bench check on the same visit:** `lib/download-watch.mjs` `cancelledByApp` (branch
   `fix/t756-keep-running-job`, not merged) reads "onStopJob … cancel() called by app" as the app stopping a job. That
   line is the system downloader's own (callingUid is its uid) and also appears after every "WAITING_TO_RETRY …
   rescheduling", so the check would fail on any network retry. Replace it with a `[download]` line the app itself
   logs whenever `queue` removes a job that had bytes, and have the bench read that.

## Acceptance
- A stale paused row for the same address no longer hangs a first install: it is queued again and said in the log.
- A bench run cut short leaves nothing behind that the next run adopts.
- The setup screen never sits silent on a download that does not move.

Depends: T756 (same file; land together or after). Size: M. On the list for the owner's code review.
