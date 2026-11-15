# T756 — Reopening the app mid-download throws away a first install's big files when they were queued "Wait for Wi-Fi"

Status: IN PROGRESS (iOS chat; the fix is in shared code, so it needs the Android bench and a word to the Android chat before it merges).

Source: found 2026-11-15 by the R8 lane (T749) while the `bg-install` bench hung on a slow line; mechanism read from the
code by the iOS chat the same day. The Android chat (first install on Android is its area) found no decision that
accepts it and asked for this ticket: "It is a defect by our own rules".

## What happens
1. First open on mobile data. The reader picks "Wait for Wi-Fi". `boot.ts` calls `background.queue(big, false)`: the
   big files (atlas, basemap, mask, rule book; about 95 % of 479 MB) are handed to the system's downloader as
   Wi-Fi-only jobs.
2. Wi-Fi comes back; the jobs run by themselves, the app open or closed (T298). So far as designed.
3. The app is opened again while they run (or was closed and is relaunched). Now on Wi-Fi, `mobileData.decide()`
   answers "go", and `boot.ts` calls `queue(big, true)`.
4. `bgDownload.ts` `queue` keeps a job only when `job.metered === allowMetered`. The running jobs are `metered:
   false`, the call says `true`, so every one is removed (`bg.remove`) and enqueued again from zero.

Captured (flag-on test-bed APK of 164c928c2, emulator, 2026-11-15, the app restarted at 11:58:04):
`[download] mobile data: go (479277504 bytes)`, then DownloadManager "onStopJob id=441 / 443, reason=cancel() called by
app", the partial atlas and basemap `.pmtiles` deleted, new jobs 445 / 446 from zero. Files in the scratchpad `hang/`
(hang.png, logcat.txt, dm.txt).

The mirror case also restarts, and there it must: jobs queued "go" (mobile data allowed), the app reopened on mobile
data with the setting now "Wi-Fi only". The system cannot change a job's network rule, so honouring the new choice means
a new job.

## Why it matters
- Progress is thrown away without a word (rule W46, no silent fallbacks). On a slow line it can double the wait: on
  2026-11-15 the restarted 225 MB basemap took 686 s.
- It is on Wi-Fi in the captured case, so the reader does not pay for the bytes twice there. A reader who allowed
  mobile data later ("Use mobile data now") and reopens does.
- Not new: the rule is T298's (commit b75860825). The passing bench runs restarted too ("map up 260 s / 304 s later" is a
  whole second download on a fast line). Not R8, not T737.
- iPhone: T722 (`ios/t722-install`, not merged) added the iPhone downloader behind the same `queue`, so the same rule
  applies there once it merges. T722's own fix (a relaunch re-attaches to the running jobs) does not cover this case.

## Work
1. `packages/data/src/release/bgDownload.ts` `queue`: a job that is RUNNING under a stricter rule than the one now
   asked (queued Wi-Fi-only, mobile data now allowed) is kept. It is on an unmetered network; both choices permit it.
   A job that is pending or paused under the stricter rule is still queued again (it is waiting for Wi-Fi and the
   reader has just allowed mobile data; nothing is lost, it has no bytes worth keeping or is not moving).
   The looser-to-stricter case stays as it is (queued again; the choice is honoured).
2. `fill`: a kept Wi-Fi-only job that later pauses for want of Wi-Fi while mobile data is allowed must not wait
   forever: queue it again then, and say so in the `[download]` log (no silent switch). Decide by the platform's
   paused reason (Android `PAUSED_QUEUED_FOR_WIFI`; the iPhone module's equivalent).
3. Tests in `bgDownload.test.ts` (fake downloader): kept when running; queued again when paused / pending; the mirror
   case queued again; the later-pause case queued again once, with its log line.
4. Bench `bg-install`: (a) after the reopen, read the DownloadManager lines and FAIL if a job was cancelled by the app
   in the "Wait for Wi-Fi, then reopened on Wi-Fi" path; (b) its step "a tap on the notification opens the app; setup
   finishes and the map opens" waits 15 minutes whatever the line speed, and when that runs out its failure text quotes
   the status bar's words, not the app's: wait on bytes moving (fail on no progress for N minutes) and name the setup
   screen's own line in the failure; (c) it taps the text "Downloading BC fishing rules" while the shade shows the
   system's group ("2 files downloading"), so the tap never opens the app and the step falls back to a launch: tap the
   group, or expand it first. The notification tap is on the owner's phone-walk list and is NOT covered by the bench
   today.
5. Android device run: `bg-install`, `install`; iPhone twin once T722 is merged.

## Acceptance
- "Wait for Wi-Fi", Wi-Fi back, app reopened mid-download: no job is cancelled; the bytes already down are kept; the
  bench proves it from the downloader's own log.
- The mirror case and the later-pause case behave as written above, each with a test.
- A failed or slow `bg-install` names the app's own screen and the bytes, not the status bar.

Depends: nothing to start; merges after T722 (`ios/integration`) so the iPhone downloader is covered by the same tests.
Size: S (code) + S (bench). On the list for the owner's code review.
