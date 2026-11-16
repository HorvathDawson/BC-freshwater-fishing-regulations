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

## Results (2026-11-15, iOS chat)
Branch `fix/t756-keep-running-job`, commit a03372ec5, off the shared head ff4463a61. Pushed, NOT merged.
- **Work 1, done:** `queue` keeps a job when it `serves` the rule now asked: the same rule, or Wi-Fi only + running +
  mobile data allowed. Pending / paused Wi-Fi-only jobs are queued again as before ("Use mobile data now" still works).
- **Work 2, done, platform-free:** in `fill`, a kept Wi-Fi-only job that sits paused for `stuckMs` (20 s) while mobile
  data is allowed is removed and queued again under the allowed rule, with the line `[download] <url>: a Wi-Fi-only
  download paused (reason N) at X of Y bytes while mobile data is allowed; queued again to use it`. It goes by time,
  not by the platform's paused reason, so the shared code needs no Android or iPhone constant; the cost is that a
  20-second retry pause on Wi-Fi also restarts that file.
- **Work 3, done:** four tests in `bgDownload.test.ts` (kept when running; the mirror case queued again; the later
  pause queued again once with its line; a short pause waited out). Each fails with its half of the change removed
  (checked by hand). The file: 11 passed. `pnpm -C app check:fast` exit 0; `tools/failures`, `tools/no-silent-skips`,
  `tools/release-boot` and `packages/data/src/release/` pass (24 files, 220 tests).
- **Work 4, written 2026-11-15 14:50, NOT run on a device** (commit c0ad16d62 on the same branch; no internet line):
  `scenarios/bg-install.mjs` (a) reads the system downloader's log every ~20 s through the reopen (it rolls on a long
  run) and FAILS when the app stopped a running job ("onStopJob id=N, reason=cancel() called by app"), naming the jobs;
  (b) waits for as long as the app's "X of Y MB downloaded" line or the bytes on disk move, fails after 4 minutes
  without movement (45 minutes in all) and quotes the setup screen's own line and the bytes on disk; (c) the
  notification tap is its own step: it unfolds the system's group ("2 files downloading") first, and a tap that does
  not bring the app's activity to the front FAILS (before, it fell back to a launch and passed). Pure parts in
  `lib/download-watch.mjs`, five node tests on the downloader lines captured from the defect
  (`node --test app/tools/testbed/lib/download-watch.test.mjs`); `tools/testbed-static.test.ts` 20 pass.
  To check on the first device run: that a group tap unfolds (not opens) on this Android image; that a finished file's
  tidy-up never logs an onStopJob cancel; that verifying after "479.3 of 479.3 MB" is not read as a stall.
- **Not done:** work 5 (no device run yet: Android `bg-install` + `install`; iPhone after T722 merges).
- **Not verified:** that the iPhone downloader (T722) reports `running` and `metered` the way this rule needs; that
  DownloadManager reports a Wi-Fi-only job as `running` (not `paused`) for the whole time it moves on Wi-Fi.

## Device runs, 2026-11-15 evening (emulator n1_pixel, branch `fix/t756-keep-running-job`, lane 2, `--no-r2`)
Branch now: the shared head 35dce9bf3 merged in, plus 81591d974 (a paused Wi-Fi-only job that has bytes is kept; every
restart from zero logs `[download] <url>: queued again from zero (was <status>, N of M bytes): <why>`; the bench reads
that line, not the system downloader's), the T760 bench change (a clean app is an uninstall + install) and 872cfe14e.
- **`install` PASS** (run 2026-11-16T01-29-07, commit 8ada197ac): map up 635 s. The system downloader hit read
  timeouts for about seven minutes (both big files "paused (reason 1)" then "running" again every 20–60 s) and
  finished by itself; the host's own line was steady at the time (120 MB twice at 1.7 MB/s), so the stalls are in the
  emulator's network path or came and went.
- **Reopen mid-download, by hand** (first launch on Wi-Fi, killed at 45 s, relaunched): after the relaunch the app
  logged both big files "running, 30302145 of 197196706 bytes" / "76166400 of 225356297 bytes", no "queued again from
  zero" line, and both finished and verified (basemap "fill 125426 ms"). Bytes kept. This is the both-rules-equal
  case; the "Wait for Wi-Fi, then reopened" case is what `bg-install` drives.
- **`bg-install`, first try, FAILED in the bench, not in the app:** the new tap step found no notification because
  `expand-notifications` on an already open shade turns it into the quick settings; and the reopen step then waited to
  its cap because its progress measure was wrong (the downloader's files are full-size on disk from the first second,
  by `ls -l` and by `du`). Both fixed in 872cfe14e: the shade is closed first; progress is the app's own byte line,
  and no byte line at all for 4 minutes is a stall. Re-runs below.
- The earlier `install` failure of the same build (17:22, 20 minutes, the setup screen with NO bar and NO byte line)
  is not explained by a run of mine; the log kept only "mobile data: go". It may be the same read timeouts lasting
  longer; a screen that shows nothing while every download is paused is T760's point 3.
- **`bg-install` with the fixed bench, two runs of the same app code:**
  - 18:49 (commit 872cfe14e, run 2026-11-16T01-49-33): steps 1–6 PASS, **the notification tap opens the app** ("opened
    by a tap on the group "2 files downloading" unfolded, then its first notification"); the reopen step FAILED on
    the new stall rule: 249 s at "36.7 of 479.3 MB downloaded", and the app logged nothing after
    `[download] mobile data: go`. No log was kept (the bench now keeps both logs on a failure).
  - 18:59 (run 2026-11-16T01-59-44): **8 of 8 PASS.** "opened by the notification tap; map up 282 s later; no download
    queued again from zero". After the reopen both big files were found "running, 96918696 of 197196706 bytes" /
    "39018496 of 225356297 bytes" and finished: the bytes down before the reopen were kept. This is the acceptance
    case ("Wait for Wi-Fi", Wi-Fi back, app reopened mid-download), proven once.
  - Not explained, and not T756's own: between `mobile data: go` and the first line of any file the app is silent for
    16 s (by hand), 46 s (the passing run) and more than 284 s (the failed run). `boot.ts` does `await queue(metered)`
    and then `install(...)` there. A lead for T760, not verified: something in that stretch waits on the network with
    no time limit while the emulator's connections stall (the system downloader's own read timeouts fire in the same
    minutes).
- **State:** work 1–4 done and proven once on the emulator; the unexplained silent stretch is carried by T760. Not
  merged; goes in with the iOS merges after build 33, after one more green `bg-install` + `install`.
