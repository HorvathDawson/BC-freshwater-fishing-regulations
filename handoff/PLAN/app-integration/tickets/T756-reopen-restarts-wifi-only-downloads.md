# T756 — Reopening the app mid-download throws away a first install's big files when they were queued "Wait for Wi-Fi"

Status: IN PROGRESS (merged onto `redesign/stream-sections` 2026-11-16 with T760 in merge 3 (c62c1fa51); bench settled 076ff72ad: three bg-install sessions 8/8; left (Claude): work 5's iPhone twin run (T722 is merged))

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T756 | Reopening the app mid-download throws away a first install's big files when they were queued "Wait for Wi-Fi": `queue` removes every running Wi-Fi-only job because the reopen (now on Wi-Fi) asks "go". Keep a running job; fix the `bg-install` bench's 15-minute wait, failure text and notification tap. | IN PROGRESS (merged c62c1fa51 with T760, 2026-11-16; Android bench settled; left (Claude): the iPhone twin run) | 81591d974, c62c1fa51 | R8 lane capture 2026-11-15 (T749); Android chat: "a defect by our own rules"; rule is T298's, not R8, not T737 |

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

Why it matters: progress was thrown away without a word (rule W46, no silent fallbacks); on 2026-11-15 the restarted
225 MB basemap took 686 s. Not new: the rule is T298's (commit b75860825). Not R8, not T737.

## Done (branch `fix/t756-keep-running-job`, merged c62c1fa51 with T760, 2026-11-16)
- a03372ec5 work 1-3: `queue` keeps a job when it `serves` the rule now asked (the same rule, or Wi-Fi only + running +
  mobile data allowed); pending / paused Wi-Fi-only jobs are queued again ("Use mobile data now" still works). `fill`: a
  kept Wi-Fi-only job paused for `stuckMs` (20 s) while mobile data is allowed is queued again, logged (by time, not the
  platform's paused reason; cost: a 20-second retry pause on Wi-Fi also restarts that file). Four tests in `bgDownload.test.ts`.
- 81591d974: a paused Wi-Fi-only job that has bytes is kept; every restart from zero logs `[download] <url>: queued
  again from zero (was <status>, N of M bytes): <why>`; the bench reads that line.
- c0ad16d62, 872cfe14e, b4c1a72d9 work 4, bench `scenarios/bg-install.mjs`: fails on a restart from zero, on no byte
  line for 4 minutes (quotes the setup screen's line), and on a notification tap that does not bring the app to the
  front (it unfolds the group first); `lib/throttle-proxy.mjs` (3 MB/s through the phone's global HTTP proxy, unset at
  the end). Pure parts in `lib/download-watch.mjs` (`node --test app/tools/testbed/lib/download-watch.test.mjs`).
- Android proof (076ff72ad): `install` PASS; `bg-install` three sessions `m3d-bg1` / `m3d-bg2` / `m3d-bg3` 8/8 (APK tag
  `m3`, R8): app closed at under 3 MB of 479, the tap on the group's first notification opened the app, nothing queued
  again from zero, the map opened.
- The silent stretch between `mobile data: go` and the first file line (16 s to over 284 s) is not T756's: carried by T760.

## Left
- Work 5's iPhone twin run (T722 is merged): `bg-install` + `install` on the iPhone. Not verified: that the iPhone
  downloader (T722) reports `running` and `metered` the way this rule needs.
- Bench time only (no result depends on it): unsetting the proxy at the reopen step seems to make the system downloader
  drop and retry its connections (about 4 min); lifting the rate without unsetting until the end would shorten a session.

## Acceptance
- "Wait for Wi-Fi", Wi-Fi back, app reopened mid-download: no job is cancelled; the bytes already down are kept; the
  bench proves it from the downloader's own log.
- The mirror case and the later-pause case behave as written above, each with a test.
- A failed or slow `bg-install` names the app's own screen and the bytes, not the status bar.

Depends: nothing to start; merges after T722 (`ios/integration`) so the iPhone downloader is covered by the same tests.
Size: S (code) + S (bench). On the list for the owner's code review.

