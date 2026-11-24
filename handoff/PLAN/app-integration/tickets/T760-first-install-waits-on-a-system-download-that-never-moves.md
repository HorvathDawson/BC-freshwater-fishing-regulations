# T760 — First install waits for ever, without a word, on a system download that is paused and never moves; and the bench does not clear the downloader

Status: IN REVIEW — merged c62c1fa51 (Android, 2026-11-16); iPhone half 455ced856 (lane L6, a Wi-Fi-only job held on mobile data is an honest wait, reason 3) with the iPhone twin run 3 of 3 (6fdb3d8db); owner: OWNER-TODO "a first install on mobile data with" (optional phone check)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T760 | First install waits for ever, without a word, on a system download that is paused and never moves (found on the emulator: rows left by cut-short bench runs, adopted after `pm clear`); the bench must clear the downloader; the T756 bench check reads the wrong log line. Next free ticket T761 | IN REVIEW (iPhone half 455ced856: a Wi-Fi-only job held on mobile data is an honest wait; iPhone twin run 3 of 3, lane L6); owner: OWNER-TODO "a first install on mobile data with" (optional phone check) | fa93e83f0, e600c5dc1, c62c1fa51, 455ced856 | iOS chat 2026-11-15 (R8 bench); first install on Android is the Android chat's area |

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

## Added 2026-11-15 evening
- **Work 1 (bench) is written** on branch `fix/t756-keep-running-job`: `device.cleanApp()` (uninstall + install of
  `TESTBED_APK_FILE`; throws without it) replaces `pm clear` in `install`, `bg-install` and `memory`, and `session.sh`
  uninstalls before it installs. Seen working: "uninstalled the app left by the last batch", new uid, downloads start
  at once.
- **Work 5 is done there too** (the bench reads the app's own "queued again from zero" line).
- **Seen on the emulator, for work 2 and 3:** the system downloader's read timeouts ("Failed reading response:
  SocketTimeoutException", then WAITING_TO_RETRY) pause both big files together, again and again, for minutes; the app
  logs "paused (reason 1)" and waits. One run sat 20 minutes on a setup screen with no bar and no byte line. Whether
  the screen hides the byte line while everything is paused is NOT checked in the code yet; if it does, that is the
  silent screen of point 3.
- First install on Android is the Android chat's area: works 2 and 3 wait for its word on N and the count.
- **A second silent wait, seen 18:49–18:57 (T756 branch, run 2026-11-16T01-49-33):** after a reopen the app logged
  `[download] mobile data: go` and then nothing for more than 284 s, the screen fixed at "36.7 of 479.3 MB downloaded".
  The same stretch took 16 s and 46 s in two runs that went on to finish. In `boot.ts` it is `await queue(metered)`
  followed by `install(...)` up to the first file's first log line. To find first: what in that stretch reads the
  network or the downloader without a time limit. The bench now keeps the app's and the downloader's logs when this
  happens (`T756-stalled-logs.txt` in the run folder).

## Added 2026-11-15 late (new iOS chat): the second silent wait is found and fixed on the branch
- **Where it waited.** "36.7 of 479.3 MB" is exactly the small data files up to `salmon-4` (the phone's 31 files in
  name order sum to 36.67 MB there; "10.5 MB", where the app was closed, is the sum through `core`). So after the
  reopen the app was fetching `salmon-5` (116 kB on the wire) ITSELF: the small files are fetched one by one in
  `installNow` before the big files' `fill` logs its first line. Not the system downloader, not `queue`.
- **Why it never ended (read in the code, then proven by a test that hangs on the old code).** The app's fetch is
  `shareFetch` (T265) over expo's fetch. `fetchResilient` cuts a try by aborting its own signal after 20 s without a
  byte; `shareFetch` runs the request under its OWN signal and, once the headers arrive, drops the asker's abort
  listener, so the abort reached nothing; React Native's OkHttp has no read timeout ("No timeouts by default"). A body
  that stalled after its headers was read for ever, with no log line. The emulator's line stalls mid-body often (the
  system downloader's "Failed reading response: SocketTimeoutException" of the same hour).
- **Not proven:** that this is what happened in run 2026-11-16T01-49-33 (its logs were not kept). A second silent path
  exists and would look the same for up to about 4.5 minutes: no answer at all, 4 tries of 20 s with no log line, then
  the start fails and `App.tsx` starts it again twice without showing anything. The fix below makes both say so.
- **Fix, branch `fix/t756-keep-running-job` d4eed7594 (not merged):** `net/fetch.ts` races every wait on the transport
  against the try's abort and cancels a cut body's reader; `net/share.ts` hands the last asker the response itself
  (the others a clone), so that cancel closes the connection (before, an unread original kept it open and buffered
  each file twice); `install.native.ts` logs `[download] try N failed (why); trying again in X ms`. Six new tests, each
  failing with its part removed; `check:fast` and the 2,279 related tests pass. This touches every release-file fetch
  on phone and web (shared `packages/data`), so it needs the Android benches and the main chat's word before merging.
- **Still open here:** works 2 and 3 (a paused system job with no new bytes is queued again after N minutes; the setup
  screen says when nothing moves) wait for the Android chat's N and count. The two silent automatic restarts of a
  failed start in `App.tsx` are untouched (they log `[boot] start failed`, the screen shows nothing until the third).
- **Bench:** three `bg-install` runs of d4eed7594 on the emulator (lane 2) started 2026-11-15 21:54; result goes here.

## Added 2026-11-15 22:27 (app chat): works 2 and 3 are written, on `fix/t760-stuck-download` 53d9610d4 (stacked on d4eed7594; not merged)
- The second chat is the app chat since tonight, so N and the count are its call: **10 minutes, 2 re-queues** (owner
  may change them: To-Do row `t760-download-words`).
- **Work 2 (`bgDownload.ts` `fill`):** a job `pending`, or `paused` for a reason that is not a wait for a network
  (Android's PAUSED_WAITING_FOR_NETWORK 2 and PAUSED_QUEUED_FOR_WIFI 3 are honest waits; 1 WAITING_TO_RETRY and 4
  UNKNOWN are not; read from the SDK's `DownloadManager.java`, the Kotlin module passes the reason through), with no
  new byte for `stuckMs` is removed and queued again with a `[download]` line (file, seconds, status, reason, bytes
  lost). After `stuckTries` the install throws core's `INSTALL_WORDS.stuck`. T756's option is renamed `wifiWaitMs`
  (same 20 s, same meaning). Why 10 minutes: the downloader's own back-off is believed to wait up to about 8 minutes
  between tries (NOT verified: DownloadProvider's constants are not in the SDK sources on this machine).
- **Work 3:** `useDownloadStill` (`packages/ui/src/boot.ts`): true once the byte count has been the same for 60 s; the
  setup screen then shows core's `stillLine` under the bar. Checked in the code: while every job is paused the bar and
  the byte line STAY on screen, frozen; the "no bar, no byte line" screen is the stretch before the first small file
  finishes (the stalled fetch d4eed7594 fixes), and the line covers it too (it counts from 0 bytes).
- **A stuck failure is shown at once** (53d9610d4): `App.tsx` restarts a failed start twice before showing it; each
  restart kept the parked job and began the 10-minute waits over (about 90 minutes to the words). core
  `restartsByItself` excludes the stuck failure. The other failures still restart twice in silence (untouched).
- Tests: six in `bgDownload.test.ts`, the hook on a fake clock, the sentences whole; each part's removal fails a test
  (proven by the lane). `check:fast` passes; 4,843 related tests pass. **Not run on a device or the emulator.**
- Left: the Android `install` and `bg-install` benches on this branch; the owner's yes on the words; the Wi-Fi-wait
  case shows the same "carries on by itself" line (true, but not its own words); merge with T756 after the benches
  and a message to the main chat.

## Bench round 1, 2026-11-15 21:54–22:29 (d4eed7594, `bg-install` x3, lane 2, `--crash-stub --no-r2`): 2 PASS (8/8), 1 FAIL at the reopen step
- **The failed run (2026-11-16T04-55-15) is the fix working on a dead line, not a silent wait.** After the reopen the app
  logged `[download] try 1 failed (…down-….sqlite.gz: no answer in 20000 ms); trying again in 972 ms`, try 2, try 3,
  then `[boot] start failed`, two manifest tries (`no answer in 5000 ms`), and ended on the failure screen ("The first
  start needs a connection… Try again"). The kept downloader log shows why: from 20:58:44 to at least 21:04:23
  (emulator time) BOTH system downloads timed out on every try (SocketTimeoutException, WAITING_TO_RETRY, five rounds,
  waits of 45, 58, 60 and 71 s between them). The line was dead for five and a half minutes or more.
- Not known: whether the Mac's line was dead too or only the emulator's. Round 2 adds a line probe to the stalled step
  (Mac and emulator, status code and bytes) so the bench says so itself.
- The two passing runs logged no failed try (map up 481 s and 278 s after the reopen).
- **Open, owner's pick (To-Do row `t760-download-words`, second pick):** on a line dead for minutes the app now stops
  on the failure screen and waits for a tap, while the system downloads carry on by themselves once the line is back.
  Proposal: for a failure that is the connection's, keep trying by itself (every minute) while showing the same words.
- Round 2 (started 22:31): `install` x1 and `bg-install` x3 on `fix/t760-stuck-download` with the probe.

## Owner's answers, 2026-11-15 23:00 (question dialogs in the app chat)
- The three setup-screen lines and the numbers: "Yes, with Claude's numbers (Recommended)": the "not moved" line after
  1 minute; a parked file restarted after 10 minutes without a byte, at most 2 times.
- A connection dead for minutes: "Keep trying by itself (Recommended)": the failure screen keeps its words and its
  Try again button, and the app also tries again about once a minute, so the install finishes without a tap when the
  connection returns. TO BUILD on `fix/t760-stuck-download` (today the app restarts twice, then waits for the tap);
  only for a failure that is the connection's (never for the stuck-download failure or an app too old).

## Bench round 2, 2026-11-15 22:33–23:15 (`fix/t760-stuck-download` 6f043b450: both T760 fixes + a line probe in the bench)
- `install` PASS (4/4, up in 632 s); `bg-install` PASS 8/8, FAIL 7/8, PASS 8/8 (map up 304, 215 and 279 s after the reopen).
- The one failure is at the EARLIER step "Wi-Fi back: the download starts by itself": no progress in 120 s, the screen
  on "The map data could not be loaded. The first start needs a connection… Try again" (the app had given up on a dead
  line; the reopen step after it then passed). Whether the Mac's line or only the emulator's was dead is NOT known: the
  probe sits in the reopen step and did not run, and the app-log capture came back empty for all four runs (cause not
  looked into). This is the case the owner's "keep trying by itself" answer covers; it is not built yet.
- The probe: 6f043b450, `app/tools/testbed/lib/line-probe.mjs` (+ three `node --test` tests): a Mac fetch of the release
  manifest (status code and bytes; proven by hand: 200, 25,131 bytes), an emulator half (ping, the connectivity
  service's VALIDATED lines, a fetch if the image has a tool) that has NEVER run yet, and a one-line verdict in the
  step's failure text. To do: the same probe at the "Wi-Fi back" step.
- Tonight's tally for `bg-install` on the fixed code: 5 PASS, 2 FAIL in 7 runs, both failures with the app saying the
  connection was gone rather than sitting silent.
- Left: build "keep trying by itself"; the probe at the earlier step; find why the app-log capture was empty; then
  three clean runs before T756 + T760 merge.


## Built 2026-11-16 00:13 (app chat, Opus lane): "keep trying by itself", the earlier probe, the empty app log
On `fix/t760-stuck-download` (worktree only, not pushed, not merged, NOT run on the emulator):
- e600c5dc1, the owner's answer of 23:00: a failed first start that is the connection's tries again by itself every
  60 s (`RETRY_BY_ITSELF_MS`), behind the failure words and the Try again button. The decision is core
  `keepsTryingByItself(error, firstRunOffline)`: yes when the first run's manifest could not be read or the error is
  the environment's (`isEnvironment`); no for the stuck-download failure (checked first), an app too old, a size or
  sha failure, an HTTP refusal, the system downloader's own "failed (reason N)", anything unknown. The words stay up
  until a quiet try gets past the manifest stage (the first proof bytes move again). One try at a time: a tap during
  the app's own try shows that try, it does not start a second. The timer stops in the background and starts a fresh
  minute on return. Every failed try is reported as a tapped one is. No new on-screen words.
  Tests: `install.test.ts`, `bootRetry.render.test.tsx` (9); six mutations each caught, one survivor closed with a
  new test. Re-run by the app chat: 22 passed.
- Not covered, by design: when a dead line makes Android's system downloader END a file as failed (not paused), or
  iOS's native fill errors, the error is a plain one and waits for a tap. Whether a dead line ever ends that way is
  not known; the bench will show.
- 4e3bfffe9, bench: the "Wi-Fi back" step now passes when bytes move within 120 s of Wi-Fi returning or of the
  failure screen, without a tap, and on failure writes the line probe's verdict.
- The empty app log, cause found: Maestro 2.12.0 clears the device log at the start of every flow (`logcat -c`), and
  the bench driver read the log once after the run, so it only saw lines after the last flow. Fix in the same
  commit: the runner keeps the app's and the downloader's lines before each flow and writes
  `<run dir>/app-log-<scenario>.txt`. The scratchpad driver must copy that file, not `adb logcat -d`.
- Side finding, NOT fixed: `logcat -c` also clears the crash buffer, so a crash followed by a flow inside one bench
  step could be missed by the runner's crash check.
- Left: three clean `bg-install` runs and one `install` on the emulator (lane 2, after tonight's Play build), the
  crash-buffer finding, then T756 + T760 merge together with the benches and a message to the main chat.

## Merge 3 state, 2026-11-16 13:05 (the app chat, at its handover)
`merge/t756-t760` @ a74ea4aa0 (T756 + T760 merged on the shared head, plus two bench commits): `pnpm check` passed;
five of the six benches passed; `bg-install` passes every step except the two about the download notification with
the app closed (6 sessions before the bench rework, 1 after it). Cause as read from the runs: today's line brings the
479 MB in 35 to 60 s, so the system downloader's row is gone seconds after the app is closed and there is nothing
left to tap; "reopened mid-download: no running download is thrown away" passes every time. With the rework
(a74ea4aa0) the "closed" step passes and only the tap fails. Not merged: three fully passing `bg-install` sessions
are owed first. Details and the command: `handoff/PLAN/ios/START-HERE-ios.md`, top section, "BENCH FIX RESULT".

## Bench settled, 2026-11-16 13:20–14:00 (new app chat, session -9c)
The bench was at fault, not the app: on the Mac's full line the 479 MB were down 35 to 60 s after they started, so the
tap came after the system's notification had gone. The emulator's speed shaping does not reach its Wi-Fi. Now
`scenarios/bg-install.mjs` points the phone's global HTTP proxy at `lib/throttle-proxy.mjs` (a proxy on the Mac, one
shared budget of 3 MB/s, CONNECT tunnels relayed unread; its own test) from the clean install until the tap, then lifts
it; it is always unset at the end, and `session.sh` unsets one a cut-off run left (b4c1a72d9).
Three sessions on that (APK tag `m3`, R8, `--no-r2`, lane 2): `m3d-bg1` 8/8, `m3d-bg2` 8/8, `m3d-bg3` 8/8. Each: the
app closed at under 3 MB of 479, the shade showing "3 files downloading", the tap on the group's first notification
opened the app, nothing queued again from zero, the map opened. With `pnpm check` and the other five benches from before,
merge 3 is ready to land.
Left for later (bench time only, no result depends on it): unsetting the proxy at the reopen step seems to make the
system downloader drop and retry its connections, so the rest took about 4 min; lifting the rate without unsetting
until the end would shorten a session.

## iPhone: a Wi-Fi-only job waiting on mobile data was called stuck (lane L6, 2026-11-20, commit 455ced856)
- **Cause (read in the code).** The iOS background session holds a Wi-Fi-only task (`allowsCellularAccess` /
  `allowsExpensiveNetworkAccess` / `allowsConstrainedNetworkAccess` false) on mobile data without a word: the task
  stays running with no bytes, so `BgDownloadModule.swift` `list` reported it `pending` (reason 0). `fill` read any
  pending job as parked (T760 work 2): after 10 minutes it was removed and queued again, twice, then the install failed
  "stuck" (~30 min). Android does not hit this: DownloadManager says PAUSED with reason 3 (an honest wait).
- **Fix.** The module now gives every live job Android's numbers from the phone's current network path
  (`NWPathMonitor`): reason 2 when there is no network, 3 when the job is Wi-Fi only and the path is expensive (mobile
  data, a hotspot) or constrained (Low Data Mode), else 0. `bgDownload.ts` `waitsForNetwork(job)`: any pending, paused
  or running job with reason 2 / 3 is an honest wait, never parked; T756's later-pause rule reads it too, so an iPhone
  job still "running" but held back for Wi-Fi while mobile data is now allowed is queued to use it (as Android's paused
  one). The log line names it ("pending, waiting for a network (reason 3)"). Android: its running and pending jobs carry
  reason 0 (DownloadManager's `getReason`), so nothing changes there.
- **Tests** (`bgDownload.test.ts`, 3 new): a Wi-Fi-only job pending with reason 3, Wi-Fi only asked, two hours: never
  queued again, claimed as it was (FAILS on the old code: queued again); a kept Wi-Fi-only job running with reason 3
  while mobile data is allowed: queued again to use it, its line whole (FAILS on the old code: waits for ever); the
  control: the same pending job with reason 0 (on Wi-Fi) is still queued again after `stuckMs`.
- **Not provable on the simulator:** it shares the Mac's network (always Wi-Fi), so the path is never expensive there.
  The Swift half compiled in the simulator build (see the run below). The real check is the owner's optional phone
  check (lane plan item 6: a first install on mobile data with "Wait for Wi-Fi").
- **Android benches on the change** (APK tag `l6`, commit c682a5af5, R8, patched MapLibre, `--no-r2`, lane 1, 08:00-08:08):
  `install` PASS (up 72 s after launch). `bg-install` 6 of 9 steps: the app asked "Download about 480.8 MB using mobile
  data?" (shot), but the bench's screen read came back empty twice ("no text … (saw: )", uiautomator dumps failing beside
  "dumpsys connectivity: Broken pipe"), so the ask and "Wait for Wi-Fi" steps failed in the bench; the notification tap
  failed on the known race (the system's downloads were done 5 s after the app was closed, the shade empty). The steps
  this change touches passed: "Wi-Fi back: the download starts by itself", "app CLOSED mid-download", "reopened
  mid-download: no running download is thrown away … no download queued again from zero", map up 110 s later. The app
  log has no "stuck" and no "waiting for a network" line (Android's running / pending jobs carry reason 0). The 0-byte
  Wi-Fi-only data-file jobs were queued again on the reopen, as T756 designed (nothing down, nothing lost).
- **iPhone twin run (work 4)** (`scenarios-ios/first-relaunch.mjs`, build 6fdb3d8db `--testbed`, iPhone 17e, iOS 26.5,
  a FRESH install via `session-ios.sh --fresh`, 2026-11-20 12:14-12:20): 3 of 3 PASS. The process was ended at the bar's
  157.3 of 480.8 MB (the background session held 148 MB in 4 jobs: atlas 64.2 MB and basemap 72.5 MB pending, two small
  files done); opened again at once, the bar's first figure was 307.9 MB at +5.3 s (never under what was held; 9 looks,
  never down); the install ended on the map, all 4 jobs held at the kill claimed as they were, nothing queued again from
  zero, nothing called stuck. The same run is T738's relaunch proof on iOS (the bar never goes down after a reopen) and
  T756's "no running download is thrown away" on iOS.
- Left: the owner's optional phone check (OWNER-TODO 2026-11-20 12:15, "a first install on mobile data with").
