# T760 — First install waits for ever, without a word, on a system download that is paused and never moves; and the bench does not clear the downloader

Status: IN REVIEW — merged c62c1fa51 (Android, 2026-11-16); iPhone half 455ced856 (lane L6, a Wi-Fi-only job held on mobile data is an honest wait, reason 3) with the iPhone twin run 3 of 3 (6fdb3d8db); owner: see ## Owner (optional phone check)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T760 | First install waits for ever, without a word, on a system download that is paused and never moves (found on the emulator: rows left by cut-short bench runs, adopted after `pm clear`); the bench must clear the downloader; the T756 bench check reads the wrong log line. Next free ticket T761 | IN REVIEW (iPhone half 455ced856: a Wi-Fi-only job held on mobile data is an honest wait; iPhone twin run 3 of 3, lane L6); owner: see ## Owner (optional phone check) | fa93e83f0, e600c5dc1, c62c1fa51, 455ced856 | iOS chat 2026-11-15 (R8 bench); first install on Android is the Android chat's area |

Source: 2026-11-15, the R8 bench (T749). Traced on the emulator, not inferred. Owner's answers, 2026-11-15 23:00
(question dialogs in the app chat): the setup-screen lines and numbers: "Yes, with Claude's numbers (Recommended)" (the "not
moved" line after 1 minute; a parked file restarted after 10 minutes without a byte, at most 2 times); a connection dead for
minutes: "Keep trying by itself (Recommended)".

## What
`pm clear` keeps the app's uid, so the system downloader kept rows left by cut-short bench runs; the fresh app found a
row for the same address, saw "paused" (not failed, not stale), kept it (`bgDownload.ts` `queue`) and `fill` polled it
for ever. On a real phone a battery saver, a reboot or the downloader's own back-off leave a job "paused" the same way:
the setup screen showed a bar that does not move and "You can close the app: the download carries on", for ever, with
nothing shown or reported (rule W46).

Done (merged c62c1fa51 with T756, Android 2026-11-16; iPhone half 455ced856):
- Bench: `device.cleanApp()` (uninstall + install of `TESTBED_APK_FILE`) replaces `pm clear`; `session.sh` uninstalls
  first; the T756 check reads the app's own "queued again from zero" line; `lib/line-probe.mjs`; the runner keeps the
  app's and downloader's lines before each flow (`<run dir>/app-log-<scenario>.txt`; Maestro 2.12.0 runs `logcat -c`
  per flow); `scenarios/bg-install.mjs` throttles through `lib/throttle-proxy.mjs` (3 MB/s) until the tap (b4c1a72d9).
- App: the stalled-body fetch fixed (`net/fetch.ts`, `net/share.ts`, `install.native.ts` logs each failed try);
  `fill` re-queues a job `pending` or `paused` for a non-network reason (1, 4) with no new byte for `stuckMs` (10 min),
  `stuckTries` 2, then `INSTALL_WORDS.stuck`; `useDownloadStill` (`packages/ui/src/boot.ts`) shows `stillLine` after
  60 s; a stuck failure is shown at once (`restartsByItself`); "keep trying by itself" every 60 s for a connection
  failure (e600c5dc1, `RETRY_BY_ITSELF_MS`, `keepsTryingByItself`).
- iPhone: `BgDownloadModule.swift` gives live jobs Android's reasons from `NWPathMonitor` (2 no network; 3 Wi-Fi only on
  an expensive or constrained path); `waitsForNetwork(job)` treats 2 / 3 as an honest wait. iPhone twin run 3 of 3 PASS
  (6fdb3d8db). Not provable on the simulator (always Wi-Fi).

## Work (left)
- The owner's optional phone check (below).
- Recorded, not fixed: `logcat -c` also clears the crash buffer, so a crash followed by a flow inside one bench step could
  be missed by the runner's crash check. The other start failures still restart twice in silence in `App.tsx` (they log
  `[boot] start failed`, the screen shows nothing until the third). The Wi-Fi-wait case shows the same "carries on by
  itself" line (true, but not its own words). Not covered by design: when a dead line makes Android's system downloader
  END a file as failed, or iOS's native fill errors, the error is a plain one and waits for a tap. Bench time only:
  unsetting the proxy at the reopen step seems to make the downloader drop and retry its connections (about 4 min);
  lifting the rate without unsetting until the end would shorten a session.

## Acceptance
- A stale paused row for the same address no longer hangs a first install: it is queued again and said in the log.
- A bench run cut short leaves nothing behind that the next run adopts.
- The setup screen never sits silent on a download that does not move.

Depends: T756 (same file; land together or after). Size: M. On the list for the owner's code review.

## Owner

- [ ] 2026-11-20 — Optional, on a real iPhone: first install on mobile data, pick "Wait for Wi-Fi", leave it 40 minutes, then turn Wi-Fi on; it should wait quietly and start by itself. Default: ship as built (Android already behaves so).
