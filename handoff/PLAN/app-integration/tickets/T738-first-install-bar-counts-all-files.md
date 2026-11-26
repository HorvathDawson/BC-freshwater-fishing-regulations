# T738 — The first-install bar counts every file that is downloading, not one at a time

Status: IN PROGRESS (merged onto `redesign/stream-sections` 2026-11-16 via `merge/held-group1`, the owner's merge order;
left (Claude): the device runs under Results; shared code, so Android's figure changes too, the ticket's default "do it").

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T738 | The first-install bar counts every file that is downloading, not one at a time (it read 38.8 MB with 116 MB on disk after a relaunch; both platforms) | IN PROGRESS (merged onto redesign/stream-sections 2026-11-16 via `merge/held-group1`; left (Claude): the Android bg-install and iOS first-open device runs in the ticket) | — | T722 simulator proof 2026-11-14 (iOS chat); Android downloads the same way |

Source: T722's simulator proof, 2026-11-14 (branch `ios/t722-install`, run on build 347281f96). After the app was
force-closed at 104.3 MB on disk and reopened, the bar read "38.8 of 479.3 MB" while 116.3 MB were on disk. An earlier
run showed 27.3 MB with about 180 MB down. Shot: sent to the owner 2026-11-14 (`proof4/relaunch-4s.png` in the iOS
chat's scratchpad).

## Done (merged 2026-11-16, `merge/held-group1`)
- Cause (branch `ios/t738-bar`): the big-file phase already summed its four transfers; the low figure came from the phase
  BEFORE it — `installNow` (`packages/data/src/release/install.ts`) fetches the data files one by one (~44 MB) while
  the bar counted only those, though the four big files were already downloading (queued by `boot.ts`, T298). After a
  relaunch `install` starts again from the data files. Both platforms run the same `install.ts`.
- Fix: `packages/data/src/release/tally.ts` (`had`/`bytesHad`, `lookHad`, `downloadTally`: `shown` never above the total,
  never back within one install; a bad count `reportOnce`); `install.ts` looks at the big files every 500 ms until their
  transfers take over; `drivers/install.native.ts` `look`; `boot.ts` passes `dev.look` to all three `install` calls.
  Wording unchanged (`installLine`, MB). Tests `packages/data/src/release/tally.test.ts` (10; with the look off, 4 turn red).
- Merge fix: T760's still line (`useDownloadStill`, App.tsx `BootStillLine`) watches `Progress.had` (`tally.real()`), not
  the held figure; test "a big file queued again from zero (T760)".
- Android 2026-11-20 (L4, `bg-install`, build of 124d79efa; shots `handoff/PLAN/app-integration/L4-android-shots-2026-11-20/T738/`):
  relaunch PASS (shade closed first, `T738/T738-after-relaunch-shade-closed.png`): 193.3 MB of 480.8 after the tap vs 211.2
  before the kill, no jump back to zero, not re-queued from zero. In flight the bar read 13-18 MB above the throttle
  proxy's bytes every time (98.5 vs 79.9, 217.1 vs 200.0; run 3: 92.1 vs 75.2, 211.2 vs 195.3), the same gap, never going
  down: the proxy lags the downloader by what it buffered, so not proven either way.

## Work (left)
- Android: read the downloader's own `current_bytes` as root (`adb root` at the start of the run) against the bar, to
  settle the 13-18 MB gap (the shell cannot read it without ACCESS_ALL_DOWNLOADS; uiautomator never idles while the fish
  turns, so the bar's figure is read from the violet fill's share of the track times 480.8 MB, about 5 MB under the words).
- iOS: `node app/tools/testbed/run-ios.mjs first-open` and T722's background scenario (Settings in front 60 s;
  `simctl terminate` at ~100 MB; relaunch, shot at +4 s), comparing the bar with nsurlsessiond's temp files + the
  module's job bytes.
- Pass line: at every shot the bar's figure is within a few MB of the bytes held (never above them by more than the 500 ms
  poll); it never goes down between shots; the first shot after a relaunch is at least what was held at the kill; the
  install finishes at the same point as before (`[setup] downloaded` / `installed` log lines).

## Acceptance
On both platforms the bar's figure is within a few MB of what is on disk at any moment of the first install, also right
after a relaunch; it never goes down; no change to when the install finishes.

Depends: T722 merged (the iOS half). Size: S. Owner: yes (it changes how Android's bar moves; default: do it).
