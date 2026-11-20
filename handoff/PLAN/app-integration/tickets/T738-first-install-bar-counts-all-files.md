# T738 — The first-install bar counts every file that is downloading, not one at a time

Status: IN PROGRESS (merged onto `redesign/stream-sections` 2026-11-16 via `merge/held-group1`, the owner's merge order;
left (Claude): the device runs under Results; shared code, so Android's figure changes too, the ticket's default "do it").

Source: T722's simulator proof, 2026-11-14 (branch `ios/t722-install`, run on build 347281f96). After the app was
force-closed at 104.3 MB on disk and reopened, the bar read "38.8 of 479.3 MB" while 116.3 MB were on disk. An earlier
run showed 27.3 MB with about 180 MB down. Shot: sent to the owner 2026-11-14 (`proof4/relaunch-4s.png` in the iOS
chat's scratchpad).

## Cause (read in the code by the T722 lane)
The system downloads the four big files at once (tiles, mask, basemap, rule book), on Android (DownloadManager) and, on
the T722 branch, on iOS (a background URLSession). The first-install progress adds the files that are finished plus only
the ONE file whose turn it is; bytes already down for the other three do not show until their turn. So the bar reads low
for most of the download and jumps when a turn changes. T722's iOS module already reports each job's bytes at once after
a relaunch (commit 347281f96); the under-count is in the shared JS that sums them.

## Work
1. The first install's progress sums the bytes of every queued job while the big files download (one total, both
   platforms). The words under the bar stay as they are.
2. Tests: a whole-sentence test of the bar's line with two jobs part-way; a mutation that counts one job turns it red.
3. Android: `regs-open` and `tap-stalls` benches, plus the Android `bg-install` scenario with a shot mid-download; iOS:
   the T722 first-install run with a relaunch at about 100 MB (the first shot shows at least what was on disk).

## Acceptance
On both platforms the bar's figure is within a few MB of what is on disk at any moment of the first install, also right
after a relaunch; it never goes down; no change to when the install finishes.

Depends: T722 merged (the iOS half). Size: S. Owner: yes (it changes how Android's bar moves; default: do it).

## Results

### Cause (read in the code, branch `ios/t738-bar` from `origin/ios/t722-install`)
The ticket's first reading ("one file at a time") was close but not exact: the big-file phase already ran its four
transfers side by side and summed them (`install.ts` @ t722-install, lines 386-402). The low figure came from the
phase BEFORE it:
- `packages/data/src/release/install.ts` @ t722-install:339-352: `installNow` first fetches the data files one by one
  (~44 MB, each ≤ 7 MB), then re-verifies them (354-361). The bar's figure there was `bytes`, the data files only
  (350: `bytes += e.bytes`). The four big files were already downloading in the system's downloader (queued by
  `boot.ts` before `install` starts, T298), but they were not counted until line 395 started their own transfers and
  each first reported (389: `bytes + sum(downs)`, `downs` starting at 0).
- So while the data files were fetched the bar showed at most ~44 MB however much the big files held. Both T722
  observations sit under that: "38.8 of 479.3 MB" with 116.3 MB on disk, "27.3 MB" with ~180 MB down.
- After a relaunch: `install` runs again from the start (data files: each kept file is re-read and hashed, which takes
  time), so the figure went back to the data files' sum while the big jobs held everything done before the kill. It
  caught up only when the big phase reached them.
Both platforms run the same `install.ts`, so Android had the same fault.

### Fix
- `packages/data/src/release/tally.ts` (new, platform-free): `had`/`bytesHad`, the sum over every file (each held to
  its size, the whole to the total; -1 = "not known yet" counts as none; a BigInt is its number; NaN is thrown);
  `lookHad`, what a big file holds before its turn (the finished file = all of it; a live system job = its count; a
  failed or emptied job = nothing, it is fetched again; no job = its `.part`); `downloadTally`, the running figure
  (`shown` never above the total and never back within one install; `real` decides when the second bar starts). A bad
  count is reported once (`reportOnce`) and the file keeps its last count.
- `install.ts`: from the first moment of `install`, the big files are LOOKED AT (every 500 ms, the downloader's own poll)
  until their own transfers take over, and every progress the bar gets is `tally.shown()`. A look that fails is reported
  once; the figure then stays where it was.
- `drivers/install.native.ts` `look`: the facts only, one `jobs()` for all four files plus the two file sizes each.
  The web's device is null, so it gets no look (it installs nothing). `boot.ts` passes `dev.look` to all three
  `install` calls (first install, the T403 reinstall, updates; an update now also counts a `.part` an earlier attempt
  left, from the first moment).
- The second bar ("Checking the downloaded files") is unchanged. The wording is unchanged too: `installLine`, MB.

What a reader sees differently, both platforms: during a first install the figure includes the big files' bytes from
the first moment. It climbs steadily instead of sitting at the data files' ~44 MB and then jumping. After a relaunch it
shows what was already down at once. Nothing else changes: when the files download, the order, the notification, and
when the install finishes are all the same.

### Tests (`packages/data/src/release/tally.test.ts`, 10)
The sum alone covers: parallel files, the clamps, -1, BigInt, NaN, each `Look` case, never back, and a bad count
reported. Through `install` with a fake phone, checked against the line `installLine` makes (App.tsx shows it under
the bar):
- relaunch at 60 + 44 MB held: the first line is "104.0 of 482.0 MB downloaded" before the data file is fetched
  (the old code's first line was "2.0 of 482.0").
- "116.0" when the tiles' transfer reports 70 MB while the basemap still holds 44 MB (counting one job: 72.0).
- the figure moves while the data files wait (15.0 → 65.0).
- a finished file, a pending job (-1), an older app's `.part` and a BigInt count: "129.3 of 512.0".
- a failing look: reported once, the figure holds, the install finishes.
- no look: as before.
Mutation: with the look switched off, the 4 look tests turn red.

### Device runs that prove it (not run here)
- **Android**: `node app/tools/testbed/run.mjs install bg-install`. At three moments of the first install (early,
  about half, late), take a shot of the bar and at the same moment the bytes held (`adb shell dumpsys` / the
  DownloadManager's `bytes_so_far` for the four jobs + data files on disk). Then kill the app mid-download, relaunch,
  and take the first shot within ~5 s.
- **iOS**: `node app/tools/testbed/run-ios.mjs first-open` and T722's background scenario (Settings in front 60 s;
  `simctl terminate` at ~100 MB; relaunch, shot at +4 s), comparing the bar with nsurlsessiond's temp files + the
  module's job bytes.
- **Pass line**: at every shot the bar's figure is within a few MB of the bytes held (never above them by more than
  the 500 ms poll); it never goes down between shots; the first shot after a relaunch is at least what was held at the
  kill; the install finishes at the same point as before (`[setup] downloaded` / `installed` log lines).

## Merged, 2026-11-16 (`merge/held-group1`)
The code merged without a conflict onto the head's T756 / T760 install (only the ticket and the ledger conflicted). One
interaction fixed in the merge: T760's "the download is not moving" line (`useDownloadStill`, App.tsx `BootStillLine`)
watched the bar's figure, which this ticket holds up ("never back"). When T760 queues a stuck big file again from zero,
the re-download catches up behind a held figure, so the line would have said "not moving" while it moved. `Progress`
now also carries `had` (the sum as it is, `tally.real()`), and the still line watches that. Test: `tally.test.ts`
"a big file queued again from zero (T760)". Android bench for it: `bg-install` plus the T760 stuck-download scenario.

## Android measure 2026-11-20 (L4, `bg-install` extended, emulator, build of 124d79efa)
New step "T738: the bar against the bytes held" at three moments of the first install on the bench's 3 MB/s line: a shot, the bar's
figure, the bytes held. The bench cannot read the system downloader's own count from the shell (ACCESS_ALL_DOWNLOADS), so the held
bytes are the throttle proxy's count; and the bar's words cannot be read while the fish turns (uiautomator never idles), so the bar's
figure is the violet fill's share of the grey track times 480.8 MB (about 5 MB under the bar's own words). Run 2: the shot says 98.5 MB
(pixels 92.9) with the proxy at 79.9 MB, then 217.1 MB (pixels 212.9) with the proxy at 200.0 MB: the bar read 13-18 MB above the proxy's
bytes at both moments, the same gap, never going down. Outside "within a few MB", but the proxy lags the downloader by what it has
buffered: not proven either way. To settle: the downloader's own `current_bytes` read as root (`adb root` at the start of the run).
The relaunch shot was taken with the notification shade open and is not a reading: still owed. Shots
`handoff/PLAN/app-integration/L4-android-shots-2026-11-20/T738/` (49.5 MB on run 1; 98.5 and 217.1 MB on run 2).

