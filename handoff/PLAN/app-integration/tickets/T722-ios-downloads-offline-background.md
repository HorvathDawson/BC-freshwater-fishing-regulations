# T722 — iOS: downloads, offline, background and notices (IOS-5)

Status: OPEN.

Source: owner 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is
rock solid too?" and "We want to do as much work as possible without the Apple account". Stub IOS-5 in
`handoff/PLAN/ios/START-HERE-ios.md` (approved to start there, "Unattended day"). Simulator only; no Apple account.

## Work
**IOS-5. Downloads, offline, background.** Goal: first run survives a kill, app-switch, Wi-Fi to cellular, low disk, and
a big update; background release check and notices behave on iOS. Acceptance: `first-open`, `offline`, `big-update`,
`bg-install` equivalents on iOS; notification permission and one delivered notice shown; the Always/background
declarations explained for review. Size: L (background behaviour needs a real device to be honest).
  Sub-steps: port `install.mjs`, `bg-install.mjs`, `big-update.mjs`, `offline.mjs`, `gauge-offline.mjs`; kill mid-download and
  relaunch; app to background mid-download; low-disk (simctl) message; notification permission prompt flow and one
  raised notice via the feeds test channel; read what expo-background-task allows on iOS and write it down; background-
  mode declarations explained for review. Mirrors: same-named Android scenarios.

## Acceptance
`first-open`, `offline`, `big-update`, `bg-install` equivalents on iOS; notification permission and one delivered notice shown; the Always/background declarations explained for review.

Depends: T718. Size: L. Owner: no (real-device proof waits for T88).

## Results
(filled in as the work runs)

## Audit (2026-11-14, read from the code)
Full table: `handoff/PLAN/ios/parity-audit.md` (nothing was run; no package source was installed, so package behaviour is "from memory").
- **F1 (core, likely broken):** the first install moves only with the app open and the screen on: foreground session (`install.native.ts:136`), no iOS background downloader (`bgDownload.ts`), no keep-awake or background time. Test first: `first-open`, home button at 20 %, wait, return.
- **F8 (wrong words):** "starts by itself when you connect" (`core/src/install.ts:123`) holds on iOS only while the app is open; the boot line says "Wi-Fi recommended" (`App.tsx:735`), not "keep the app open".
- **F3 (wrong words):** notification settings promise "about every 15 minutes at best" (`core/src/alerts.ts:333-334`); iOS BGTaskScheduler is the OS's call, may be rare or never, and does not run on its own in the simulator. Port `feeds` plus the package's test trigger.
- **F6:** the release (~480 MB) is in Documents with no iCloud-backup exclusion (`install.native.ts:24`); spots and photos share that folder and should stay backed up. Check with `xattr` after install.
- **F9:** no free-space check; a full disk shows "Check the connection" (`App.tsx:618`). Shared with Android.
- **F14:** iOS hashes the big files in JS (`install.native.ts:167`); count `[stall]` lines in the hash phase.
- Notices: permission asked only from a reader's switch (`notify.native.ts:112`), `trigger: null`, tap code is platform-free; cold-start tap needs one run. No iOS update notice until `EXPO_PUBLIC_APP_STORE_URL` is set (by design, answered: nothing shows, the too-old boot screen says so without a button).
- `deps.md:211-213` still claims a background URLSession on iOS: stale (F12).

## Code half (branch ios/t722-install, 2026-11-14)
Owner decisions mid-lane (2026-11-14): F1 "we want it to progress with app closed in both app and android" (replaces the
keep-awake brief: dropped, not built); F3/F8 "need to just update text for ios"; F6, F9 go ahead.

Audit check: every F1/F3/F6/F8/F9/F12/F14 claim held in the code. Android's "WorkManager" is a DownloadManager
(`modules/bg-download/android/.../BgDownloadModule.kt`): it does carry on with the app swiped away. Why b8e33ca80 dropped
expo's background session: its task lives in a JS `DownloadTask` promise (lost on a kill) and writes no `.part` until the
end (audit G1).

**F1 built and PROVEN on the simulator (2026-11-14 18:15, build c750d571d, see "Simulator proof" below).** The iOS half of `apps/mobile/modules/bg-download`
(`ios/BgDownloadModule.swift`, `ios/BgDownload.podspec`, `expo-module.config.json` "apple"): the Android module's JS
surface (`jobs/enqueue/remove/claim/sha256`), so the one JS state machine (`packages/data/src/release/bgDownload.ts`)
drives both; no JS logic forks. One background `URLSession`, fixed identifier `ca.canifishthis.bgdownload`, made at launch
by an AppDelegate subscriber, which also answers `handleEventsForBackgroundURLSession`; jobs recorded in
`Application Support/bg-download/jobs.json`; the finished file moved in `didFinishDownloadingTo`; an error with resume data
is resumed natively (up to 3 times: a dropped line, a lock, a swipe-away answered at the next launch), so a drop does not
restart from zero (G1) and "What has downloaded is kept" holds while resume data exists (the server must keep ETag/Range;
R2 does). A record with no task and no word for 10 s is "failed" (-1) and JS queues it again. Chose this over 32 MB range
chunks (G1's proposal) because it gives both background progress and resume in one mechanism, the same as Android's.
**G4:** a Wi-Fi-only job sets `allowsCellularAccess/allowsExpensiveNetworkAccess/allowsConstrainedNetworkAccess = false`
(hotspot and Low Data Mode refused). Not fixed: a START on a hotspot is still seen by NetInfo as Wi-Fi and queued as
allowed (the shared metered detection; changing it moves Android).
**F14 fixed by the same module:** iOS now hashes natively (CryptoKit, `sha256`). To measure what it saved: a `--probes`
build, first open, count `[stall]` lines between "[setup] downloaded" and "[setup] installed" (before: JS hash).
**F6 built.** `BootOptions.noBackup` (iOS module's `excludeFromBackup`; null on Android/web): `release/` and the rule book
after each install and at every boot; `spots/`, `settings/`, `feeds/` stay outside (pinned by path from the drivers in
`tools/release-boot.test.ts`). The module's own `bg-download/` folder is excluded too.
**F9 built (both platforms).** `checkRoom` (`release/boot.ts`) before the first install, the T403 reinstall and every
update: manifest sizes less what is on disk (`.part`, file) or in the system download, against `Paths.availableDiskSpace`.
Too little: `SpaceError` with `spaceLine` ("Not enough free space: the download needs about N MB and this phone has M MB
free. Free up some space and try again.") on the boot screen (`said`) or as the update's error; reported. Unknown free
space: reported, download goes on.
**F3/F8 text only.** `NOTIFY_WORDS.introIos` (G3: BGProcessingTask, idle, often overnight), `MOBILE_DATA_WORDS.waitingIos`,
`closeOkIos` (swipe-away stops it until the app is opened); `Platform.select` in `NotificationsPage.tsx` and `App.tsx`
(no new INLINE_ALLOWED entry). **F12:** `deps.md` and START-HERE corrected.
**G13 not done:** a dropped iOS download reaches `reportFailure` through the boot failure unclassified; classify it in the
report path (network condition) as a follow-up.

**Simulator proof (iPhone 17 Pro, build c750d571d, --testbed, fresh install, staging release r-f749475d0962, 479.3 MB).**
The cause of the refusal below was the build, not the simulator: `build-ios-sim.sh` passed `CODE_SIGNING_ALLOWED=NO`, so
Xcode never linked the simulated entitlements (`__TEXT,__entitlements` / `__ents_der`, `application-identifier`
`FAKETEAMID.ca.canifishthis.app`) that nsurlsessiond reads the app's identity from. The script now signs "to run locally"
(`CODE_SIGN_IDENTITY=- CODE_SIGNING_ALLOWED=YES CODE_SIGNING_REQUIRED=NO DEVELOPMENT_TEAM=`, no account, no keychain) and
fails when the section is missing (commit dc77c8914). **This changes EVERY iOS simulator build: the test-bed lane needs a
rebuild.** With it, one run:
- foreground: download started at once (no "rejecting connection" in 11 MB of log);
- Settings in front for 60 s: nsurlsessiond's temp files 15.0 → 167.5 MB (152 MB with the app not in front);
- `simctl terminate` mid-download: 163.2 → 179.3 MB in the 20 s with the app gone; relaunch: the same job ids [1, 2, 3, 4]
  (no new job, no double download), the bar moving again ("27.3 of 479.3 MB downloaded", screenshot);
- installed at 18:22 (6 min from first open, `Documents/release/current` → r-f749475d0962), the map drawn;
- `xattr`: `com_apple_backup_excludeItem` on `Documents/release`, the rule book `synopsis-80614b20045f74c9.pdf` and
  `Application Support/bg-download`; none on `Documents/spots`;
- 0 "Cannot convert BigInt" lines (T718 saw 4,999 on the old path; that path is fixed too, `Number(...)`, and is now only a
  fallback: a client without the module, an older app's `.part`). The module's own numbers reach JS as doubles.
**Bar after a relaunch, second look (build 347281f96, 20:19).** The module now keeps each job's bytes from its progress
(on disk every 4 MB) and reports them for a re-attached task at once (JS seam test in `release/bgDownload.test.ts`).
Run: terminated at 104.3 MB on disk, relaunched; at +4 s the bar said "38.8 of 479.3 MB" with 116.3 MB on disk; installed
at 20:22. So the low bar is NOT (only) the re-attach: the system downloads all four big files AT ONCE, while the bar
(`install` progress) counts finished files plus only the file being filled, one at a time. On disk the bytes are spread
over the four. Same on Android (DownloadManager also runs them together). Fix (not made: it moves Android's bar): the
first install's progress sums every queued job's bytes (`jobs()`) while the big files download. Owner/main-lane call.
Earlier note: right after a relaunch the bar restarted low (27 MB while ~180 MB were down) because a re-attached task
reports bytes only after its next progress tick; it caught up as the download went on. The iOS line under the bar was two
lines edge to edge: now centred with side padding (no visible change for Android's one-line text).
Not shown by the simulator: real suspension when the phone locks, cellular / hotspot / Low Data Mode, swipe-away in the
app switcher (resume data) — those need a device (T88).

**Earlier result (build 1f7c2ab3f, before the signing fix), kept for the record.** Swift compiles and links. But every background task
failed at once with NSURLError -1: nsurlsessiond logged "Process with pid … does not have a bundle ID, rejecting
connection" (also with the app ad-hoc re-signed with its bundle id; an `application-identifier` entitlement made
SpringBoard refuse the launch). JS then re-queued jobs in a loop (ids 1 → 81 in 25 min) and the first install never
finished. This is very likely why expo's background session also "stopped with no .part" in T313. **So on the simulator
build as made by `build-ios-sim.sh` (CODE_SIGNING_ALLOWED=NO) the branch's first install fails; on a signed build it is
expected to work but is unproven.** Proven on the simulator: the module loads, the session is made at launch, records
persist, `isExcludedFromBackup` lands (`xattr`: `com.apple.metadata:com_apple_backup_excludeItem` on `bg-download/`).
Steps 1–4 below now pass with the signing fix; step 5 still needs a device. The steps, for re-running:
1. Xcode-signed simulator run (Sign to Run Locally, a personal team) or a TestFlight build (T88): fresh install, first open;
   in `log stream --predicate 'process == "nsurlsessiond"'` no "rejecting connection"; `CFNetworkDownload_*.tmp` under
   the sim's `data/Library/Caches/com.apple.nsurlsessiond` grows.
2. Home (`simctl launch … com.apple.Preferences`) for 60 s: the tmp keeps growing (background progress).
3. `simctl terminate` mid-download, relaunch: the same job ids in `Application Support/bg-download/jobs.json`, no new
   ones (no double download), the bar continues; the install finishes (`Documents/release/current`).
4. `xattr -l Documents/release` shows `com_apple_backup_excludeItem`; `Documents/spots` shows none.
5. Device only: lock the phone (real suspension), cellular with Wi-Fi only, swipe-away then reopen (resume data) — the
   simulator cannot show these.
Proof script used: a `session-ios.sh` command that does 1–4 (kept in the lane's scratchpad; easy to re-create from the
steps above).
