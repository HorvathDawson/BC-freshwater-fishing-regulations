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

## Owner answers (2026-11-14, on the parity audit)
Verbatim: "we want it to progress with app closed in both app and android (First install: the ~480 MB download only
progresses with the app open and the screen on, and the app doesn't say so.)" / "need to just update text for ios (Wrong
promises on iOS: "about every 15 minutes" for notices and "download starts by itself when you connect" don't hold
there.)" / "everything else looks like a good find should look for more things like that".
- F1: the release download must continue with the app not in front, on iOS as on Android: a real iOS background
  download (replaces the foreground-only session of b8e33ca80). Known iOS limit, to be said in the text: a force-quit
  from the app switcher cancels background transfers; the next open resumes or restarts cleanly.
- F3 / F8: text only, iOS-truthful wording.
- The other findings (keyboard, modal back, iCloud backup, full-disk message, style-swap cover) are accepted as work;
  a second audit pass looks for more of the same kind.
- Code lane: branch `ios/t722-install` (kept off `redesign/stream-sections` until the simulator and the Android benches
  have proven it).
