# iOS parity audit (static, read from the code), 2026-11-14, app 0.7.9 at HEAD

Read-only lane. Nothing was built or run. Every line number is at HEAD on `redesign/stream-sections`. Paths are under
`app/` unless they start with `handoff/` or `deploy/`. Companion pages: `handoff/PLAN/ios/START-HERE-ios.md` (the work
list), tickets T722 (IOS-5) and T723 (IOS-6), which each carry a short "Audit" section pointing here.

Verdict words: **same** (works the same), **by design** (differs on purpose), **likely broken**, **missing**, **needs run**
(cannot tell from code).

## What I could NOT determine (read this first)

- **No package source is installed here.** `app/node_modules/.pnpm` holds only `lock.yaml`, so I could not read the
  packages' own docs (expo-background-task, expo-file-system `DownloadTask`, expo-notifications, expo-image-picker). Where a
  verdict leans on what a package does on iOS, it says "from memory" and needs one simulator look.
- **The generated `app/apps/mobile/ios/` folder is stale** (an early prebuild: only five Expo pods in `Pods/`, an Info.plist
  that still carries Always-location and Motion strings and no background modes, empty entitlements). It is git-ignored and
  proves nothing about HEAD. Its `IPHONEOS_DEPLOYMENT_TARGET = 16.4` (project.pbxproj:388) is the only minimum-iOS number on
  disk; re-read it after a fresh prebuild (IOS-1e).
- **Obj-C tap code**: the iOS binding source is not on disk either. That iOS waits for a double tap before the click comes
  from the T719 bench's own reading of `MLRNMapView.m` (`scenarios-ios/regs-open.mjs:12-17`), not from me.
- Nothing about frame times, memory, real-phone gestures, background task delivery, camera (the simulator has none), HEIC
  handling, or how long iOS keeps a foreground download alive can be settled by reading.

## 1. The handoff table, row by row (START-HERE-ios.md "What is Android-only or different on iOS")

| Row | Status at HEAD | Note |
|---|---|---|
| MapLibre binding patch | **still true, one addition** | The seven Obj-C files are in the patch (lines 1237-1910). No Obj-C hunk changed since the last iOS run (b99268518, 11-06); only Android and the TS/codegen specs did (see section 6). So the iOS half is the same, but the codegen specs it compiles against grew three props/events. |
| T716 `onTapUp` | **still true** | `Map.native.tsx:922` passes `onTapUp`; nothing emits it on iOS. The click path is a full fallback (`pressCue.ts:135-151`: `click` confirms the cue and opens). Cue arrives with the click. |
| Back | **still true, one gap added** | Edge swipe is `EdgeBack` around the Shell only (`Shell.tsx:735`, `useShellNav.ts:53`). Modal pages have none (finding F7). |
| Notifications | **still true, wording wrong on iOS** | `notify.native.ts:55` early return, `:71` `trigger: null`. The settings intro says "about every 15 minutes at best. Android may check less often" (`core/src/alerts.ts:333-334`); iOS has no such promise (F8). |
| Release download | **still true** | Foreground session, `install.native.ts:136`. `deps.md:211-213` still says iOS runs on a background URLSession: that line is stale. |
| Crash records | **still true** | `exitInfo.ts:1-8`, `modules/exit-info/expo-module.config.json` has `apple` with the Swift module and an AppDelegate subscriber. |
| Share | **still true** | `share.native.ts:16`. |
| Shared links | **WRONG now** | `app.json:37` has `associatedDomains` for both hosts and the site build writes the AASA file when a Team ID is set (T724, commit 9af44b246 and the one before). Only the Team ID and a signed build are missing. `appLink.native.ts` is platform-free, as the row said. |
| App update notice | **still true; answer: nothing shows** | `appUpdateChannel` returns `none` for iOS without `EXPO_PUBLIC_APP_STORE_URL` (`core/src/appUpdate.ts:148`); nothing in `deploy/` or `app.json` sets it. `makeAppUpdater` returns null (`appUpdater.native.ts:34-35`), `notifications.appVersion` is false (`notify.native.ts:109`), so the bell has no update row and the "New app version" switch is not offered. A too-old app on first start shows the failure text with no button (`App.tsx:611-619`). Said, not silent; acceptable until T88. |
| PDF viewer | **still true** | `PdfViewer.native.tsx:33` is a `Modal` (see F7 for back). |
| Permissions | **still true** | `app.json:11-28`. |
| Safe areas | **still true, one risk added** | `WindowInset.native.tsx:11` iOS branch. `Sheet.tsx:262` `BottomInset` uses the native `SafeAreaView` inside a `Modal`, the class of bug the iOS branch was written for (F13). |
| Gauge pill | **still true** | Patch comment at line 1257 ("text sits a little further in"). |
| Fonts, sprites, data | **cannot tell** | No iOS-specific code found; not verified. |
| Rotation | **CHANGED** | `app.json:7` is `"orientation": "portrait"` (owner answer). The stale `ios/Info.plist` still lists upside-down portrait, so check the Info.plist after a fresh prebuild (IOS-9). |
| iPad | **still true** | `app.json:38` `supportsTablet: false`. |
| Memory trim | **the handoff's own "out of date" note is right** | `memoryTrim.native.ts:30` listens to `memoryWarning` on iOS. |
| `Platform.OS` list | **still true** | Re-listed in section 2. |
| Test bed on iOS | **CHANGED** | `run-ios.mjs` now has `regs-open` and `tap-stalls` by name (T719/T720, `scenarios-ios/`). Still no iOS twin of: `saved-spots`, `spots`, `gestures`/`glide`/`feel`, `pane`, `part-steps`, `regs-switch`, `route-scroll`, `photo-zoom`, `report`, `big-update`, `bg-install`, `install`, `feeds`, `locate`, `gauge-offline`, `edge`, `back-flash`. |

## 2. Android-only APIs and `Platform.OS` branches

Searched `PermissionsAndroid`, `BackHandler`, `ToastAndroid`, intent launchers, `startActivity`, `android_*` props,
`elevation`, `Platform.OS` across `apps/mobile/src` and `packages/*/src` (tests excluded).

| Where | What iOS gets | Verdict |
|---|---|---|
| `systemBack.native.ts:6-13` `BackHandler` | Never fires on iOS (no hardware back). The edge swipe is the only back. | by design |
| `Sheet.tsx:177,210`, `MenuPage.tsx:31`, `PdfViewer.native.tsx:33`, `PhotoViewer.tsx:107` `onRequestClose` | React Native calls it on Android only; iOS closes through the page's own control (‹, Close, ×, backdrop, drag). | by design, but see F7 |
| `notify.native.ts:55,71` channels / `{channelId}` | iOS skips channels, uses `trigger: null`. | by design |
| `notify.native.ts:119` `registerTaskAsync(TASK, {minimumInterval: 15})` | iOS: BGTaskScheduler, the OS decides (see section 3). | by design, wording wrong (F8) |
| `appUpdater.native.ts` Play branch | Never reached on iOS (channel is `app-store` or `none`). `PLAY_PACKAGE` unused. | same |
| `emulator.native.ts:12` | Returns false on iOS. A simulator is tagged by the build script. | by design |
| `memoryTrim.native.ts:30` | `AppState` `memoryWarning`. The `MemoryTrim` native module is Android-only (`modules/memory-trim`: `"platforms": ["android"]`); `requireOptionalNativeModule` returns null on iOS and the code goes on to the `AppState` branch. | same, by design |
| `bgDownload.ts` (apps) `requireOptionalNativeModule("BgDownload")` | null on iOS; every caller handles null (`App.tsx:269-272`, `:298`, `:735`; `install.native.ts:165-177`). | by design, but see F1 |
| `share.native.ts:16` | `{url, title}` on iOS; `dialogTitle` is Android-only and ignored. | same |
| `openBook.ts:21` `bookUrl(pdf, os)` | iOS gets the plain PDF URL (no `#page`), only used when the release has no copy. | by design |
| `WindowInset.native.tsx:11` | iOS branch (`IosInsets`). | by design |
| `platformLine.native.ts:5` | "ios 26.5" in report headers and request headers. | same |
| `App.tsx:449` `Share.share({message})` for "copy" in the updates log | iOS opens the share sheet where Android does too (there is no native clipboard path). Returns true even when the sheet is dismissed. | cosmetic (F16) |
| `index.js` / `App.tsx` `StatusBar barStyle` only | No Android-only `backgroundColor`/`translucent`. | same |

No `PermissionsAndroid`, `ToastAndroid`, intent launcher, `android_ripple` or `statusBarTranslucent` use remains in app code
(`expo-intent-launcher` was removed with T354). Nothing Android-only is called without a platform guard.

## 3. Download, update, background task, notifications on iOS

- **Download.** `install.native.ts:119-158` `nativeFill`: `DownloadTask` with `sessionType: "foreground"` (`:136`), Range resume
  from the `.part` (`:126-146`), 4 tries with 1.5/3/6 s backoff, then it throws and the boot screen retries twice
  automatically (`App.tsx:529-535`) and then shows "Check the connection and try again. What has downloaded is kept"
  (`core/src/install.ts:11`, `App.tsx:618`). Kill mid-download: the `.part` stays and the next launch resumes
  (`install.native.ts:126`), as on Android's old path. Failure is shown and reported (`boot.ts:326,394`).
- **App to background / screen lock.** A foreground session stops being served when iOS suspends the app. Nothing here
  asks iOS for extra time: no `beginBackgroundTask`, no keep-awake (`expo-keep-awake` is not a dependency, `package.json`),
  and the iOS boot screen shows "Wi-Fi recommended" (`App.tsx:735`), not "you can close the app". From memory: the screen
  auto-locks in 30 s to 5 min, which suspends the app and the download with it. Expected: no progress while away, and on
  return the dead task throws, `nativeFill` retries from the `.part`. Needs a run (F1).
- **Waiting for Wi-Fi** (`MOBILE_DATA_WORDS.waiting`, `core/src/install.ts`: "the download starts by itself when you
  connect") is true on iOS only while the app is open. The wait is a JS listener (`App.tsx:279-290`).
- **Low disk.** No free-space check exists on either platform (grep of `release/` and `core/src/install.ts` finds none).
  A write failure lands in the same "Check the connection" text (`App.tsx:618`): wrong cause named, shared with Android.
- **Integrity cost.** On iOS the big files are hashed in JS (`install.native.ts:167`: `bg.sha256` only when the Android
  module is there; the module notes ~23 s of JS for the basemap versus ~1 s native). The follower hash (T272) hides part of
  it. Needs a stall measurement (`tap-stalls` is ported, but the first-install phase is not).
- **Backup.** The release (~480 MB) is written under `Paths.document` (`install.native.ts:24`) and nothing marks it
  excluded from iCloud backup (no `isExcludedFromBackup`/equivalent anywhere under `app/`). iOS backs up Documents. Likely
  an App Store review point (guideline 2.23) and an iCloud quota cost. Saved spots and photos live in the same place
  (`spots/store.native.ts:25`, `photoStore.native.ts:15`) and should stay backed up. Fix would split them (F6).
- **In-app update notice.** See table row above: nothing shows on iOS until `EXPO_PUBLIC_APP_STORE_URL` is set; the
  `expo-in-app-updates` import is lazy and never loaded.
- **expo-background-task on iOS** (from memory; confirm with the package's README once installed): runs through
  BGTaskScheduler, the minimum interval is a request not a promise, the OS may run it rarely or never (low power mode, an
  app the person has not opened lately, a force-quit app), runs are short, and the Simulator does not run them on its own
  (the package ships a test trigger for that). `notify.native.ts:119` registers only when the reader turns something on.
  The product promise "about every 15 minutes" must be reworded for iOS (F8). `notify.native.ts:75-97` is platform-free and
  returns `Success`/`Failed`.
- **Notifications.** Permission is asked only from a reader's switch (`notify.native.ts:112-115`), never at launch: good.
  `raise` with `trigger: null` shows at once; the foreground handler (`:100-102`) shows a banner while the app is open.
  Tap handling (`:129-141`) reads the cold-start response and live responses; same code both platforms. Denied: the page
  says so and `Linking.openSettings()` (`:116`) works on iOS. `ensureChannels` is skipped. Needs a run: the cold-start tap.
  From memory the expo-notifications config plugin also adds the `aps-environment` (push) entitlement; the app sends no
  push, so check the App ID capability when T88 starts.

## 4. Features added since 2026-11-05 (git log of `app/`, 50 commits), grouped

| Feature | Android-shaped code? | iOS verdict |
|---|---|---|
| Saved spots, add by "At my location" or pick | `locate.native.ts` is expo-location, foreground, no Android API | same |
| Spot photos: library and camera | `pickPhoto.native.ts:16` always offers camera; `:24-29` asks permission then `launchCameraAsync`. The simulator has no camera, so the real failure text will show (reported). `photoStore.native.ts:23` copies the picker file into Documents; refs are relative (`photos/<name>`), so an iOS container move does not break them. HEIC: picker recompresses with `quality: 0.8` (`:21`); output type unconfirmed. | same, needs run (camera and HEIC) |
| Photo zoom (`PhotoViewer.tsx`) | RNGH pinch inside its own `GestureHandlerRootView` in a `Modal` (`:107-108`); `photoInput.native.ts` no wheel | same, needs run |
| Share link | `share.native.ts:16`; slug read over the network (`:11`) | same |
| Shared links | `appLink.native.ts` uses `Linking`; iOS entitlement now in `app.json:37` | same, proof waits for T88 |
| Search "At my location" row | **Not a search row anywhere.** It is the Spots tab's "add" row (`SpotsScreen.tsx:64-86`, `BrowseHost.tsx:165-169`, `core/src/mapRail.ts:41`). The iOS `location` scenario looks for it in Search (`run-ios.mjs:297-304`), finds none and skips; T313's table says the same. | **test-bed expectation wrong, not an iOS gap** (F11) |
| Salmon colouring, run timing tab | Colouring is `colouring.ts` (platform-free) over the binding's `MLRNTileIds` event, which has an iOS half (patch line 1296). Run timing store is `Paths.document/feeds` (`runTimingStore.native.ts:6`). | same, needs run |
| Style swap on tab change (T451, T566) | Kotlin keeps the old picture over a swap (patch lines 436-460). iOS has no such cover, and the grey-streams cause is in the shared MapLibre core. | likely visible defect (F5) |
| Tap cue (T716) | See F4 | differs, degraded |
| Part steps, parts sheet removal (T698) | JS only | same |
| Problem report | `ReportPage.tsx:55` `mailto:` through `Linking.openURL` (iOS Simulator has no Mail account: the open may be refused and is reported by `openLink`); `:58` Share sheet fallback | same, needs run |
| PDF rule book | `PdfViewer.native.tsx` is a `Modal` | same, F7 |
| In-season notice inbox in the bell (T700) | JS only | same |
| In-app update (T390) | none on iOS | by design |
| Settings screen (T703) | JS only; `SettingsPage.tsx:55` web-only branch | same |

## 5. Native modules (`apps/mobile/modules/`)

| Module | iOS half? | What JS does without it |
|---|---|---|
| `bg-download` | **none** (`"platforms": ["android"]`) | `backgroundDownloads` is null (`bgDownload.ts`); the install downloads in the app; boot screen says "Wi-Fi recommended". Handled, but see F1. |
| `memory-trim` | **none** (android only) | `requireOptionalNativeModule` null; iOS uses `AppState` `memoryWarning` (`memoryTrim.native.ts:30`). Handled. |
| `exit-info` | **yes** (`ios/ExitInfoModule.swift`, podspec, AppDelegate subscriber) | n/a. Jetsam and watchdog kills are not recorded (known, MetricKit is the follow-up). |

## 6. MapLibre binding patch (`patches/@maplibre__maplibre-react-native@11.5.0.patch`)

| Change | Android | iOS counterpart | What JS depends on it |
|---|---|---|---|
| Stretch images (`stretchX/Y`, `content`) | Kotlin, `MLRNImagesManager.kt`, `ImageEntry.kt` | `MLRNImageLoader.m:stretchable` (one zone per axis; `content` = stretch box) | gauge pill look (small padding difference, by design) |
| `setFeatureStates`, `watchTiles`, `MLRNTileIds` | `MLRNVectorSource*.kt`, `TileIds.kt` | `MLRNVectorSource.m`, `MLRNPmTilesIds.{h,m}`, `MLRNVectorSourceModule.mm`, `MLRNMapView.m` (tile delegate) | colouring engine (Conditions, salmon, runs); the one path that must match |
| T420 once-per-watch dedupe | yes | yes (`MLRNPmTilesIds.m`, same commit) | fewer events |
| **`onTapUp`** (T716) | `MLRNMapView.kt` | **none** | `Map.native.tsx:742-770`, `pressCue.ts` `lift`; iOS falls back to the click |
| **`maxPixelRatio`** (T448) | `MLRNMapView.kt` | none (prop declared in the spec, ignored) | `Map.native.tsx:912` passes it; harmless |
| **`prefetchZoomDelta`** (T401) | `MLRNMapView.kt` | none (ignored) | `Map.native.tsx:914`; harmless |
| **Old picture held over a style swap** (T451) | `MLRNMapView.kt` (PixelCopy cover, patch 436-460) | **none** | `lookKey`/`onLookDrawn` (T566) are written to lift "at the full frame the held picture lifts at" (`Map.native.tsx:253-260`); on iOS they work off the same full-frame event but there is no cover |
| **Own fling glide, fling distance** (T312, T452), **pinch carry** (T707), camera probe | `MLRNMapView.kt`, `MLRNCameraCache.java` | none: iOS uses MapLibre's own inertia | feel only; no JS dependency. Owner's "flick/zoom clunky" fixes are Android-only |
| Spec changes since 11-06 | n/a | **codegen specs changed** (`MapViewNativeComponent.ts`: three new members) | the iOS app compiles against the new spec; first iOS build after the 11-06 run exercises it |

The patch's `Images.tsx`/`VectorSource.tsx` JS hunks are shared and used by both.

## 7. Safe areas, status bar, keyboard

- Root: `SafeAreaProvider` + `SafeAreaView` (`App.tsx:598-603`), same both platforms. `StatusBar barStyle` only (`:604`).
- Full-screen `Modal` pages and the PDF viewer use `WindowInsets` (`MenuPage.tsx:33`, `PdfViewer.native.tsx:35`), with the iOS
  branch from T313. Bottom sheets use `BottomInset` (`Sheet.tsx:262`), which is the native `SafeAreaView` (F13).
- **No keyboard handling at all**: no `KeyboardAvoidingView`, no `automaticallyAdjustKeyboardInsets` anywhere under
  `packages/ui-native/src` (grep). Android resizes the window for the keyboard; iOS does not (F2).

## 8. Findings, sorted by severity

Severity: **core** (breaks a core flow), **visible** (a defect the person would notice), **cosmetic**, **none**.

| # | Sev | Area | Finding | Evidence | Verdict | Prove it on the simulator (one line) | Proposed fix and file (shared with Android?) |
|---|---|---|---|---|---|---|---|
| F1 | core | First install | The 480 MB first install makes progress only while the app is in the foreground and the screen on. There is no iOS background downloader, no extra background time and no keep-awake; the boot screen does not say so ("Wi-Fi recommended"), and the Wi-Fi wait text promises it "starts by itself". | `install.native.ts:136,165-177`; `bgDownload.ts:1-27`; `App.tsx:735`; `core/src/install.ts:123-124`; no keep-awake in `package.json` | likely broken (from memory of iOS suspension; needs run) | Start `first-open` fresh, press the home button at 20 %, wait 2 min, return: does the bar move, does it finish, is a failure shown? Repeat with `simctl` lock | Short term: keep the screen awake during the first install (JS only; needs a `deps.md` line) and say "keep the app open" on iOS (iOS-only words file: `core/src/install.ts`, shared words file, choose by `Platform.OS` in `App.tsx:735`). Better: restore expo's background session (`sessionType` default) for the big files with the `.part` resume, if the kill-loses-JS-side problem (b8e33ca80) can be handled by re-adopting the task on launch. Files: `install.native.ts` (shared with Android only through the `device()` factory, Android ignores the option) |
| F2 | visible | Typing | On iOS the keyboard covers inputs in bottom-anchored sheets and long pages. Android resizes. Affected: spot title and notes, the alert value and percent fields (in a `Sheet` pinned to the bottom), the problem report, the notifications search, glossary search. | none of `KeyboardAvoidingView`/`automaticallyAdjustKeyboardInsets` in `packages/ui-native/src`; inputs at `SpotScreen.tsx:180,252`, `AlertMe.tsx:233,257`, `ReportPage.tsx:89,98`, `NotificationsPage.tsx:108`, `GlossaryPage.tsx:97`; sheet pinned bottom `Sheet.tsx:213` | likely broken | Shared Maestro flows `spot-notes.yaml`, `report-open.yaml` with the software keyboard on (Simulator > I/O > Keyboard > Toggle Software Keyboard); screenshot with the keyboard up | Add `automaticallyAdjustKeyboardInsets` to the ScrollViews (iOS-only prop, a no-op on Android) in `Sheet.tsx:317,333`, `MenuPage.tsx`, `SpotScreen.tsx`; for the pinned-bottom sheet a `KeyboardAvoidingView behavior="padding"` gated to iOS. All shared files, change is iOS-gated |
| F3 | visible | Notifications promise | Settings intro says checks run "about every 15 minutes at best. Android may check less often". On iOS the OS decides, may run it rarely or never, and not at all in the Simulator. A gauge alert the person relies on can be late or absent, and the page does not say so. | `core/src/alerts.ts:333-334`; `notify.native.ts:119` | likely wrong words | Turn an alert on, background the app, trigger the task with the package's test trigger (IOS-5), see when a notice lands; read the page text on iOS | An iOS line in `NOTIFY_WORDS` chosen by platform (`core/src/alerts.ts`, shared words; the platform arrives through the `NotifyControls` object from `notify.native.ts`, which is already per platform) |
| F4 | visible | Tap cue | `onTapUp` never fires, so the cue and the page open wait for the click. iOS probably waits for MapLibre's double-tap recogniser to give up (~300 ms), as Android did before T716 (1.6 s to 0.5 s on the Fraser was the lift change). Nothing is wrong or silent: the click path runs the same open. | `Map.native.tsx:694,742-770,921-922`; `pressCue.ts:135-151`; `scenarios-ios/regs-open.mjs:12-17` | differs, degraded (delay size unknown) | `run-ios.mjs regs-open` (T719/T720 already built): numbers beside Android's | Per the handoff default: measure, then an iOS `onTapUp` only if lean and the delay matches. Would sit in the patch (Obj-C, iOS-only file `MLRNMapView.m`) with no JS change (same event name) |
| F5 | visible | Style swap | Android hides the grey streams that MapLibre draws for 1-3 frames after a tab switch (Conditions/Runs to Map) with a held picture. iOS has no cover and the cause is in the shared MapLibre core, so the grey flash is likely there. | patch 436-460 (Kotlin only); `Map.native.tsx:253-260,275` | likely visible (needs run) | `switch`-style bench: Conditions tab to Map, film the transition, step the frames (`switch-frames.py` exists) | An iOS cover in the patch (UIView snapshot, `MLRNMapView.m`), iOS-only; or accept. Not worth a patch line until seen |
| F6 | visible | Storage | The release (480 MB) sits in Documents with nothing marking it out of iCloud backup. Spots and photos in the same folder should stay backed up. | `install.native.ts:24`; `spots/store.native.ts:25`; no exclusion in `app/` | likely problem (review and quota; needs a signed run to see backup size) | `xcrun simctl get_app_container booted ca.canifishthis.app data` then check the `release` dir flag with `xattr` after install (`com.apple.metadata:com_apple_backup_excludeItem` expected, absent today) | Set the exclusion flag on `release/` after create: a small iOS-only step (expo-file-system has no API for it in my memory; needs a tiny Swift function in the existing `exit-info` iOS module or a new one, a `deps.md` line). Files: new iOS-only module code, `install.native.ts` call behind `requireOptionalNativeModule` (null on Android) |
| F7 | visible | Back by swipe | Menu pages, the PDF viewer, sheets and the photo viewer are `Modal`s, which are separate windows; `EdgeBack` wraps the Shell only, and `onRequestClose` is Android-only. So on iOS a left-edge swipe does nothing over any of them; the only way back is the on-screen control. | `Shell.tsx:735`; `MenuPage.tsx:31`; `PdfViewer.native.tsx:33`; `Sheet.tsx:177,210`; `Modal` iOS behaviour from memory | missing on iOS (the handoff's IOS-4 walk will show it) | `edge-back.yaml` from inside More > Settings and from the PDF: expect no back | Wrap Modal content in an iOS-only `EdgeBack` (`MenuPage.tsx`, `PdfViewer.native.tsx`; both shared files, the wrapper is already a no-op where `drag.available` is false), or use `presentationStyle` page sheet which iOS dismisses by swipe down |
| F8 | visible | Waiting words | "Waiting for Wi-Fi: the download starts by itself when you connect" is true on iOS only while the app is open (JS listener, no system downloader). | `App.tsx:286-296,717-722`; `core/src/install.ts` `MOBILE_DATA_WORDS.waiting` | wrong on iOS | Metered simulation (Network Link Conditioner) with the setting on Wi-Fi only; background the app | Same as F1's wording fix |
| F9 | visible | Low disk | No free-space check; a full disk shows "Check the connection and try again". Shared with Android, but iOS sims and phones with 480 MB more are likelier to hit it on small phones. | `core/src/install.ts:11`; `App.tsx:618`; grep finds no check | missing (shared) | Fill the simulator's disk (`mkfile`) before first open | Check free bytes before queueing and say "needs about N MB free". Shared (`packages/data/src/release/boot.ts`), needs a way to read free space on both platforms |
| F10 | visible | Camera on simulator | `PHOTO_SOURCES` always lists the camera; the simulator has none, so choosing it shows the failure text (and a report). On a real phone it asks permission then opens. | `pickPhoto.native.ts:16,24-29` | same; test-only noise | `spot-add-photo.yaml` uses the library: fine. Do not run the camera path on the simulator | none; tell the lane so the camera failure is not ticketed as a bug |
| F11 | cosmetic | Test bed | The iOS `location` scenario skips its last step ("At my location from search") because that row is not in search. It is in the Spots tab. A skip that hides coverage, not an app gap. | `run-ios.mjs:297-304`; `SpotsScreen.tsx:64-86`; T313 results table | test-bed wrong | n/a | Point the step at Spots > "At my location" (file `run-ios.mjs`, iOS-only) and fix the handoff line |
| F12 | cosmetic | Docs | `deps.md:211-213` says iOS downloads on a background URLSession and "the next open resumes from the .part"; the code forces a foreground session (b8e33ca80). START-HERE row on shared links is out of date. | `deps.md:211-213`; `install.native.ts:133-137` | stale doc | n/a | Correct both lines (`app/deps.md`, `handoff/PLAN/ios/START-HERE-ios.md`) |
| F13 | cosmetic | Sheet foot inset | `BottomInset` is a native `SafeAreaView` inside a `Modal`; on iOS this was the measure-mid-slide bug for menu pages. Sheets use `animationType="none"` so it may be fine. | `Sheet.tsx:210,262`; `WindowInset.native.tsx:6-8` | needs run | Shots of a sheet on iPhone 17 and 17e: foot clear of the home indicator | Use the `IosInsets` path in `BottomInset` too (`WindowInset.native.tsx`, shared file, iOS-gated) |
| F14 | cosmetic | JS hashing | iOS hashes the big files in JS (no native sha256). Stall risk during first install, largest on the basemap. | `install.native.ts:167` | needs run | `install` with `--probes`: count `[stall]` lines during the hash phase | A Swift `sha256` in an iOS module, or CryptoKit via the existing `exit-info` iOS module (new function, iOS-only) |
| F15 | cosmetic | Pill padding | Known: gauge pill text sits a little further in. | patch 1257-1281 | by design | iPhone shot of a gauge pill | none |
| F16 | cosmetic | "Copy" | The updates log "copy" opens the share sheet, and reports success even if the sheet is dismissed. Shared with Android. | `App.tsx:447-452` | cosmetic | open Settings > updates log > Copy | use the result of `Share.share` (`App.tsx`, shared) |
| F17 | cosmetic | Camera/photos strings | Permission strings say "Can I Fish This?"; the home-screen name is "Can I Fish?". Name decision already settled (both stores "Can I Fish This?"). | `app.json:3,14,15,22` | pending the name apply (IOS-10) | read the permission prompts | `app.json` `expo.name` (shared) |
| F18 | none | Orientation | `orientation: "portrait"`. The stale `ios/Info.plist` lists upside-down too; a fresh prebuild decides. | `app.json:7`; stale Info.plist line 74-77 | needs run | open the built `Info.plist` after `expo prebuild` | none yet |
| F19 | none | Map patch drift | No Obj-C change since the last iOS run; spec changed (onTapUp, maxPixelRatio, prefetchZoomDelta). | patch diff vs b99268518 | needs run (a first build) | IOS-1 build | none |
| F20 | none | Shared links | `associatedDomains` present; AASA only with a Team ID. Until T88 a link opens the web page. | `app.json:37`; T724 results | by design | `simctl openurl` to a water link: Safari opens the web page | wait for T88 |
| F21 | none | In-app update | No notice, no switch on iOS until `EXPO_PUBLIC_APP_STORE_URL`. | see section 1 | by design | read the bell and Settings | set the URL when the listing exists (T88) |

## 9. Likely fine, still needs one simulator look

- Search results and the part steps (JS only).
- Run timing slider and tab (JS only; check the 300 ms stall bar once, `tap-stalls` is ported).
- Salmon colouring on the Map tab (event path has an iOS half; look at one tile load).
- Saved spots: add by tap and by "At my location", rename, delete, restart (relative photo refs, Documents).
- Photo from the library through `simctl addmedia` (HEIC file on purpose) and the photo zoom pinch.
- Share a water's link; the sheet shows the url.
- Notification permission prompt text and the denied page (`openSettings`).
- Satellite, location dot, PDF opens on the marked page in PDFKit and closes (`pdf` scenario).
- In-season inbox in the bell; Settings screen; "New/Updated" marks.
- Problem report: the mail draft path (the Simulator has no Mail account) and the share fallback.
- Dark and light, largest text (IOS-9).
- Crash report on next start (`crash` scenario) after the Swift module.
- Cold-start notification tap opening the water (the one `openRequest` path).

# Pass 2 (2026-11-14)

Read-only, same standard as pass 1: nothing built or run. Paths are under `app/` unless they start with `handoff/`.
**Correction to pass 1's first caveat:** the package sources ARE installed, hoisted at `app/node_modules/<package>`
(expo-file-system 58.0.11, expo-background-task, expo-notifications, expo-location 58.0.19, expo-image-picker 58.0.10,
expo-sqlite 58.0.14, react-native 0.88.0, @react-native-community/netinfo). Every package claim below is from its own iOS
source, with the file named. G1 also corrects pass 1's F1 and F14 (the `.part` resume does not happen on iOS).

Severity words as pass 1: **core** (breaks a core flow), **visible**, **cosmetic**, **review** (App Review risk).

## Findings, sorted by severity

**G1 · core · iOS only · the big download restarts from zero after any drop, and "What has downloaded is kept" is false.**
- Evidence: `install.native.ts:131,136` hands DownloadTask the `.part` itself as the destination. expo's iOS DownloadTask is a
  `URLSessionDownloadTask` in BOTH session types (`node_modules/expo-file-system/ios/FileSystemDownloadTask.swift:277`
  `session.downloadTask(with:)`): URLSession writes to its own temp file and the bytes reach the `.part` only at the end
  (`:420` `moveItem(at: location, to:)`). On a failure only `error.localizedDescription` comes back (`:456`), so the resume
  data iOS keeps is lost. So on iOS `have` (`install.native.ts:126`) is always 0 after a drop, a lock or a kill: each of the
  4 tries and each launch starts the ~480 MB basemap again. The code comment at `:133-135` believes the foreground session
  leaves a `.part` to resume from; it does not. Knock-on: the T272 follower (`release/install.ts:125-146`) has nothing to read
  while the file grows, so on iOS the whole file is hashed in JS after the download (F14 is the full cost, not a part).
  The boot screen's "What has downloaded is kept" (`core/src/install.ts:11`) is untrue for the file in flight.
- Prove it: first open on the simulator, `xcrun simctl get_app_container booted ca.canifishthis.app data`, watch
  `Documents/release/tiles/**.part` during the download: it stays absent until the very end; toggle the Mac's Wi-Fi at 50 %:
  the next try logs `0 of N bytes on disk` (`install.native.ts:128`).
- Fix (`install.native.ts`, shared file, the change only matters where `bg` is null, i.e. iOS): fill the `.part` in bounded
  ranges, one DownloadTask per range (`Range: bytes=have-(have+32 MB-1)`), each landing in `.tail` and appended with the
  existing `appendFile`. A drop then costs one range, and the follower gets a growing file again. (Alternative: `pause()`
  on AppState background for its resumeData; does not help a dropped line.)

**G2 · visible · iOS only · a staged update is applied whenever the app comes back from "inactive", not only from the background.**
- Evidence: `apps/mobile/src/App.tsx:389-393` applies a ready update on `st === "active" && was !== "active"`, commented
  "back from the background ... the safest moment". iOS also passes through `inactive` for Control Center, Notification
  Center, the app switcher glance, an incoming-call banner and every system permission alert (Android has no `inactive`).
  So on iOS pulling down Notification Center, or answering the first-open location prompt (G7), swaps the release under the
  page being read, with the "Updated to ..." toast. Same pattern, cheaper effect: `App.tsx:363-366` (update checks,
  throttled by `service.ts:541-546`) and `shell/useResumed.ts:15-16` (app-update check count).
- Prove it: stage an update (`deploy/staging/test-updates.mjs`), open a water's regulations, pull down Control Center
  and release: the page re-boots.
- Fix: track `background` explicitly (`was === "background"`) in the three listeners. Shared files; Android behaviour unchanged.

**G3 · core (for gauge alerts) · iOS only · the background check is a BGProcessingTask, Apple's overnight-maintenance kind.**
- Evidence: `node_modules/expo-background-task/ios/BackgroundTaskScheduler.swift:92` submits a `BGProcessingTaskRequest`
  (earliest begin = now + interval, `:95`); the plugin declares `UIBackgroundModes: processing`
  (`plugin/build/withBackgroundTask.js:25-26`). iOS runs processing tasks when the phone is idle (typically locked, often
  charging, often overnight), not on a 15-minute rhythm. This sharpens F3: on iOS "Crosses above" alerts
  (`core/src/changelog.ts:166`, "when a river rises or falls past a level") can arrive hours late or not that day.
- Prove it: only on a real phone over a day (the simulator never fires them by itself; `e -l objc -- (void)[[BGTaskScheduler
  sharedScheduler] _simulateLaunchForTaskWithIdentifier:@"com.expo.modules.backgroundtask.processing"]` proves the task
  runs, not when).
- Fix: product decision. Either iOS words that say alerts are checked "when your phone allows, often overnight"
  (`core/src/alerts.ts` via F3's fix), or a `BGAppRefreshTask` (needs a small iOS module; expo-background-task has no
  option for it). A `deps.md` line either way.

**G4 · visible · iOS (both differ) · "Wi-Fi only" downloads over a phone hotspot and ignores Low Data Mode on iOS.**
- Evidence: metered is `s.type === "cellular" || isConnectionExpensive` (`ui-native/src/netInfoConnectivity.ts:36`). NetInfo's
  iOS half uses SCNetworkReachability and sets `expensive` only for WWAN
  (`node_modules/@react-native-community/netinfo/ios/RNCConnectionState.m:45-47`): Wi-Fi to another phone's hotspot is
  "wifi", not expensive, and iOS Low Data Mode (`NWPath.isConstrained`) is never read. Android's NetInfo uses the
  system's metered flag, so a hotspot counts as metered there. The hundreds-of-MB update (`core/src/install.ts:114`) goes
  out over the reader's cellular plan.
- Prove it: Mac Internet Sharing is not a hotspot; needs a real iPhone on an iPhone hotspot with the setting on Wi-Fi only.
- Fix: read Low Data Mode / `isExpensive` from `NWPathMonitor` in a small iOS function (new module or the `exit-info` iOS
  module), merged in `netInfoConnectivity.ts` (shared file, the extra signal is null on Android).

**G5 · visible · both · a spot "At my location" or a parking pin "here" is saved from an approximate fix without a word.**
- Evidence: `apps/mobile/src/locate.native.ts:30-32` takes `getCurrentPositionAsync` whatever its accuracy; callers save it
  as is (`ui/src/mapRail.ts:253` `addAt(await locate.here())`, `ui-native/src/shell/SpotHost.tsx:111`). With iOS
  "Precise: Off" (or Android's "Approximate") the fix is km off; expo already reports it
  (`node_modules/expo-location/ios/Requesters/EXBaseLocationRequester.m:162-170` `accuracy: "reduced"`), and the fix's own
  `accuracy` is in hand (`locate.native.ts:32`) but unused for spots.
- Prove it: Simulator Settings > Privacy > Location > Can I Fish > Precise off; Spots > At my location: the pin lands far off.
- Fix: in `ui/src/mapRail.ts` (shared), refuse or warn above ~200 m ("Your location is approximate; pick the spot on the
  map"), and on iOS ask `requestTemporaryFullAccuracyAsync` (needs an `NSLocationTemporaryUsageDescriptionDictionary`
  entry in `app.json`, iOS-only).

**G6 · visible · both, worse on iOS · a refused camera or location has no way to Settings, and the camera refusal is reported as a defect.**
- Evidence: camera: `pickPhoto.native.ts:24-25` throws a plain `Error("the camera was not allowed")`, which `ui/src/photos.ts:76`
  reports (not an `environmentError`, against `core/src/failure.ts:5-8`) and shows as "The photo was not added (the camera
  was not allowed)" (`data/src/spots/words.ts:306-309`), with no Settings button. Location: after one "Don't Allow" iOS never
  prompts again; `locate.native.ts:20` turns that into `denied`, the Spots row disappears with only "Location access is
  turned off for this app" (`ui/src/mapRail.ts:80-81`, `core/src/mapRail.ts:28,44`), no button. Notifications already have
  the button (`AlertMe.tsx:113`, `notify.native.ts:116`). Also `ui/src/mapRail.ts:250-256` turns every `here()` failure,
  refusal included, into "Your location could not be read" and reports none of them.
- Prove it: deny the camera prompt (real phone) or `xcrun simctl privacy booted revoke location ca.canifishthis.app`, then the Spots tab.
- Fix: an "Open Settings" link (`Linking.openSettings`) under both lines (`SpotsScreen.tsx`, the photo error in
  `SpotScreen.tsx`); `environmentError` in `pickPhoto.native.ts`; `refusedLine` (already in `ui/src/myLocation.ts:51`) in
  `addMine`. All shared files.

**G7 · review · iOS (both ask) · the first open asks for location to centre the map; the purpose string says it is for pinning a spot.**
- Evidence: `ui/src/myLocation.ts:140-147` (`useOpenAtMe`) raises the prompt on the first plain open, right after the
  first-run notice (`ui-native/src/Shell.tsx:366`). The only string the prompt shows is "Allow Can I Fish This? to pin a
  spot where you are standing" (`apps/mobile/app.json`, expo-location `locationWhenInUsePermission`). Apple asks that the
  string say every use (guideline 5.1.1); a reader also meets the prompt with no context (Apple's in-context guidance).
- Prove it: fresh install on the simulator, accept the notice: read the prompt.
- Fix: one string covering both ("to show where you are on the map and pin spots where you stand. It stays on this
  device.") in `app.json` (shared, both platforms use it).

**G8 · visible · iOS · "Directions ... in your maps app" opens Google Maps in Safari.**
- Evidence: `ui-native/src/shell/SpotHost.tsx:107` always opens `https://www.google.com/maps/dir/?api=1&destination=...`;
  the label promises "your maps app" (`core/src/spotWords.ts:38`). On Android that URL opens Google Maps; on an iPhone
  without the Google Maps app it opens the web page in Safari, never Apple Maps.
- Prove it: simulator, a spot with a parking pin, Directions: Safari opens.
- Fix: on iOS `maps://?daddr=lat,lon` (Apple Maps; no `LSApplicationQueriesSchemes` needed for `openURL`), chosen by platform
  in a `directionsUrl(os)` beside `bookUrl` in core (shared, platform-chosen).

**G9 · visible · both · a link that cannot open does nothing on screen.**
- Evidence: `ui-native/src/openLink.ts:9-14` logs and reports a refused `Linking.openURL` but shows nothing, against the
  shown-and-reported rule (`app/MAP.md` "Failures"). RN's iOS `openURL` does reject when the open fails
  (`node_modules/react-native/Libraries/LinkingIOS/RCTLinkingManager.mm:144-163`). Callers that then look dead: the report's
  "Email" (`menu/ReportPage.tsx:55`, an iPhone with Mail deleted), Directions (G8), Licences, Info pages, privacy link.
- Prove it: simulator has no Mail app account: More > Report a problem > Email: no draft, no message.
- Fix: a shared toast/line for `openLink` failures (`openLink.ts`, shared file), or move callers to `openLinkShown`.

**G10 · visible · iOS (shared code) · with the keyboard up, the first tap on a button is eaten, and the number pads have no Done key.**
- Evidence: the menu page and spot page ScrollViews set no `keyboardShouldPersistTaps` (`menu/MenuPage.tsx:39`,
  `SpotScreen.tsx:246`), so RN's default "never" makes the first tap on Save / a search result only close the keyboard.
  The alert value and percent fields use `decimal-pad` / `number-pad` (`AlertMe.tsx:235,259`): on iOS those keyboards
  have no return key, so tapping elsewhere is the only way to close them (Android's pad has Done and a back key). The
  gauge search on the Notifications page has `autoFocus` and no `autoCorrect={false}` (`menu/NotificationsPage.tsx:108-109`),
  so iOS autocorrect rewrites river names on space (the top search and glossary turn it off: `TopBar.tsx:132`,
  `GlossaryPage.tsx:99`). Not F2 (that is the keyboard covering fields).
- Prove it: More > Notifications > Add an alert, type "Coquihalla " (autocorrect), pick a gauge, type a value, tap Save once.
- Fix: `keyboardShouldPersistTaps="handled"` on both ScrollViews, an `InputAccessoryView` Done bar for the two numeric
  fields (iOS-only component), `autoCorrect={false} autoCapitalize="none"` on the gauge search. Shared files.

**G11 · review · iOS · About > What's new tells iPhone readers about Google Play and Android.**
- Evidence: `changelogVM` lists every version unfiltered (`core/src/appUpdate.ts:95-101`); notes include "only once Google
  Play has it for your phone" (`core/src/changelog.ts:135`), "on Android" (`:141`, `:178`) and Android-only changes such as
  the pinch glide (`:31`, T707 is Kotlin only, pass 1 section 6). Apple rejects mentions of other platforms in app UI
  (guideline 2.3.10), and the iOS app's first version would show a long Android history.
- Prove it: simulator, More > About > What's new.
- Fix: a `platforms?: ("android"|"ios"|"web")[]` per note or entry in `core/src/changelog.ts`, filtered by `changelogVM`
  (shared; `build-apk.sh` reads the same file, so keep the field optional).

**G12 · review · iOS · the build asks for the push entitlement the app never uses.**
- Evidence: expo-notifications adds `aps-environment` unless told not to
  (`node_modules/expo-notifications/plugin/build/withNotificationsIOS.js:51-57`, default `enableRemoteNotifications: true`);
  `app.json` passes only `icon` to the plugin. The app is local-only (`notify.native.ts:2`). Push then has to be enabled on
  the App ID for signing, and a review may ask what push is for.
- Prove it: `npx expo prebuild -p ios`, read `ios/*/*.entitlements`.
- Fix: `["expo-notifications", { "icon": ..., "enableRemoteNotifications": false }]` in `app.json` (shared file, iOS-only effect).

**G13 · cosmetic · iOS mostly · a dropped native download is reported as a defect.**
- Evidence: offline is recognised by a thrower's flag or the fetch words in `core/src/failure.ts:26,38`. The native
  download's iOS failure is `UnableToDownloadException(error.localizedDescription)`
  (`FileSystemDownloadTask.swift:456`, e.g. "The network connection was lost."), unmarked; `nativeFill` rethrows it
  (`install.native.ts:157`) and `ui/src/boot.ts:91` reports it. With F1 and G1, every locked screen during a first install
  is a crash-inbox entry. Android mostly uses the system downloader (bg-download), so it is rarer there.
- Fix: in `nativeFill`, mark the last error `environmentError` when the device is offline or the message is an
  NSURLError/OkHttp line drop (`install.native.ts`, shared).

**G14 · visible · Android · spots and photos are never backed up to Google.**
- Evidence: the prebuild manifest has `android:allowBackup="true"` and no backup rules (`apps/mobile/android/app/src/main/
  AndroidManifest.xml:17`, generated: `app.json` sets nothing). Spots, photos and the ~480 MB release share the files
  directory (`spots/store.native.ts:25`, `photoStore.native.ts:15`, `install.native.ts:24`). From the Android docs (not
  code): Auto Backup stops when an app's data passes 25 MB, so nothing is saved. Device-to-device transfer has no cap.
  The mirror of F6: iOS backs up too much, Android nothing.
- Prove it: `adb shell bmgr backupnow ca.canifishthis.app` on an installed emulator; logcat shows the quota exceeded.
- Fix: `dataExtractionRules` / `fullBackupContent` excluding `release/` (a config plugin beside `plugins/with-no-minify`,
  Android-only), which also lets spots back up.

**G15 · review (low) · both · no sentence says the app is not affiliated with the Province.**
- Evidence: the first-run notice says "Not the official regulations" and "not legal advice" (`core/src/menu/legal.ts:20-24`);
  only the DFO licence text says "not ... in affiliation with ... the Government of Canada" (`core/src/menu/licences.ts:98`).
  Store reviewers ask government-information apps for an explicit non-affiliation line (Google Play's policy says so; Apple
  reviewers ask under 5.x). Not verified against a current rule text.
- Fix: add "This app is not made by or affiliated with the Government of British Columbia." to the first point
  (`legal.ts`, shared; `FIRST_RUN_VERSION` bump re-asks).

## Angles checked, clean

- **Hermes / Intl / dates (5):** every day and time goes through `core/src/clock.ts` (UTC-7 written in code, not tz data)
  and `day.ts` B.C. fields; `Intl.DateTimeFormat` only for an explicit zone, with a reported fallback (`day.ts:172-190`);
  numbers through `grouped.ts`, not `toLocaleString`. `localeCompare` is only a tie-break in sorts (`search.ts:104,146,223`).
- **sqlite on iOS:** pass 1's open question (does `openDatabaseAsync` take a `file://` directory on iOS) is answered yes:
  `expo-sqlite/ios/DatabaseFileUtils.swift:17-33` percent-encodes and parses `file:` URIs, so spaces are safe.
- **Files (6):** HEIC from the iOS library comes back raw `.heic` (`expo-image-picker/ios/ImageUtils.swift:147`) and is
  kept as such (`data/src/spots/photos.ts:50-61`); photo refs are relative. Release file names are lowercase hashes (54 files
  in `data/release/r-f749475d0962`, no two differ only by case, none has a space).
- **Network (8):** no `http://` outside the local dev LAN base (`config.ts:8`, `env.ts:8`); no `canOpenURL`, so no
  `LSApplicationQueriesSchemes` needed. Background-task identifier and mode come from the plugin.
- **Staging switches in production (10):** `app.config.js` refuses a production build with the map lab or a local MapLibre;
  the Updates log and test tags are off in production (`config.ts:69,111-112`).
- **Reduce Motion (9):** read and followed (`motion.ts:43-44`, `FishSpinner.native.tsx:59-62`, `RegsScreen.tsx:159`).
  Pressables carry roles or labels nearly everywhere (scan of `ui-native` and `map`); the exceptions are scrims with labels.
- **Report outbox:** "will be sent automatically" is backed by a file outbox flushed on every check and reconnect
  (`spots/store.native.ts:88`, `update/service.ts:497-498`); true while the app is open, on both.

## Needs a run to know

- Dynamic Type at the largest accessibility size: iOS goes to about 3.1x, Android to 2x; most Text is uncapped by design
  (`type.ts:117-122`). Shots at AX5 on iPhone 17e.
- Restore from an iCloud backup onto a new phone: `location-ask.txt`, `first-run.txt` and `notify.json` come back
  (`spots/store.native.ts`), the permissions may not, so alerts read "on" with notification permission undetermined, and
  the open never asks for location again. `install-id.txt` is duplicated across the two phones.
- A JS `fetch` in flight when iOS suspends the app: does it settle (error -1005) or hang a spinner on return.
- Photo viewer memory with a 48 MP HEIC (raw, not recompressed, per the clean item above).
- A dev or test-bed build aimed at a LAN data base (`http://192.168...`) will raise iOS's Local Network prompt; is the
  `NSLocalNetworkUsageDescription` string present after prebuild.
- VoiceOver across the map: what the map view itself announces, and whether the tap cue is read out.
