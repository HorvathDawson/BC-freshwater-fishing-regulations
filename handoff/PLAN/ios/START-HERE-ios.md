# iOS hardening: start here (written 2026-11-14)

Owner, 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is rock
solid too?" This **reopens iOS**, which the owner benched on 2026-11-06 ("no iOS device work until DFO extents labelled +
Play closed testing started; then paid TestFlight"). Read that as: simulator and code work may start now, in its own chat,
while Android closed testing proceeds. Whether the Apple Developer account is bought, and when, is the owner's to confirm
(decision 1 below). Nothing in this page is approved work: the ticket stubs are **proposals for the owner to accept**
and are not in the tickets ledger.

**First message to paste into the new chat:** "read handoff/PLAN/ios/START-HERE-ios.md and work the Unattended day section; I am away, batch questions for when I am back"

Read first: `AGENTS.md`, `CLAUDE.md`, `app/MAP.md`, then this page. History: tickets T88, T217, T250 (iOS walk), T313,
T336, T362, T363, T364, T365, T420 ("iOS half"), T244. The newest restart page is `handoff/PLAN/START-HERE.md`.

## Unattended day (owner away 2026-11-14): follow this without stopping

Owner, verbatim: "Portrait only. We want to do as much work as possible without the Apple account in the next agent also
anything else the agent could work on at same time? Need enough tasks for the agent for the day while I am gone."
The IOS-1..10 stubs below are **owner-approved to start** as of this message ("as much work as possible"); open each as a
real ticket in the ledger (next free number) when you start it. IOS-11 is not approved.

- **Goal:** everything that needs no Apple account, IOS-1 then IOS-2 ... IOS-10, simulator only. **IOS-11 (TestFlight, real
  iPhone) is NOT started.**
- **Open decisions do not block.** Use the default, mark it a proposal in the ticket, queue the question for the owner's
  return (batched, multiple choice, with pictures sent by SendUserFile). Defaults:
  minimum iOS = leave what the current Expo/RN setup gives and record the number; iPhone only (no iPad layout work);
  tap-up patch = measure first (IOS-2), write up the numbers, build an iOS patch only if the measured delay matches
  Android's and the patch is lean, as a proposal behind the same event name; Apple-account questions wait. Rotation and name
  are answered (above).
- **Parallel lanes** (disjoint files; at most 2 heavy jobs via `scripts/heavy.sh`, the second on `HEAVY_LOCK=~/.claude/heavy-job-2.lock`):
  1. iOS lane(s) in `app/` and the iOS test bed (this document).
  2. **Pipeline/tests lane** works `handoff/PLAN/NEXT-SESSION-parked-tickets.md` in this order: T667 4c, T667 7, T667 6,
     T686 survey + prototypes + ranking (**stop before the owner's pick**), then T667 slice 8. **T602 is NOT started**
     (needs a full build and the owner).
  3. **Docs lane when idle:** iOS store pack drafts (IOS-10): review notes, age-rating answers, App Privacy answers checked
     against `handoff/PLAN/play/data-safety.md`, iPhone 6.9" / 6.5" screenshots from the simulator.
  4. **Clean-up hunt (Sonnet, lowest priority, runs when a slot is free).** Owner 2026-11-14, verbatim: "can we send another
     thing to that agent to look for things that could be cleaned up too". Survey the repo for what can go or be tidied: dead
     code and unused exports, unused dependencies, files nothing reads, duplicate helpers, stale `handoff/PLAN` and
     NEXT-SESSION pages that newer ones replace, tickets marked open that are done, old generated outputs and scratch dirs
     inside the repo, old releases beyond live + previous, leftover experiment flags (e.g. `EXPO_PUBLIC_TAP_AT_LIFT`),
     TODO/FIXME comments that are done, tests that pin nothing, scripts no longer called, large files tracked in git that
     should not be. Write findings to `handoff/PLAN/CLEANUP-CANDIDATES.md` as a table: what, where, evidence it is unused
     (who was checked for readers; `graphify query` first), size or lines saved, risk (safe / needs a look / owner's call).
     Do the **safe** ones straight away (owner rule 2026-10-24: git is the safety net, quick importance check, no approval
     list): one small commit per group, `git commit -- <paths>`, tests for what changed plus `pnpm check:fast` or the
     pipeline fast-fail as fits, no behaviour change, app CI and fast-fail green. **Do NOT touch:** `marks.jsonl`,
     `data/generated/review/dfo-suggestions.json`, `data/curated/**` content, the tiles dir, guards and their tests,
     anything the other chat is changing (`app/packages/ui-native` Shell, `app/packages/map`, the test bed), files
     T667/T686/T602 already cover (the pipeline lane's), credentials and anything under `~/.canifishthis`. Never drop, rename
     or filter data content. Never pipe `ls` into `rm`. Anything not clearly safe stays in the table for the owner, batched
     with the end-of-day questions.
- **Each IOS ticket:** open it as a ticket when started; record baseline numbers; fix the parity gaps found; add or port the
  bench so it runs on the simulator; tests only for what changed plus `pnpm check:fast` run unfiltered; app CI green on push.
- **Hard limits:** no Play upload, no TestFlight, no production, no atlas promote or release, no parser or `claude` CLI,
  nothing needing the owner's Apple ID. Android behaviour must not change: run the Android `regs-open` and `tap-stalls`
  benches after any shared-code change. Never loosen a guard or skip CI. **The other chat owns the Android build train and is
  changing the tap/back path** (`packages/ui-native` Shell, `map/`, test bed): `git pull --rebase` often, commit small with
  `git commit -- <paths>`, keep iOS changes in `.native`/iOS-only files where possible.
- **If a lane is blocked, park it with a note and move to the next item; do not wait.**
- **End of day:** a plain summary at the TOP of this document (what was done, numbers, what failed, batched owner
  questions with pictures), `handoff/PLAN/OWNER-TODO.md` and the Owner To-Do page rows updated.

## Where things stand (checked 2026-11-14; "unknown" means not confirmed)

1. **It builds and runs on the simulator.** `deploy/staging/build-ios-sim.sh [--testbed|--probes]` builds committed HEAD in
   a temp worktree (pnpm offline, `expo prebuild -p ios`, pods, `xcodebuild` Release, iphonesimulator, no signing, staging
   data) into `app/dist/ios-sim/`. Last verified build: 2026-11-06, HEAD b99268518, SDK 58 / RN 0.88 (T420 "iOS half").
   The app is now 0.7.9 on Android (versionCode 31); **no iOS run has happened since 0.7.x began** (the last
   run-ios.mjs run found: 2026-11-06; `deploy/staging/build-ios-sim.sh` last touched 2026-11-10). Expect drift: unknown.
2. **Config.** `app/apps/mobile/app.json` `ios`: bundle id `ca.canifishthis.app`, `supportsTablet: false` (iPhone only),
   `buildNumber` "31", `usesNonExemptEncryption: false`, a full `privacyManifests` block (required-reason APIs + collected
   data, matches `handoff/PLAN/app-integration/store/privacy-labels-draft.md`). Plugins: expo-image-picker (photos +
   camera strings), expo-location (when-in-use only), expo-notifications, expo-background-task (declares `fetch` +
   `processing`), the MapLibre RN plugin. Minimum iOS version: **unknown** (not set in app.json; Expo SDK 58's default
   applies; read `app/apps/mobile/ios/Podfile.properties.json` / the generated Podfile after a prebuild).
   `ios/` is generated by `expo prebuild` and git-ignored (`app/apps/mobile/.gitignore`): never edit it.
3. **Signing / TestFlight: scripted, never run.** `deploy/staging/build-ios-testflight.sh [--production] [--no-upload |
   --unsigned] [--dry-run]` (T88). `--unsigned` device archive compiled and linked in 80 s on 2026-11-05. The signed run
   needs `~/.canifishthis/ios-team-id` and `~/.canifishthis/asc-api-key.json` (+ the .p8): **neither exists** (only the
   Play files are in `~/.canifishthis/`). The App Store Connect app record (bundle id, SKU, "English (Canada)") is not
   made. `eas.json` has `ios` only on the `development` profile and is not the release path; the scripts are.
4. **Simulator evidence exists.** Run `ios-t313g` (0.5.0, iPhone 17, iOS 26.5): 11 of 11 scenarios passed (first open
   168 s, map, tap-water, offline, satellite, location, walkthrough 25 of 25, PDF in PDFKit, crash record, memory, perf).
   Numbers in `handoff/PLAN/app-integration/tickets/T313-ios-simulator-basic.md` and `T363-ios-memory.md`
   (footprint 147 to 290 MB; a memory warning freed nothing then; `memoryTrim.native.ts` now has an iOS `memoryWarning`
   listener, so that line is out of date: re-measure).
5. **Never on a real iPhone.** T364 (real-iPhone run) was merged into T88. Real GPU memory, the left-edge swipe back
   by thumb, location prompt, cellular download and the gauge pill padding are all unverified.
6. **Tooling on this Mac (2026-11-14):** Xcode 27.2, simulators iOS 26.5 (iPhone 17 / 17 Pro / 17 Pro Max / 17e / Air,
   iPad Pro 13 M5), CocoaPods installed. A Xcode first-launch had to be run by the owner once (T313); `lib/xcrun-shim.sh`
   is the insurance.

### What is Android-only or different on iOS (each with evidence)

| Piece | Android | iOS today |
|---|---|---|
| MapLibre binding patch `app/patches/@maplibre__maplibre-react-native@11.5.0.patch` (one patch file, both halves) | Kotlin: tile event, `setFeatureStates`, stretch images, T420 lean changes | Obj-C half (T217): `MLRNMapView.m`, `MLRNVectorSource.m`, `MLRNPmTilesIds.{h,m}`, `MLRNVectorSourceModule.mm`, `MLRNImageLoader.m`. T420 dedupe/repeat-tile LRU is in both; iOS compiled and ran 2026-11-06 (0 "not held"). iOS frame times never measured (`app/deps.md` says re-run emulator AND simulator pans on any binding bump) |
| T716 `onTapUp` (a tap said at the finger's lift, before MapLibre's 300 ms double-tap wait) | `MLRNMapView.tuneTap`, Kotlin | **iOS emits nothing** (`app/deps.md`); `app/packages/map/src/tapLift.ts` + `Map.native.tsx` `onTapUp` just never fire, so the cue arrives with the click. Whether iOS also waits ~300 ms for a click is **unknown, never measured** |
| Back | system back button / predictive back | left-edge swipe: `app/packages/ui-native/src/EdgeBack.tsx`, `@app/ui` `useEdgeBack`, `useShellNav.ts`. Simulator verdict unsettled: fast mid-height drag stays, slower higher one goes back (`edge-back*.yaml`); a thumb is the judge |
| Notifications | channels (`notify.native.ts` creates three); background task checks the alert feed | `ensureChannels` returns early on iOS (fine); `raise` uses `trigger: null`. iOS background tasks are opportunistic (expo-background-task): **delivery timing unknown, never run**; permission prompt text seen on simulator only |
| Release download (~480 MB) | background download | (T722, branch `ios/t722-install`) the iOS half of `modules/bg-download`: one background URLSession with a fixed identifier, re-attached at launch, same JS state machine as Android. Was: a foreground URLSession (b8e33ca80; expo's background session lived in JS and was lost on kill). A swipe-away in the app switcher cancels it (iOS's rule); the next open resumes. Cellular and real suspension: unverified (a device) |
| Crash records | ApplicationExitInfo | own signal/NSException handler (`modules/exit-info/ios/ExitInfoModule.swift`, T362); jetsam and watchdog kills not caught (MetricKit is the follow-up) |
| Share | `{message: url}` | `{url}` (`share.native.ts`) |
| Shared links | Android App Links shipped (T625: `assetlinks.json`, `app.json` intent filters, `appLink.native.ts`) | **none**: no `associatedDomains`, no `apple-app-site-association` served. `appLink.native.ts` is platform-free (`Linking`), so only the entitlement + site file are missing. T625 item 5 says "out of scope while iOS is benched" |
| App update notice | Play in-app update (T390) | `appUpdateChannel({os})`: iOS has no channel ("iOS before T88" in `notify.native.ts`); `EXPO_PUBLIC_APP_STORE_URL` is the hook. Unknown whether anything shows |
| PDF viewer | react-native-pdf (patched, `app/patches/react-native-pdf@7.0.5.patch`) | PDFKit (T354), opens on the marked page; ~50 MB while open, no leak |
| Permissions | location, photos, camera, notifications | strings in app.json plugins; Always-location, motion, microphone removed (T313) |
| Safe areas | `SafeAreaView` | `WindowInset.native.tsx` has an iOS branch (`IosInsets`); menu pages once sat under the status bar (fixed 404a65da0). Dynamic Island / home indicator on 17-series: seen only on iPhone 17 simulator |
| Gauge pill | stretch + content box | one stretch box per axis: text sits a few px further in; owner never saw it on a phone |
| Fonts, sprites, data | self-hosted | same JS; nothing iOS-specific found. Not separately verified |
| Rotation | unknown | unknown (app.json orientation not checked) |
| iPad | n/a | `supportsTablet: false`; App Review may still run it compatibility-mode; no iPad sim run yet (T365 item 6) |

`Platform.OS` branches in shared code (all on the platform allow-list, `app/tools/check-platform.mjs`): `appUpdater.native.ts`,
`notify.native.ts` (3), `share.native.ts`, `memoryTrim.native.ts`, `WindowInset.native.tsx`, `openBook.ts`,
`useShellNav.ts` (edge back), `emulator.native.ts` (Android only), `platformLine.native.ts`; the rest are web-vs-native.

### Test bed on iOS
`app/tools/testbed/session-ios.sh` (boots a simulator, installs, runs one capped command, shuts down only what it booted)
and `run-ios.mjs` with scenarios: `first-open map tap-water offline satellite location walkthrough pdf crash perf memory`.
It drives Maestro flows through `lib/device-ios.mjs` (simctl) and `lib/maestro.mjs` (a sandbox profile refuses
localhost:5555-5585 so Maestro does not wait forever). Offline = the app's own "Simulate offline" (staging builds only).
**Missing on iOS:** the benches added since 11-05: `regs-open`, `tap-stalls`, `saved-spots`, `gestures`/`glide`, `feel`,
`pane`, `regs-switch`, `route-scroll`, `photo-zoom`, `report`, `big-update`, `bg-install`, `feeds`, `locate` (all
`app/tools/testbed/scenarios/*.mjs`, Android `run.mjs` only). Maestro flows like `spot-add-photo.yaml` are shared and
may already work. Whether the scenario code is portable: unknown; the `pidOf`, `adb`, `gfxinfo` helpers are Android.
Store video (`app/tools/store-video`, T709/T715): Android only; T709 step 2 plans the iOS run at 886 x 1920 and
1200 x 1600 "when iOS opens".

### Store side
- Shared with Play: listing text (`handoff/PLAN/play/store-description.md`, "same text for the App Store"; `listing.md`),
  icon (`icon-1024.png`, `app/tools/build-icons.mjs`), support URL https://canifishthis.ca/about, privacy page.
- Exists: `store/privacy-labels-draft.md` (Apple section; the Device ID row is a proposal), `handoff/PLAN/play/data-safety.md`
  (Play), the `ios.privacyManifests` block, T365 compliance pass (met / missing table), `app/tools/store-shots.json` has
  `iphone67` / `iphone55` raw sizes defined (`store-shots.mjs` `RAW_PRE`); whether it has ever produced iPhone shots: unknown.
- Missing: App Store Connect record and App Privacy answers entered; screenshots at 6.9" and 6.5" actually captured
  (the tool needs an iOS capture source); age rating, category; review notes (no login, data source, "Simulate offline"
  if a staging build goes to review); the name decision ("Can I Fish?" home screen vs "Can I Fish This?" prompts, T371);
  the review build must read PRODUCTION data (needs T86 production hosting live); iPad stance; the "background modes"
  answer (downloads the next regulations release).

## Rules that carry over (all in `AGENTS.md`; these bite)
- **No parser runs, ever** (A1): `run_parse.sh`, `dispatch`, anything spawning `claude -p`. Hand the command over.
- Branch `redesign/stream-sections`; merge only into it, named explicitly; `main` is untouched. Commit only your own
  paths: `git commit -- <paths>` (others share the checkout). Never commit `data/generated/review/pfma_basin_areas.json`,
  `write-through.jsonl`, or store images. `data/generated/review/dfo-suggestions.json` is uncommitted and the owner's.
- Prefix commands with `rtk`; `graphify query` before grepping. Verify gating test runs unfiltered (rtk once false-passed).
- Heavy jobs (iOS builds, simulator runs, pods) go through `scripts/heavy.sh <label> <min free %> -- <cmd>`, one per
  lane, two lanes max. **Kill only your own PIDs and ports; never `pkill`/`killall`** (one took down the owner's tile
  server). `session-ios.sh` already shuts down only what it booted.
- Self-host everything: fonts, sprites, data only from our servers (Esri imagery and Open-Meteo are the only exceptions).
- No silent fallbacks: a failure that is shown is also reported (`reportFailure`); if iOS lacks a feature, it says so.
- One load path per screen: same output whatever the way in. No TS cascade logic: Python resolves, app reads.
- Owner decisions: ask by multiple-choice dialog; record the answer in the ticket + `handoff/PLAN/OWNER-TODO.md` + the
  Owner To-Do page (https://claude.ai/artifact/Lh2ougAwfxx1wNmCV18m8f, collection `todos`) the same turn. Search the
  transcript before re-asking.
- Subagents get an explicit Opus or Sonnet model (Fable is for the main chat). Test only what changed (Tier 0 file,
  Tier 1 `scripts/test-smoke.sh` per commit, full suite once). Send images, not paths, to the owner.
- Anything the owner must see goes to staging; the owner is the only user; reproduce device bugs in the simulator.
- Binding patch changes: re-run emulator AND simulator pans (`app/deps.md`). Lean T420 patch stays; no iOS-only fork.
- Tickets are the sole source of work; new numbers come from the README ledger (next free number: check the README, it
  is later than T716). The stubs below use IOS-n labels until the owner accepts them.

## Work list (IOS-1..10 approved to start 2026-11-14, see Unattended day; IOS-11 still a proposal)

Order matters: baseline first, then parity gaps by severity, then benches, then TestFlight.

**IOS-1. Build and run on the simulator; record the baseline.** Goal: know today's truth on 0.7.9. Do: `heavy.sh` ->
`deploy/staging/build-ios-sim.sh --testbed`, then `session-ios.sh --fresh --max-min 40 -- node app/tools/testbed/run-ios.mjs all`;
fix test-bed drift only (not app behaviour yet). Acceptance: report with the 11 existing scenarios, build time, first-open
time, memory table, shots; every failure listed with cause (app vs test bed). Size: M (one heavy run + drift fixes).
  Sub-steps: (a) `rtk git pull`, check `heavy.sh --status`; (b) build with `--testbed`, then again with `--probes` for perf/memory;
  (c) run all 11 scenarios, triage each failure app vs test bed; (d) fix bed drift (hard-coded tab/back coordinates in
  `run-ios.mjs`, Maestro ids); (e) record minimum iOS from the generated Podfile; (f) shots into `handoff/PLAN/ios/shots/`;
  (g) list every Android scenario with no iOS twin (feeds IOS-3..6). Mirrors: Android `run.mjs all`.

**IOS-2. Tap open speed and cue on iOS (T716 parity).** Goal: a tapped water shows something within ~100 ms of the tap
and opens in the Android time class. Do: add `regs-open` to `run-ios.mjs` (press / cue / shown from the `[open]` lines);
measure whether iOS waits for a double-tap before the click; then decide: accept (if click is prompt), or an iOS
`onTapUp` in the patch (decision 4). Acceptance: cold + 2 warm taps on small/mid/lake waters, numbers beside Android's,
double tap still zooms and does not open twice. Size: M (L if a patch).
  Sub-steps: port `scenarios/regs-open.mjs` (press/cue/shown) using the `[open]` lines; measure click delay with and
  without a double-tap recogniser; cold + 2 warm taps on small/unnamed/mid/Fraser/lake waters; double-tap-zoom check;
  write numbers beside Android's (T716); lean iOS `onTapUp` only per the default. Mirrors: Android `regs-open`.

**IOS-3. No-stall bench on iOS (`tap-stalls`).** Goal: no JS stall of 300 ms or more at cold start, Salmon colouring,
Run timing tab, the bell. Acceptance: bench passes on the simulator with the same threshold. Size: S-M (needs `[stall]`
lines in the `--probes` build).
  Sub-steps: port `scenarios/stalls.mjs`; make `--probes` print `[stall]`/`[work]`; cover cold start, Salmon colouring, Run
  timing tab, the bell; fail at 300 ms; fix any iOS-only stall. Mirrors: `tap-stalls`.

**IOS-4. Back and swipe.** Goal: left-edge swipe back is dependable by thumb and by the test bed; sheets, menu pages,
walkthrough, rule book, salmon/run-timing screens all leave predictably. Acceptance: `back-layers`-style walk on
simulator; real-iPhone verdict (needs owner) recorded; if a fast mid-height drag still stays, fix the gesture or its
hit area. Size: M.
  Sub-steps: port `edge.mjs` and the `edge-back*.yaml` flows; sweep drag height/speed grid, record which go back; walk every
  layer (water page, sheets, menu pages, walkthrough, rule book, salmon/run timing, search) one step per swipe; fix hit
  area/threshold in `EdgeBack.tsx` if the grid fails (iOS-only file); confirm Android untouched. Mirrors: `edge`,
  `back-layers.test.ts`.

**IOS-5. Downloads, offline, background.** Goal: first run survives a kill, app-switch, Wi-Fi to cellular, low disk, and
a big update; background release check and notices behave on iOS. Acceptance: `first-open`, `offline`, `big-update`,
`bg-install` equivalents on iOS; notification permission and one delivered notice shown; the Always/background
declarations explained for review. Size: L (background behaviour needs a real device to be honest).
  Sub-steps: port `install.mjs`, `bg-install.mjs`, `big-update.mjs`, `offline.mjs`, `gauge-offline.mjs`; kill mid-download and
  relaunch; app to background mid-download; low-disk (simctl) message; notification permission prompt flow and one
  raised notice via the feeds test channel; read what expo-background-task allows on iOS and write it down; background-
  mode declarations explained for review. Mirrors: same-named Android scenarios.

**IOS-6. Map, salmon and run timing, search, saved spots, photos.** Goal: the screens added since 11-05 work on iOS.
Do: port `saved-spots`, `photo-zoom`, `route-scroll`, `pane` scenarios; shots side by side with Android. Acceptance: each
scenario passes or its failure is ticketed. Size: M-L.
  Sub-steps: port `saved-spots`, `spots`, `photo-zoom`, `route-scroll`, `pane`, `part-steps`, `regs-switch`, `report`, `feeds`,
  `locate`; photos via simctl addmedia; side-by-side shots with Android; salmon + run timing map walks. Mirrors: those
  scenarios; `spot-*.yaml` flows are shared.

**IOS-7. Shared links as universal links.** Goal: a shared `canifishthis.ca/water/...` link opens the app on that water.
Do: `associatedDomains` in app config for both hosts, site build serves `/.well-known/apple-app-site-association` (mirror
the T625 `assetlinks.json` work and its site test), Team ID from the owner. Acceptance: simulator `xcrun simctl openurl`
opens the water; link without the app opens the web. Size: M (final proof needs a signed build).
  Sub-steps: copy the T625 site test for `/.well-known/apple-app-site-association` (Team ID as a placeholder read from config,
  never invented); `associatedDomains` via app config for both hosts; check `appLink.native.ts` and `resolveAppLink` handle
  the iOS launch URL; `xcrun simctl openurl` runs; no-app case = web. Real proof needs a signed build (IOS-11, parked).
  Mirrors: T625 items 2-4.

**IOS-8. Memory and cold start on a real-class device.** Goal: the footprint table re-run on 0.7.9; the memory-warning
trim measured; cold start to map. Acceptance: table in T363 shape; a verdict on the oldest supported iPhone (needs the
minimum-version decision and, ideally, a real older phone). Size: S-M.
  Sub-steps: re-run `memory.mjs` on 0.7.9 and diff with T363; confirm the `memoryWarning` trim (`simctl` memory warning) frees
  what it claims; cold start x3 via `perf`; record minimum-iOS number; no device claims. Mirrors: `memory`, `perf`,
  T360 table.

**IOS-9. Layout: safe areas, Dynamic Island, rotation, dark/light, Dynamic Type, VoiceOver.** Acceptance: shots on iPhone
17 / 17e / Air / 17 Pro Max sims in light and dark; large text; VoiceOver reaches every card and button (T336 class).
Size: M.
  Sub-steps: **lock portrait on iPhone** (`expo.orientation`/`ios` Info.plist; check Android's setting and leave it);
  shots on iPhone 17 / 17e / Air / 17 Pro Max in light and dark; largest Dynamic Type; VoiceOver tree walk like T336
  (every card/button reachable); Dynamic Island / home-indicator insets; port `a11y-walk.mjs`. Mirrors: `check-colours`,
  `regs-text-scale.test.ts`, `a11y-walk.mjs`.

**IOS-10. Store pack for iOS.** Goal: everything for the App Store listing except what needs the account. Do: iPhone
6.9"/6.5" screenshots from `store-shots`, the iOS store video (T709 step 2), App Privacy answers drafted from
`privacy-labels-draft.md`, review notes, age-rating answers, name decision applied. Acceptance: a folder + a checklist the
owner pastes from. Size: M.
  Sub-steps: 6.9"/6.5" shots from the simulator through `store-shots.mjs` (iphone67 / iphone55); iOS store video via
  `store-video` (886 x 1920, 1200 x 1600); review notes; age-rating answers; App Privacy answers cross-checked with
  `play/data-safety.md` and the manifest; apply the name "Can I Fish This?" where `app.json` differs. Mirrors: T319, T709,
  T365 missing list.

**IOS-11. TestFlight (needs account; owner runs the signed script).** Goal: build 1 on TestFlight, internal testers, the
real-iPhone run (T364 list: location prompt, download on Wi-Fi and cellular, edge-back by thumb, pill padding,
walkthrough with VoiceOver, crash reports staying on). Acceptance: installed on the owner's iPhone, checks answered in the
Owner To-Do page. Size: S for Claude, owner time for the rest.

## "Rock solid" means (acceptance checklist for iOS parity)

Each line names the Android check to reuse; "new" means no iOS or Android bench exists yet.

| Check | Reuse | Pass |
|---|---|---|
| First run + download of the release | `first-open`, `install`, `bg-install` | map drawn from fresh install on Wi-Fi; kill mid-download then relaunch resumes or restarts cleanly with a shown, reported state |
| Offline | `offline`, `gauge-offline` | regs and map from the installed set, "Check now" says offline, no blank screens |
| Tap open + cue | `regs-open` | cue within ~100 ms, page first frame in Android's class, no double open on double tap |
| Double tap | `gestures`, `glide`, `feel` | zoom, not open; pinch momentum |
| Back / swipe | `edge`, `back-layers.test.ts`, `edge-back*.yaml` | every layer leaves one step at a time; cannot swipe past the root |
| Salmon + Run timing maps | `tap-stalls`, `pane`, `part-steps` | no stall of 300 ms or more; same colours and words as Android |
| Search | `search.yaml`, `search-golden` | same results; "At my location" row (missing on iOS search today: check) |
| Saved spots + photos | `saved-spots`, `spot-add-photo.yaml`, `photo-zoom` | add, rename, notes, photo from library and camera, delete, survives a restart |
| Alerts / notices | `feeds`, `report` | permission prompt once; a notice arrives; tap opens the water (`openRequest`) |
| Share link open | T625 flow (iOS: IOS-7) | link opens the app on that water |
| Satellite, location | `satellite`, `locate` | as `ios-t313g` |
| Rule book PDF | `pdf`, `regs-open` | opens on the marked page in PDFKit, closing returns to the app |
| Dark / light, text size | `check-colours`, `regs-text-scale.test.ts` | contrast and no clipping at the largest text |
| Rotation | new | decide supported or locked; no half-rotated state |
| Memory | `memory`, T360 table | footprint under half the RAM of the oldest supported phone; memory warning frees what the trim says |
| Cold start | `perf`, `near-cold` | map drawn within the same class as 0.5.0's 0.63 s warm / first-open time recorded |
| Crash path | `crash` | a signal crash reports on next start; no report loop |
| Privacy / review | T365 table | every "Met" row still true on the new build |

## Decisions that are the owner's (ask as multiple choice dialogs, batched on return; defaults in the Unattended section)

1. **Apple Developer account and TestFlight.** "Buy the Apple Developer account ($99/year)?" (a) now, so TestFlight
   starts as soon as the Android closed test is running; (b) after Android closed testing has begun (default, per the
   2026-11-06 bench); (c) not yet, simulator work only. Also: individual or organization (T88 asked this; an
   organization needs a D-U-N-S number; the name on the listing differs).
2. **Minimum iOS version.** (a) the Expo SDK 58 default (default); (b) iOS 17; (c) iOS 16 (more old phones, more risk and
   more testing). Say which oldest iPhone you want to support.
3. **iPad.** (a) iPhone only, as now (default; Review may run compatibility mode); (b) iPhone plus iPad layout (new
   design and test work, T365 item 6).
4. **iOS own tap-up patch.** "If iOS also waits for a double tap before opening a water:" (a) measure first, then decide
   (default); (b) always add an iOS `onTapUp` to the binding patch for parity; (c) accept the iOS delay. Note the patch
   is not upstreamed and each addition makes bumping the binding harder.

**ANSWERED 2026-11-14 (owner, "Portrait only."):** rotation = portrait only. Lock it on iPhone (part of IOS-9).
**ANSWERED (name; settled for Play 2026-11-13, same for iOS; T371 closed on that point):** "Can I Fish This?" on both stores.
Apply it to app.json's `expo.name` / prompts / listing in IOS-10 if they still differ.

## What needs the owner physically (kept to the minimum)

- Apple ID with 2FA to enrol, pay and accept agreements (only after decision 1).
- The Team ID and an App Store Connect API key (.p8, role App Manager), saved as `~/.canifishthis/ios-team-id` and
  `~/.canifishthis/asc-api-key.json`; Claude must not make or hold them. Create the app record (bundle id
  `ca.canifishthis.app`) in App Store Connect.
- One real iPhone for the TestFlight check (and a note of its model and iOS version); an older iPhone if one exists.
- Entering App Privacy answers and the age rating in App Store Connect (Claude supplies the text).
- Nothing else until IOS-11: IOS-1 to IOS-10 run on the simulator without the owner.

## Unknowns and risks (honest)

- **Drift since 2026-11-06.** Two weeks of app changes (0.6.x to 0.7.9: salmon build, run timing, saved spots, tap cue,
  Qualcho/tidal release) have never built for iOS. The first build may fail or a screen may break; IOS-1 says.
- **Simulator is not a phone.** Its numbers use the Mac's CPU/GPU; GPU memory, thermal limits, jetsam kills and real
  gesture feel can only show on a device. A "rock solid" claim needs IOS-11.
- **Xcode 27.2 / iOS 26.5 simulator.** Tooling is new; Maestro and `simctl` quirks were already hit (first-launch hang,
  localhost probe). Expect more.
- **Background work on iOS is not guaranteed** (the OS decides when expo-background-task runs); alert notices may be late
  or absent; the product promise on iOS may need wording changes.
- **Review risks:** 480 MB first download (Review may hit slow networks; state the progress clearly), `fetch`/`processing`
  background modes, government-information wording (T365), production data must be live first (T86).
- **Binding patch on two platforms:** every MapLibre bump needs both halves re-applied and both pans re-run.
- **Nothing here has been measured for iOS frame times**, only JS lag and stalls.
- Unconfirmed in this research: minimum iOS target, rotation setting, whether the app-update notice appears on iOS, whether
  `store-shots.mjs` has ever produced iPhone images, whether `build-ios-sim.sh` still works on HEAD.

## First moves for the new chat
1. Read the files above; `rtk git status`; check `scripts/heavy.sh --status` and that the emulator is not in use by an APK
   lane (Android lane is editing `app/`: read, do not edit while it builds).
2. Ask the owner decision 1 and 2 (they shape scope); default to simulator-only work meanwhile.
3. Run IOS-1 as one heavy job; post the baseline with shots; then propose the accepted stubs as tickets.
