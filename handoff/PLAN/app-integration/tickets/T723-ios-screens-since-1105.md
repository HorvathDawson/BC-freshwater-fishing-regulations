# T723 — iOS: salmon, run timing, search, saved spots, photos work on the simulator (IOS-6)

Status: IN PROGRESS (app chat; the keyboard and back fixes and the small audit fixes are MERGED into `redesign/stream-sections` 2026-11-16, see "Merged" at the end; left: the scenario ports, the Conditions alert field under the decimal pad, the owner's two picks (Save row, back strip width)).

Source: owner 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is
rock solid too?" and "We want to do as much work as possible without the Apple account". Stub IOS-6 in
`handoff/PLAN/ios/START-HERE-ios.md` (approved to start there, "Unattended day"). Simulator only; no Apple account.

## Work
**IOS-6. Map, salmon and run timing, search, saved spots, photos.** Goal: the screens added since 11-05 work on iOS.
Do: port `saved-spots`, `photo-zoom`, `route-scroll`, `pane` scenarios; shots side by side with Android. Acceptance: each
scenario passes or its failure is ticketed. Size: M-L.
  Sub-steps: port `saved-spots`, `spots`, `photo-zoom`, `route-scroll`, `pane`, `part-steps`, `regs-switch`, `report`, `feeds`,
  `locate`; photos via simctl addmedia; side-by-side shots with Android; salmon + run timing map walks. Mirrors: those
  scenarios; `spot-*.yaml` flows are shared.

## Acceptance
each scenario passes or its failure is ticketed.

Depends: T718. Size: M-L. Owner: no.

## Results
(filled in as the work runs)

## Audit (2026-11-14, read from the code)
Full table: `handoff/PLAN/ios/parity-audit.md` (nothing was run).
- **F2 (visible, likely broken):** no keyboard handling anywhere (`KeyboardAvoidingView` / `automaticallyAdjustKeyboardInsets` absent in `packages/ui-native/src`); spot title and notes (`SpotScreen.tsx:180,252`), the alert fields in the bottom sheet (`AlertMe.tsx:233,257`), report and search fields sit under the iOS keyboard. Test with the shared `spot-notes.yaml` and `report-open.yaml`, software keyboard on.
- **F4:** `onTapUp` is Android-only, so the tap cue waits for the click; the T719 bed reads iOS as waiting for the double-tap recogniser (about 300 ms). Run `run-ios.mjs regs-open`.
- **F5:** Android hides the grey streams after a tab switch with a held picture (T451, Kotlin only); iOS has none and the cause is in the shared MapLibre core. Film Conditions to Map.
- **F7:** Modal pages (menu pages, PDF, sheets, photo viewer) get no left-edge swipe on iOS and `onRequestClose` is Android-only; the on-screen control is the only way back.
- **F10:** the simulator has no camera; the camera path will show its failure text. Test photos through `simctl addmedia` and the library.
- **F11:** "At my location" is not a Search row on either platform; it is the Spots tab's add row (`SpotsScreen.tsx:64-86`). The iOS `location` scenario's skip (`run-ios.mjs:297-304`) is a test-bed error: point it at Spots.
- Same on iOS by code reading: saved spots storage, share (`share.native.ts:16`), photo refs (relative), salmon colouring event path (iOS half exists), part steps, in-season inbox, PDF in PDFKit (needs the F7 look).
- Still no iOS twin of: `saved-spots`, `spots`, `photo-zoom`, `route-scroll`, `pane`, `part-steps`, `regs-switch`, `report`, `feeds`, `locate` (only `regs-open` and `tap-stalls` exist).

## T734 on iOS (2026-11-14, read in the installed binding; not yet run)
The Android chat's T734 (the app closed when `project` was called before the map was ready) patched the Kotlin binding
and gated the JS call (`packages/map/src/native/labelShift.ts`, both platforms). iOS, read in
`node_modules/@maplibre/maplibre-react-native/ios/components/map-view/`: `MLRNMapViewModule.mm` `project` / `unproject`
resolve the view on the UI block (`MLRNViewModuleUtils withView`), class-check it and reject otherwise, then
`MLRNMapViewManager` calls `[view convertCoordinate:toPointToView:]` on the MLRNMapView itself, an MLNMapView subclass.
There is no separate map object that is nil until ready (Android's `mapLibreMap!!`), so the NPE has no iOS twin by the
code, and the JS gate covers iOS as well. Left: an iOS twin of the test bed's `open-race` (cold start → search → Open at
once; Chilliwack + Fraser) to prove it on the simulator.

## iOS look at shared fixes (2026-11-14)
Build **081d93f90** `--testbed`, ad-hoc signed "to run locally" (495c9d490; earlier iOS numbers were the unsigned
8ba815a23), simulator **iPhone 17 Pro**, iOS 26.5, light and dark. `run-ios.mjs shared-fixes`
(`app/tools/testbed/scenarios-ios/shared-fixes.mjs`). Runs: `ios-shared2` (all five items) and `ios-shared3` (T729 and
T728 again). Shots: `handoff/PLAN/ios/shots/shared-fixes/`.

| fix | iOS | what was seen | shots |
|---|---|---|---|
| T729 credits card | **PASS** | A centred pop-up (box 31,358 to 371,516 pt), above the legend, readable in light and dark. With satellite on it carries "Imagery: Powered by Esri · Source: Esri, Vantor, …" (ios-shared2, both looks). It closes by its Close button and by a tap outside. It opens on Map, Conditions and Run timing. | t729-map-dark/-light, t729-satellite-dark, t729-conditions-dark, t729-runs-dark/-light |
| T729 left-edge swipe | **iOS-only difference** | The edge drag over the open card CLOSED it (both looks), although it is a React Native Modal. Most likely the drag's start lands on the scrim as a tap; not looked into further. Harmless. | |
| T729 + c099e9977 | **PASS** | Card left open on Run timing, then the Map tab tapped where it sits: no card or scrim left, and the next map tap reached the map's press handler (`[tap]` 1). **iOS-only difference:** while the card is open, iOS keeps the tab bar out of the accessibility tree (a Modal), and a finger on the tab bar may land on the scrim first. So on iOS "the tab changes with the card open" is a scrim tap, then a tab tap. | |
| T728 Flow / Level | **not verified on iOS** | The Conditions layers sheet was set to Level (shot). But both waters reached from search (Coquitlam River; Fraser River at Mission) show an "Estimate for this spot" card with no Flow / Level chart (`chart-quantity` absent), so the card's pick could not be made. Needs a gauged section reached from the map (Android's way). Test bed, not app. | (in run dir: shared-t728-layers-before-*, shared-t728-card-level-*) |
| T731 step bar | **PASS (a step within a water)** | Coquitlam River Part 1 → Part 2 (Upstream), recorded: the bar's words change in place in one frame; the Downstream / Upstream buttons keep their box (67,625-145,647 / 272,625-340,647 pt); no spinner, no resize. A step that CROSSES into the next water (the case b40336850 is about) was not reached: Part 1 is the lowest, and its "Lowest part" button is disabled. | t731-step-frames-dark |
| T717 Back from a page | **PASS** | Back from Cultus Lake's page, recorded (light and dark): the frame after the page is the map as left, fully coloured; no blank, grey or light frame. A tap on the page's empty header was not heard by the hidden browse map, and the page stayed. | t717-back-frames-dark, t717-page-dark, t717-map-after-dark |
| T717 memory | **iOS difference, for the main lane** | Footprint on a water's page: **331 / 306 MB** (light / dark run), against **213-230 MB** on the baseline build (8ba815a23, unsigned, iPhone 17). +80-115 MB, consistent with the browse map kept mounted under the page. Also differs: the signing and the device. Peak elsewhere at the baseline: 274 MB. | |
| T730 search | **PASS** | Recents are one small block (2 chips). A swipe down from the handle with the keyboard up closes search. Back after opening Cultus stays on Cultus. Search reopens empty. (The step's own close by `search-panel-close` failed: no such id in the iOS tree. Test bed.) | t730-recents-dark, t730-after-swipe-dark, t730-back-on-water-dark, t730-reopened-dark |

Test-bed flakes in these runs (not the app):
- ios-shared3's satellite step: in dark the Close id was not found after the layer switch; in light satellite ended
  up off, because the earlier look had left it on and the toggle flipped it. ios-shared2 passed the same step in both
  looks.
- First run (ios-shared, 16:50): an Apple Maps widget location prompt covered the app. It is now refused and logged
  as a finding (216495454).

**Follow-up (T729, Android chat 2026-11-14):** on Android, one tap on a tab with the credits card open closes the card
and changes the tab. On iOS it takes two taps (the first lands on the Modal's scrim), a real difference. Not fixed
here: wrap the credits Modal in ModalBack after ios/t723-keyboard-back merges, and let the tab bar take the tap.

**Second pass (`ios-shared4`, build 081d93f90, iPhone 17, dark, 2026-11-14 23:30):**
- **T728: PASS.** The Conditions layers were set to Level. Vedder River's card ("Chilliwack River at Vedder Crossing")
  opens on Level (1.81 m). Switched to Flow (29.7 m³/s). Back on the map, the layers still say Level.
  Shot: `t728-level-card-flow-level-dark.jpg` (layers before, card Level, card Flow, layers after).
- **T731, the step that crosses into the next water: not reachable on Coquitlam River.** Its top part (3 of 3,
  "Coquitlam Watershed boundary → Source") has a disabled "Highest part" button, so nothing crosses there. Still
  unverified on iOS; needs a water whose end step crosses into another water (a split lake's other area, a river into a
  lake).
- T730 again PASS: search reopens empty. The step's note "still says Cultus" was the test bed reading the recents chip;
  the shot shows an empty field.

## Found on the combined build (ios/next d658f88ac, simulator, 2026-11-15 08:30)
- **Conditions → Alert me: the decimal pad covers the value field** (field 780 to 821 pt, keyboard top 566 pt; shot in the
  `next-ios-s2` session folder). The keyboard scenario records this step and marks it PASS; it is the "alert box on the
  Conditions card" this ticket listed as not proven. To fix here with the same lift the Notifications fields got.

## Keyboard and modal back (branch ios/t723-keyboard-back, 2026-11-14)
Simulator iPhone 17e (390 × 844 pt, iOS 26.5), software keyboard on, staging data. Scenarios `run-ios.mjs keyboard` and
`modal-back` (by name only, `inAll: false`). The keyboard is not in the accessibility tree: its top is read off the
screenshot's pixels (`lib/png.mjs keyboardTop`, checked by eye on shots with and without iOS 26's violet typing glow);
the field's frame comes from `maestro hierarchy`. Before = build 541e5fd5f (no fix), runs b4–b6. After = keyboard on
builds 9d24b7d44 / fc9b788d8 (runs a1, a2: same keyboard code as the head), modal back on build 8c8497669 (runs a4, a5).
Shots (downscaled JPEG): `handoff/PLAN/ios/shots/t723/` (`before-*`, `after-*`).

### F2: the keyboard over a field (keyboard top 510 pt; number pad 537 pt)
Fix: `automaticallyAdjustKeyboardInsets` on the two scrolls that hold the fields, `menu/MenuPage.tsx` and
`SpotScreen.tsx` (iOS only: Android resizes its window and ignores the prop). It insets the scroll by the keyboard's
overlap and scrolls the focused field above it, measured natively in window coordinates, so it is right in a Modal and in
a pane alike. The draft's `keyboardLift` hook + `useKeyboardLift` wrapper were dropped: a JS padding needs each host's
distance to the window bottom and would double the native inset; no reproduced field sat in a host the prop cannot reach.
| Field | Before | After |
|---|---|---|
| Report a problem, "what happened" | clear (399–508 pt, one line) | clear (399–508 pt) |
| Glossary search | clear (top, 158–200 pt) | clear (shot; the tree read empty once in a2) |
| Notifications → Add an alert, gauge search | **covered** (575–616 pt, wholly under the keyboard; its hits too) | clear (467–508 pt); the hits scroll up above it |
| Notifications alert editor, value (decimal pad) | clear (Maestro scrolled it to 378–419 pt first) | clear (364–405 pt), with the Done bar |
| New spot: title | clear (top of the page) | clear (117–151 pt) |
| New spot: notes | **covered** (last on the page: nothing could scroll it above the keyboard) | clear (476–509 pt) |
| New spot: the Save row (pinned under the scroll) | under the keyboard | still under it (744 pt): `keyboardDismissMode="interactive"` added (a drag down the page takes the keyboard with it, iOS only; Android treats it as "none"); not tried by the bed |
| A water's Conditions → Alert me, value | not reached (the bed's scroll began on the page's small map) | not proven: the value box sat at the screen's bottom edge and the tap raised no keyboard (a1, a2) |

### G10
| Probe | Before | Fix | After |
|---|---|---|---|
| Number pads: a Done / return key | **absent**; a tap on the page head closes the pad | `DoneBar` (`DoneBar.native.tsx`: `InputAccessoryView`, returns null off iOS) on both `AlertMe` number fields (`inputAccessoryViewID`) | **Done shown, closes the pad** (a1) |
| Gauge search autocorrect ("Coquihala " typed) | **not reproduced**: read back unchanged in b5, b6 (an earlier "TCoquihala" was the bed tapping the field under the keyboard: it hit the T key) | none | — |
| One tap on a button with the keyboard up | not testable: every target (gauge hits, Save spot) lay under the keyboard | `keyboardShouldPersistTaps="handled"` on the MenuPage and SpotScreen scrolls (Android runs it too: there the same first tap also only dropped the keyboard) | **not proven**: the first hit sits at the keyboard's top edge and the bed's tap landed on the keyboard (a2). A thumb check. |

### F7: left-edge swipe over a Modal (start 8 pt from the edge, mid height; 300 ms, then 1200 ms)
Fix: `ModalBack` (`ModalBack.{d.ts,native.tsx,web.tsx}` + `modalEdge.ts`) around the content of `MenuPage`, `Sheet` (both
its forms), `PdfViewer.native` and `PhotoViewer`, calling the Modal's own `useBackLayer` back (what Android's back calls
through `onRequestClose`). It is a 16 pt strip down the left edge (the page gutter: no control under it loses a tap) with
React Native's own responder; the drag decision is `@app/ui` `useEdgeBack` (80 pt or a flick), as the Shell's
`EdgeBack`. Off iOS (`edgeBackEnabled`) the children are drawn untouched. Two earlier versions failed on the simulator:
an RNGH recogniser (inside a Modal RNGH attached its root once to a 0 × 0 view; a1) and a responder on the whole content
(a menu page's native scroll took the drag; a3).
| Modal | Before | After (a4, a5) |
|---|---|---|
| About (menu page) | stays (300 and 1200 ms) | **closes** (300 ms), back on More |
| Help (menu page) | no valid run: the bed took the screen from the tallest node (Help's content is 3374 pt) and its drag began off screen; same host as About | **closes** (300 ms) |
| Layers (sheet) | stays (both) | **closes** (300 ms), the map under it untouched |
| PDF viewer (Rule book link) | no valid run (same off-screen drag) | **closes** (300 ms), back on Cultus Lake's page (one step) |
| Photo viewer (a spot's photo, enlarged) | not reached (no spot with a photo then) | **closes** (300 ms), back on the spot |

### Android
Inert by construction and by test: `edgeBackEnabled("android")` is false, so `ModalBack` draws its children as they are
(`ModalBack.native.test.tsx`: no strip, a drag never taken); `DoneBar` returns null off iOS (test);
`automaticallyAdjustKeyboardInsets` and `keyboardDismissMode="interactive"` have no Android implementation (RN
`ScrollView.js`: "On Android this is not supported and it will have the same behavior as 'none'"; no Android reader of
the inset prop). The one shared behaviour change is `keyboardShouldPersistTaps="handled"` (both platforms: a tap on a
control with the keyboard up acts at once).

### Lines proposed for other lanes' files (not edited here)
- `ConditionsPanel.tsx` / the water pane's scroll (Shell lane): the same two props on the ScrollView that holds the
  Conditions card, `automaticallyAdjustKeyboardInsets keyboardShouldPersistTaps="handled"`, so the Alert me value box
  rises above the keyboard. Unproven which ScrollView that is from here (`WaterViews.tsx:124` mounts the panel).
- `SpotCapture.tsx`: none needed (the draft is `SpotScreen`, fixed here).

### Not proven
A real thumb (all drags are Maestro's), keyboard heights on other models (only the 17e's 510 / 537 pt), iPad's floating
keyboard, VoiceOver over the edge strip (it has no label and takes no focus), the Conditions alert box, the one-tap probe,
and the Save row reached by the interactive dismiss.

## Audit fixes (branch ios/audit-fixes, 2026-11-14)
Each fixed on the branch, one commit each; tests per change; nothing built or run on a device. "Sim" = the one-line iOS simulator proof; "And" = the one-line Android check.
- **G2 fixed.** `apps/mobile/src/App.tsx` (apply listener) + new `returnedFromBackground.ts` (pure, tested). A staged release is applied only on `background -> active`. The two other `!== "active"` listeners (update check, `useResumed`) are left: a check or a "today" re-read on `inactive -> active` swaps nothing under the reader. Sim: stage an update (`test-updates.mjs`), open regs, pull Control Center down and release: the page does not re-boot; send the app Home and return: it does. And: background and return applies as before (Android has no `inactive`).
- **G8 fixed.** core `directionsUrl(os, lat, lon)` (`spotWords.ts`), `openDirections` in `ui-native/src/openLink.ts`, one line in `shell/SpotHost.tsx` (the only `shell/` line touched). iOS opens `http://maps.apple.com/?daddr=lat,lon`, others the same Google URL as before. Sim: a spot with parking, Directions: Maps opens. And: Directions still opens Google Maps (URL byte-identical, tested).
- **G9 fixed.** core `linkFailedLine(url)`; `openLink` hands it to a sink the app sets (`setLinkFailedSink`, App.tsx -> the existing bottom toast) and still reports. Names the mail draft, directions, or the host. Sim: More > Report a problem > Email with no Mail account: toast "Could not open an email draft. Is a mail app set up on this phone?". And: airplane-free check: tap a link with no handler (e.g. a PDF with no viewer) shows "Could not open www2.gov.bc.ca."
- **G6 fixed (camera, location; photo-library has no permission on either platform).** core `permissions.ts` (`needsSettings`, `CAMERA_REFUSED`, `OPEN_SETTINGS`), `ui-native/src/OpenSettings.{d.ts,native.tsx,web.tsx}` (web draws nothing), under the Spots note, the Spots add error, the My-location notice and the photo error. `pickPhoto.native.ts` now throws `environmentError` (shown, not reported). `ui/src/mapRail.ts` `addMine`: a refusal says location is off (was "could not be read"), and a non-refusal failure is now reported. Not done: the parking "Use my location" line in `shell/SpotHost.tsx` (stay-out file) still says "Could not find your location." with no button and reports nothing: proposed lines `locate.here().then(..., (e) => { reportFailure(e); setParkingNotice(refusedLine(e) === MY_LOCATION.denied ? MY_LOCATION.denied : SPOT_WORDS.parkingNoFix); })`, then render `<OpenSettings palette={palette} line={parkingNotice} />` under the notice in `SpotScreen`. Sim: `xcrun simctl privacy booted revoke location ca.canifishthis.app`, Spots tab: the note shows "Open Settings" and it opens the app's Settings page; deny the camera on a real phone: same under the photo error, no crash-inbox entry. And: revoke location in app info, Spots tab: same button opens App info.
- **G5 fixed.** core `approxFix.ts`: `ROUGH_FIX_M = 100` (the bar `locate.native.ts` holds a recent fix to, `requiredAccuracy: 100`); `captureAtDetail(accuracy)` appends "Your phone placed you only to about 3.2 km, so this pin may not be where you are." to the capture's "Tap the stream or lake..." line. The fix's `accuracy` rides on the `addSpot` point (`route.ts`, `mapRail.ts` types) to `SpotCapture.tsx`. Never blocks. Not done: the parking pin "here" (`shell/SpotHost.tsx`, stay-out): it can pass `f.accuracy` to a notice the same way (`roughFixLine(f.accuracy)` in `@app/core`). Sim: Settings > Privacy > Location > the app > Precise off; Spots > At my location: the sentence shows under the title. And: Settings > Location > app > Use approximate: the same.
- **G11 fixed.** `ChangelogEntry.only` (note text -> platform; `notes` unchanged so `build-apk.sh`, Play notes and `check-changelog.mjs` read it whole); `changelogVM(installed, log, os)`; `InfoPages.tsx` passes the platform. Four notes tagged android (0.7.8 pinch glide, 0.6.5 Google Play, 0.6.4 screen readers "on Android", 0.5.0 "on Android"); a version left empty for iOS is not listed. Tests: Android rows equal the file's notes byte for byte; iOS text never says Google Play, Android or APK. Sim: More > About > What's new has no Play or Android words. And: the same page is unchanged. Web also now omits the four Android-only notes (it is "web", not Android). Not covered: the update prompt's incoming notes come from the published version file (Android's pipeline).
- **Arrow (item 7) fixed in one place.** `ui-native/src/textPresentation.ts` + `Text.tsx`: on iOS only (`Platform.select`), every string child gets U+FE0E after an emoji-capable symbol (arrows U+2194-2199, U+21A9/21AA, U+25B6, U+25C0, U+26A0, U+2714, U+2716, U+2139, U+2B05-2B07, U+27A1). Android and web get the very same children object back (tested), so their output is unchanged. In the app's strings that is `↗` (`LINK_OUT`, the book link), `↩ Release`, and three `⚠` titles (`core/src/regs/words.ts`). `✓ ⓘ ☰ ⊘` have no emoji form and are left. Could not judge without a simulator: whether FE0E fixes `↗` on the iOS font stack (the Apple font may still draw a tile), `↗` inside `accessibilityLabel` and the map's native text (credits, style labels: not through `Text`), `⚠` in `workers/feeds` strings (not app UI). Sim: Regs page, "Rule book p.7 ↗" is a plain arrow, not a blue tile. And: the same line is unchanged.

### Simulator proof of the audit fixes (2026-11-14, iPhone 17, iOS 26.5, build of the branch `--testbed`)
Scenario `app/tools/testbed/scenarios-ios/audit-fixes.mjs` (`run-ios.mjs audit-fixes`, by name). Shots in `handoff/PLAN/ios/shots/audit-fixes/`.
- **Arrow: proven** (`arrow.jpg`). The link reads "This water in the rule book p.7 ↗" with U+FE0E after the arrow (read from the accessibility label); the arrow's pixel crop is 25x26 px, fill 0.17 (thin strokes), colour within 29 of the link text's. A plain arrow in the link colour, not a blue tile. `⚠` titles and `↩ Release` were not on the water reached (Cultus Lake): not looked at on a screen, unit-tested only.
- **G7: proven** (`g7-prompt.jpg`). Prompt: "Allow “Can I Fish?” to use your location?" / "Allow Can I Fish This? to centre the map on where you are and to pin a spot where you are standing. It stays on this device." (The title carries the home-screen name "Can I Fish?", by design.)
- **G6: failed, then fixed, then proven** (`g6-spots.jpg`, `g6-map.jpg`, `g6-settings.jpg`). First build: the button existed but its accessibility label hid its words and it sat flush left (shot read from the first run); fixed (own words as its name, inset like the line). Second build: "Open Settings" under the Spots refusal line and under the map's "Location is off for this app..." line; a tap brings the Settings app to the front (root Settings list on the simulator, with a "Can I Fish?" back link at the top left). Not provable here: the camera refusal (no simulator camera permission), tested in unit tests only.
- **G8: proven** (`g8-spot.jpg`, `g8-after.jpg`). A seeded spot with a parking pin; Directions brought the Maps app to the front (Maps running, Safari not running), no Google Maps page. Maps then raised its own location prompts: answered by the scenario.
- **G11: proven** (`g11-top.jpg`, `g11-bottom.jpg`). 17 version rows read down to 0.1.0 (175 lines), none says Google Play, Android or APK; 0.6.5 (its only note was Android's) is not listed.
- **G9: not provable on the simulator.** This build's Contact page has no mail link, and no other link is refused on the simulator; the toast line and the report are unit-tested (`openLink.test.ts`, `linkFailed.test.ts`).
- **G5: not provable on the simulator.** "Open Settings" lands on Settings' root list and its search did not lead to the app's Precise Location switch in two tries; unit-tested (`approxFix.test.ts`, `mapRail.render.test.tsx`).
- **G2: not proven.** A release was staged on the test channel; across a Notification Center pull-down and a background return the app logged no `[swap] apply` line at all (0, 0, 0), so there is no positive control: either it was not staged in time or the probe line is not reached by this method. Unit-tested only (`returnedFromBackground.test.ts`). A way to prove it: stage, read "Release, staged" in the Updates log, then the same two gestures.
- The first step of the last run failed on an unrelated stray dialog (a leftover tap path); the earlier run's pass for the arrow, G7 and G6 stands (same build, same scenario code for those steps).

## Merged, 2026-11-16 (app chat; owner's answer to question 19, 10:29: "Yes, one at a time (Recommended)")
Merge 2 of the held app work: `ios/integration` 639d54d0d (T722 install, T723 keyboard and back, the audit fixes, T719
tap-up) into `redesign/stream-sections`. 63 commits, 57 app code files, the MapLibre patch (iOS tap-up; the Android
half is byte-identical) and the bg-download module's iOS side. Two conflicts: `prefetch.render.test.tsx` (the shared
branch's later T737 version kept) and this ticket (both sides kept).
- **One fix in the merge:** T723 G11's own test failed on the merged head: the 0.7.11 note "Android: code shrinker
  (R8) on for this build…" (added on the shared branch after this branch was cut) would have shown on an iPhone. It
  is now tagged Android-only (`changelog.ts` `only`); Android's notes are unchanged.
- **Checks on the merged head:** `pnpm check`: 6,790 tests passed, that one failed, then it and `check:fast` green
  after the tag; worker suite 487; the six Android benches on 43a489846 with R8 on and the patched MapLibre
  (10:45 to 11:14, `--no-r2`): 6 of 6 scenarios. install (up 70 s after launch), crash-bed, bg-install (the
  notification tap did not open the app, the bench launched it: the known old-bench behaviour), regs-open (cue 41 to
  86 ms, page 337 to 371 ms), tap-stalls (worst 155 ms of 300), memory (boot 248.6 MB, peak 437 MB). After the bench
  only docs and one test file (T751) changed.
- **Not checked here:** nothing iOS was re-run for this merge (the simulator proofs above are the branch's own, on
  its commits); the first iPhone build from the shared branch will be the check.
