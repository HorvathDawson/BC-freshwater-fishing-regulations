# T723 — iOS: salmon, run timing, search, saved spots, photos work on the simulator (IOS-6)

Status: IN PROGRESS (iOS chat: keyboard and back fixes on branch `ios/t723-keyboard-back`, small audit fixes on `ios/audit-fixes`; not merged; scenario ports not started).

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
