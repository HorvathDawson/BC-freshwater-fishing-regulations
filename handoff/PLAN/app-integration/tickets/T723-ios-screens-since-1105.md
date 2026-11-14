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
