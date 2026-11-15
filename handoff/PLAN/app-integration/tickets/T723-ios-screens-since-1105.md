# T723 — iOS: salmon, run timing, search, saved spots, photos work on the simulator (IOS-6)

Status: OPEN.

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
