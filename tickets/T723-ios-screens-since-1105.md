# T723 — iOS: salmon, run timing, search, saved spots, photos work on the simulator (IOS-6)

Status: IN REVIEW — merged parts d15784a4f; the last three (lane L6: the Save bar on the iPhone keyboard, the Conditions alert field above the decimal pad, the credits card in ModalBack) 9e9c40b61, 31ae56eda, 83dbd63d1, proven on the simulator (keyboard 7 of 7, 418f6f3a1); item 3, the 10 iOS scenario ports, DROPPED by the owner 2026-11-20; owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T723 | iOS: salmon, run timing, search, saved spots, photos work on the simulator (IOS-6) | IN REVIEW (lane L6, branch lane/l6-ios: the Save bar on the iPhone keyboard, the Conditions alert field above the decimal pad, the credits card in ModalBack, simulator keyboard 7 of 7; item 3 DROPPED by the owner 2026-11-20); owner: see ## Owner | d15784a4f, 9e9c40b61, 31ae56eda, 83dbd63d1 | owner 2026-11-14 (iOS reopened, simulator only); stub IOS-6 in docs/agents/START-HERE-ios.md |

Source: owner 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is
rock solid too?" and "We want to do as much work as possible without the Apple account". Stub IOS-6 in
`docs/agents/START-HERE-ios.md` (approved to start there, "Unattended day"). Simulator only; no Apple account.

## What
**IOS-6. Map, salmon and run timing, search, saved spots, photos.** Goal: the screens added since 11-05 work on iOS.
Item 3, the iOS scenario ports, DROPPED: owner 2026-11-20 01:56 (lane-plan batch), verbatim: "Make sure their tickets are  removed/completed/no doing labelled".

Done (all merged): keyboard + modal back + audit fixes G2, G5, G6, G8, G9, G11, the arrow (U+FE0E) in `ios/integration`
639d54d0d → `redesign/stream-sections` (d15784a4f, 2026-11-16; owner's answer to question 19, 10:29: "Yes, one at a time
(Recommended)"); simulator proofs in `docs/ios/shots/{t723,audit-fixes,shared-fixes}/`. Owner on the two picks,
2026-11-16 12:08: "we should fix anything that doesnt seem desirable" → the Save row lifted, the back strip stays 16 pt.
The three left items, lane L6 2026-11-20 (9e9c40b61, 31ae56eda, 83dbd63d1, 418f6f3a1): Save rides on the iPhone keyboard
(`KeyboardBar.{d.ts,native,web}`, an `InputAccessoryView` on iOS, one bar per field; `keyboardDismissMode="on-drag"`
after an interactive-dismiss hang); Conditions → Alert me above the decimal pad (`ConditionsPanel`); the map's credits
card in `ModalBack` (`MapProps.modalBack`). Simulator `run-ios.mjs keyboard` 7 of 7 PASS (build 418f6f3a1, iPhone 17e,
iOS 26.5). Tests: `SpotScreen.keyboard.test.tsx` (5), `ConditionsPanel.keyboard.test.tsx` (2), `MapCredits.render.test.tsx`
(+1), `MapScreen.test.tsx` (+1). Shots: `after-l6-spot-save-bar.jpg`, `after-l6-spot-notes-bar.jpg`,
`after-l6-spot-drag-keyboard-away.jpg`, `after-l6-cond-alert-value.jpg` (before: `before-l6-*`).

## Left
- The owner's look at the Save bar (below). Audit table: git history, `docs/ios/parity-audit.md`, deleted 2026-11-25.
- Not done, stay-out file `shell/SpotHost.tsx`: G6 parking "Use my location" still says "Could not find your location."
  with no button and reports nothing (proposed: `locate.here().then(..., (e) => { reportFailure(e); setParkingNotice(refusedLine(e) === MY_LOCATION.denied ? MY_LOCATION.denied : SPOT_WORDS.parkingNoFix); })`,
  then `<OpenSettings palette={palette} line={parkingNotice} />` under the notice in `SpotScreen`); G5 the parking pin
  "here" can pass `f.accuracy` to a notice (`roughFixLine(f.accuracy)` in `@app/core`).
- Not proven on iOS: G2 (no `[swap] apply` line logged; a way: stage, read "Release, staged" in the Updates log, then
  the two gestures), G5, G9, the camera refusal (unit-tested only); T731's step that crosses into the next water (needs a
  water whose end step crosses: a split lake's other area, a river into a lake); T734's `open-race` iOS twin (cold start →
  search → Open at once; Chilliwack + Fraser); the arrow `↗` in `accessibilityLabel` and the map's native text, `⚠` /
  `↩ Release` on a screen; a real thumb, other models' keyboard heights, iPad's floating keyboard, VoiceOver over the
  16 pt edge strip.
- Audit findings with no fix here: **F4** `onTapUp` is Android-only (iOS waits about 300 ms for the double-tap
  recogniser; `run-ios.mjs regs-open`); **F5** Android hides the grey streams after a tab switch with a held picture
  (T451, Kotlin only), iOS has none, cause in the shared MapLibre core; **F11** the iOS `location` scenario's skip
  (`run-ios.mjs:297-304`) is a test-bed error: point it at Spots. **T717 memory (for the main lane):** a water's page
  331 / 306 MB on build 081d93f90 against 213-230 MB on the baseline (8ba815a23), +80-115 MB, consistent with the browse
  map kept mounted under the page. A tab tap with an iOS Modal open takes two taps (the first lands on the scrim).

## Acceptance
each scenario passes or its failure is ticketed.

Depends: T718. Size: M-L. Owner: no.

## Owner

- [ ] 2026-11-20 — iPhone: keep the purple "Save spot" bar that rides on top of the keyboard while a spot's title or notes are typed (Q182)? Pictures: `docs/ios/shots/t723/after-l6-spot-notes-bar.jpg` (after), `before-l6-spot-save-row.jpg` (before). Default: keep as built.
