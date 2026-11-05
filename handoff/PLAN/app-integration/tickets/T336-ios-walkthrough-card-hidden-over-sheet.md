# T336 — iOS: the walkthrough card over the Layers sheet is missing from the accessibility tree

Status: DONE 2026-11-05 — ed1cf4a81; verified on the simulator (ios-t313g: walkthrough 25 of 25, shot T313-shots/10-walk-17-over-layers-T336-ios.jpg).

At step 17 ("Try the satellite view") the Layers sheet is open and the card is drawn over it, but on iOS neither the
card's words nor its buttons (`guide-skip`, `guide-primary`) are in the accessibility tree (XCUITest / Maestro saw only
the sheet: "Layers | Done | Map | Satellite"). So VoiceOver can probably not reach Back / Next / Show me / Skip there,
and a walk left at that step reopens the sheet on the next launch with the card unreachable. Android sees the card.
Likely cause: the sheet is presented modally (its own view controller / `accessibilityViewIsModal`) and the card is
outside it.

## Work
Confirm with Accessibility Inspector on the simulator; render the card inside the sheet's modal while a sheet is up, or
present the sheet non-modally for the walk. Meanwhile the iOS test bed records the finding and SKIPs the rest of the walk (run-ios.mjs; 16 of 25 walked).

## Acceptance
`node app/tools/testbed/run-ios.mjs walkthrough` walks 25 of 25 (no T332 finding, no skip).

Depends: T313. Size: S. Owner: no.

## Fix (2026-11-05)
Cause confirmed in code: the phone sheet's panel (and the wide dialog) carried `accessibilityViewIsModal`, which on iOS hides
the panel's SIBLINGS from VoiceOver / XCUITest — and the walkthrough card is drawn as one (`overlay`, T293, in the sheet's
own Modal window). Drawn but unreachable. Now the panel is modal only while no overlay is up (the Modal is its own window,
so the app behind stays out of reach either way), and Shell hands the Layers sheet the card only while the walk is open
(it handed it always, so the panel would never have been modal). Android/web: the prop is iOS-only; no change.
run-ios.mjs: the hidden card is a FAILURE again (it was a skip).
