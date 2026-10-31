# T332 — iOS: the walkthrough card over the Layers sheet is missing from the accessibility tree

Status: OPEN. Source: T313 (iOS simulator test bed, 2026-10-30).

At step 17 ("Try the satellite view") the Layers sheet is open and the card is drawn over it, but on iOS neither the
card's words nor its buttons (`guide-skip`, `guide-primary`) are in the accessibility tree (XCUITest / Maestro saw only
the sheet: "Layers | Done | Map | Satellite"). So VoiceOver can probably not reach Back / Next / Show me / Skip there,
and a walk left at that step reopens the sheet on the next launch with the card unreachable. Android sees the card.
Likely cause: the sheet is presented modally (its own view controller / `accessibilityViewIsModal`) and the card is
outside it.

## Work
Confirm with Accessibility Inspector on the simulator; render the card inside the sheet's modal while a sheet is up, or
present the sheet non-modally for the walk. The test bed taps the card by position meanwhile (run-ios.mjs).

## Acceptance
`node app/tools/testbed/run-ios.mjs walkthrough` walks 25 of 25 without the position tap (no T332 finding).

Depends: T313. Size: S. Owner: no.
