# T440 — Android: the first-run notice's body (and "I understand") is missing from the accessibility tree

Status: OPEN.

Source: found 2026-11-06 by the T293/T420 emulator runs (test-bed APK of redesign/stream-sections @ 329e00542, Expo SDK 58,
n1_pixel Android 15). After a data clear the "Before you fish" sheet is drawn (title, three points, I understand, Sources
and licences — screenshot `app/tools/testbed/out/runs/base3/notice.png`), but `uiautomator dump`
(`out/runs/base3/notice.xml`) holds only the scrim (`Close`), `sheet-panel`, `sheet-handle`, the title and `Done`: nothing
between y 978 and 2327 — the sheet's scrolling body (`Sheet scroll dialog`) is not in the tree. So Maestro's
`dismiss-first-run` (tap "I understand") finds nothing, and a TalkBack reader most likely cannot reach the notice's text
or its accept button either (to verify with TalkBack). The same flow worked on 2026-11-05 (T360's `memory` scenario clears
data and dismissed it), so this is probably new with the SDK 58 upgrade.

## Work
1. Confirm with TalkBack on the emulator; check the other `scroll` sheets (Layers, rule book loading) the same way.
2. Find what hides the body (an `importantForAccessibility` / `aria-modal` / `accessibilityViewIsModal` change, the
   ScrollView inside the dialog sheet) and fix it in `Sheet`; the test bed's `dismiss-first-run` passing again is the proof.

## Acceptance
`uiautomator dump` lists `first-run-accept` and the three points; TalkBack reads them; `run.mjs memory` (which clears data)
passes its boot step again.

Depends: —. Size: S. Owner: no.
