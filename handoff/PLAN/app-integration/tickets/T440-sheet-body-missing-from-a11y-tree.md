# T440 — Android: the first-run notice's body (and "I understand") is missing from the accessibility tree

Status: DONE — 48efaa5fb (Sheet fix + test), 6c48c1047 (test bed appUp re-read); emulator proof 2026-11-06 below.

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

## Outcome (2026-11-06)

**Cause.** T250 (RNGH 3) made the scrolling body `GestureDetector(bodyPull) › GH ScrollView`. RNGH 3's ScrollView is a
gesture detector itself (its own `display: contents` native detector view), so the pan's detector had another detector as
its direct native child; Android then left that whole subtree out of the accessibility tree — drawn, but no ScrollView,
no text, no buttons for uiautomator, Maestro or TalkBack. Not `accessibilityViewIsModal`, not `Selectable`. Isolated with
an experiment APK drawing eight variants in the notice (`T440-shots/variants-drawn.png` drawn, `variants-notice.xml`
dumped): only detector › GH ScrollView was missing; GH ScrollView alone, detector › RN ScrollView, detector › View ›
GH ScrollView and detector › View were all present. The `scroll` dialog variant (wide layouts) uses RN's ScrollView and
was never affected.

**Fix** (`Sheet.tsx` PullScroll): an unflattened `<View testID="sheet-scroll-frame" collapsable={false}>` between the
pan's detector and the GH ScrollView. The pan/scroll cooperation is unchanged (relations are `simultaneousWith`, not view
nesting). Test: `gestures.test.tsx` pins the frame around `sheet-scroll` and the body content; the existing body-drag
tests stay green.

**Emulator proof** (test-bed APK rebuilt, `session.sh --build`, n1_pixel Android 15):
- `uiautomator dump` with the notice up lists `sheet-scroll-frame`, `sheet-scroll`, `first-run-notice`,
  `first-run-point-1..3` (titles and text), `first-run-accept` ("I understand") and `first-run-sources`
  (`T440-shots/after-notice.xml`; before: `before-notice.xml`, nothing between the title row and the inset).
- TalkBack on: swipe-next moves focus into the body and onto point 1's text (`T440-shots/talkback-focus-point1.png`).
- Body drag kept: a downward swipe from the body at its top closes the sheet.
- `run.mjs --no-r2 install regs-open water-nav walkthrough`: **4/4 scenarios passed** (`app/tools/testbed/out/runs/2026-11-06T16-46-03/`; install up in 77 s, the notice dismissed by Maestro's "I understand"; walkthrough 25 of 25).

The first walk on the fixed APK also showed two test-bed issues, not app ones: `install` failed with the app up, because
`appUp` threw on the words read BEFORE a long `dismiss-first-run` Maestro flow (fixed: one last read after the wait), and
two steps hit the emulator's Wi-Fi radio not coming back ("network did not reach WIFI (now MOBILE)", T314/T315's known
flake); the rerun had neither.
