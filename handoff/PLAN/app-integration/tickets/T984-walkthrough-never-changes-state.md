# T984 — The guided walkthrough only walks through: it never changes the app's state (ASAP)

Status: IN PROGRESS — built on lane/085-t984; left (Claude): merge + combined review.
Built: the tour is five cards that only point and describe (search, the map, the day, Trout/Salmon, all set), on controls the
Map tab already draws; no step is a move (`kind`, `act`, `bring`, `GuideFacts`, `GuideActs`, the paused chip, the chosen
water and its search are gone). The dim, the hole round the control included, takes every touch (tap, pan, pinch, wheel).
A control not drawn: its card with the dim whole, nothing highlighted. Tests: `Walkthrough.t984.test.tsx` runs the real
Shell through the whole tour and a skip and compares the app's DOM at every step to the app with no tour (equal; only the
tour's own seen/guide-at lines are written), and a 250 ms budget from card to ring; `help.test.ts`, `Walkthrough.test.tsx`
reworked. Also removed: `useGuideControl` hooks and host, `CoachItemContext`, `guideDay`. Emulator scenarios rewritten for the five
look-only steps, NOT RUN (owner runs them at release): `app/tools/testbed/scenarios/walkthrough.mjs` (Android: card never over
its ring, a tap on the search field and a swipe under the dim change nothing, the screen's words equal the pre-tour baseline
after Next x5 and after a Skip) and the walkthrough scenario in `run-ios.mjs`. Help's Replay still moves the app to the Map
tab (the tour's start). Device checks (Android, iPhone) not run.

Source: owner 2026-11-21: "the tutorial should be only a walk through not modify the state … right now it changes data
and is very slow in highlighting parts. which the user could zoom past and break it. it should just walk through instead
and then not change the state."

## Problem
- The walkthrough (T293, T456, T824, T842) changes real state as it goes: it opens waters, switches tabs and modes, and
  moves the map.
- Its highlights are slow to appear.
- The user can pan or zoom past a step while it waits, which breaks the tour.

## Work
- **Tour mode:** steps only point at controls that are already drawn and describe them.
  - No store writes, no navigation, no map moves, no mode or tab switches.
  - Anything a step needs to show is a still picture or description inside the step card, not a live change.
- When the tour ends or is skipped, the app is exactly as it was before it started. Pin this with a test that
  snapshots the stores before and after.
- Highlights appear at once: measure the step-to-highlight time and set a budget.
- A step whose control isn't drawn shows its card with nothing highlighted (T824's rule). User input during the tour
  can't break it.
- Check on Android, iPhone and web.

## Acceptance
- A full tour, then skip, leaves every store unchanged (the test).
- No step waits on map or network.
- Zooming or panning mid-tour breaks nothing.
- Before and after shots go to the owner.

Depends: —. Size: M. Owner: look at the shots.
