# T984 — The guided walkthrough only walks through: it never changes the app's state (ASAP)

Status: OPEN — ASAP (owner).

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
