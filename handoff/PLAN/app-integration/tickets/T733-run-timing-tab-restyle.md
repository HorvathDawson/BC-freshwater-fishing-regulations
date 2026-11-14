# T733 — Switching to the Run timing tab holds the screen for about a quarter second

Status: OPEN (after closed testing; small)

Source: T716's measurements, 2026-11-14 (`T716-stream-tap-slow-no-feedback.md`, "Run timing 262 ms: the tab's own
restyle, no salmon work in it"). Carried as a loose note until the 2026-11-14 tidy-up; tickets are the sole source.

## Problem
After T716 every other map-mode switch measures 0 ms of blocked screen on the test-bed phone. Run timing still blocks
for about 262 ms: the map restyles all of its run-timing layers in one go when the tab is picked. Build 31's
`tap-stalls` run passed (worst 133 ms, on Run timing), so this is not a failure today, only the slowest switch left.

## Work
1. Measure first on the newest build (`node app/tools/testbed/run.mjs tap-stalls`), cold and warm, so the number is
   current.
2. If it is still the worst: prepare the run-timing style before the tab is shown (as T716 did for the lift), or apply
   it in steps so no single frame holds the screen.
3. No change to what the tab shows.

## Acceptance
- `tap-stalls` shows the Run timing switch under 100 ms blocked, others unchanged.
- The tab's colours and key match before and after (one picture pair).
