# T478 — 0.6.8 fresh-install walk: two failures (install, water-nav)

Status: OPEN. Found 2026-11-07 (test bed run `app/tools/testbed/out/runs/2026-11-07T09-16-07`, APK 0.6.8 build 18
abf91d3). 2 of 4 passed: walkthrough (30/30 steps) and switch (32 swaps, 0 bare frames, 0 feature-state lost).
The install itself passed: downloaded and up in 77 s, no crash.

1. **install → "Updates log: every source checked on start"** FAILS: the flow `open-updates-log` looks for More →
   About → Updates log. T476 (46f948264) moved that link into the Developer card behind the credits long-press, so
   About no longer has it (shot 003). This is test drift, not an app bug. Fix the flow to open the Developer card. It
   passed at 2026-11-07T03-46, before T476.
2. **water-nav → "Coquitlam River: tapped on the map"** opens **Coquitlam Lake** instead (shot 008). It passed on
   2026-11-06 (16-04, 16-46). Suspect T457 (f3038a2/4872eda): the lake outline is ×1.5 wider, so the lake now wins
   the tap near the river's mouth. If so, this is a real regression: a river tap next to a lake opens the lake.
   Check: tap priority in the hit test (a line beats a polygon's outline?) vs. moving the scenario's tap point. Never
   just move the tap point if a river tap really opens the lake.
