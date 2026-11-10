# T586 — Small one-answer fixes: saved spot lead, silent gauge dot, two clocks

Status: IN REVIEW — 5a2e4117b (F9: held in one edge (a water with no flow gauge froze the first flow row, not the sheet's level-ranked lead): capture now uses `leadOver`. F10 (owner: muted): a silent gauge keeps its dot, grey, labelled with the row's words, today only (gaugePoints.ts, gaugeDots.ts; the dots are a runtime layer, not layers.source.json). F15: not fixed, named for T551: `ui/src/panel/conditions.ts` ahead `at` uses today.getFullYear/Month/Date, core/salmonFeed.ts:255 uses getHours/getDate. Shot ../T581-shots/F10-*.png. Reaches the phone with the next build; owner confirms there.)

Source: T580 audit 2026-11-10 (owner: "stuff like this should never happen … look for other things like that").
Findings, with file:line, the state that shows each and the proposed fix: `handoff/PLAN/app-integration/T580-findings.md`.

## Work
- F9: a saved spot freezes the Flow lead whatever the angler was looking at (check against the Flow-leads rule: may be correct now).
- F10 (owner): the browse map drops the dot of a station silent in the quantity, while the donor pin and row keep it and say why. ANSWERED owner 2026-11-10: "it should be muted for the gauge": keep the dot, muted.
- F15 (plausible): two clocks; goes with T551.

Acceptance: each finding named above no longer reproduces, and the test or gate the findings file proposes for it is in.

Depends: T567. Size: S. Owner: yes (F10).
