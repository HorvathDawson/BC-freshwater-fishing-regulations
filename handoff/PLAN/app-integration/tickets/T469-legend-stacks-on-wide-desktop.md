# T469 — Legend stacks in two rows on a wide desktop, with a strip of its own for the fold button

Status: DONE 2026-11-07, 0184d8e (owner report, web at about 2000 px: "on a wide desktop the legend stacks?").

Cause: the regulations key (`LegendRows`, T396) is two fixed rows chosen to give a 390 dp phone a bar of a set height
(Closed · Own rules · Base rules / No access · Fly only · Bait ban · Not fishable), whatever the width; and the fold
button (T377) had a 41 px head row to itself. At 2000 px that was 107 px of bar for one line of words.

Fix (look only, no flag): from a 900 px window the key is ONE row (`WIDE_LEGEND` in Chrome.tsx; `LegendRows` joins its
rows) and the fold button sits at the right end of that row (MapLegend.tsx). Salmon's "DFO rules checked" line still
heads the key. Folded, and below 900 px, nothing changes: phones keep the two fixed rows and the same bar height.
Bar height: 107 px before at every width; now 40 px at 1280 and 2000, 107 px at 768 and 390.

Shots (Playwright, web, 2000/1280/768/390): `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/ee95bdfc-d51a-4f83-8602-1e5d2e402692/scratchpad/before-<w>.png`, `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/ee95bdfc-d51a-4f83-8602-1e5d2e402692/scratchpad/after-<w>.png`.
Commit: 0184d8e. Size: S.
