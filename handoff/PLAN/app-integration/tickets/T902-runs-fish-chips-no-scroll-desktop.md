# T902 — Desktop Runs: every fish chip shows, no sideways scroll

Status: IN PROGRESS — left (Claude): owner sees pictures, review, merge into app/integrate-082

Owner 2026-11-19 ~01:16 (app chat), desktop web shot of the Runs map's bottom panel (All · Steelhead · Chinook · Chum ·
Coho · Pink · Sock… cut off at the panel's right edge, a sideways-scrolling row inside a centred column with empty space
both sides), verbatim: "in dekstop why do we need to scroll fish...".

Build: at the desktop layout every species chip is visible without scrolling: the row uses the panel's width and wraps
to a second row only when it must (or the panel widens to fit — whichever reads cleaner); phones keep the scroll row.
Chips keep their look and their order. Tests at 1280/1440 (no horizontal scroll container on desktop; all chips
rendered), 390 unchanged. Pictures before it lands.

## What was built (lane app/t902-runs-chips)

Both of the owner's options, one each way: the PANEL WIDENS at desktop (760 -> 960 px, `RUNS_PANEL_MAX` in
`shell/MapLegend.tsx`, by `useLayoutClass()`), so the six species + All sit on ONE line; and the chip row itself WRAPS
(`flexWrap`) from the tablet class up, so a seventh chip would drop to a second line, never scroll. Widening alone left
a lone "Sockeye" orphaned on line two at 760, so it reads cleaner widened. Phones keep the sideways ScrollView exactly as
before. Chips: same look, order, tokens. Day caption and play bar sit under the row as before (the column is wider, so the
play bar's left edge moves left by 100 px at desktop; no overlap).

Tests: `RunsControls.chips.test.tsx` (1440: all 8 chips, no scrolling ancestor, wrap; 390: scroll row, same order);
Runs.shell + surfaces stay green; `pnpm run check:fast` exit 0. Pictures (1440, 390, light) in the lane's
scratchpad `t902-shots/` (fish art is a stand-in oval of the real thumb size).
