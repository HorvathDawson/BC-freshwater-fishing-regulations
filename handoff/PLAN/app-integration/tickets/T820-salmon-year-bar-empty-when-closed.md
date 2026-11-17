# T820 — The salmon year bar is empty on a closed day instead of the full red closed stripe

Status: DONE (50e432ba3; landed 2026-11-17 with the salmon twins).

Source: owner 2026-11-16, in the app chat (~21:37, on iPhone build 36), verbatim: "also the calendar bar doesnt have a full red line for salmon when closed but
instead it is empty with no color on the calendar bar..".

## History (from the main chat)
T227 deliberately kept the year bar off for a region-wide (header) closure, and T796 X1 (a0504abc1) kept "no year bar"
for the region-closed card. The owner's words now overrule that for salmon: a closed day draws the full red closed
stripe, never an empty bar. If the emptiness comes from the chain's map runs (data), send the main chat the sid and day.

## 2026-11-16 — built (lane app/salmon-twins-1116)
Cause (app, not data): core `salmonCard` gave two kinds of page NO year bar (`year.bars: []`), so `salmonStripOf`
returned null and the Keep tab drew the bare "Salmon (DFO) / Closed" heading with no bar under it:
1. the region-wide closure (`regionClosed`: R4, R5, R7A, R7B pages; T227 item 2, kept by T796 X1);
2. a page DFO lists nothing for (`salmonYear`'s `speaks` gate: the unlisted water, "not opened" only; a T227 build
   rule; 168 pages in the T531 census). Changed too, by the owner's "never an empty bar" reading: revert the `speaks`
   gate in `salmonYear` if he wants a water without salmon to keep the bare word.
No folded-row mini bar exists; the closed hatch is the game-fish bar's own pattern (same component code on every
platform; not re-checked on an iPhone here).

Fix: `salmonYear` always draws the page's one bar from the chains' map status (the same source as the map line), and
the region-closed card uses it too — closed every day = one 365-day closed run, the full closed hatch. The
region-closed card keeps its one statement and words; its bar is named by its sources (a chain where the book's spring
closure stands first: "Salmon (DFO and rule book)"). `SalmonYearStrip` never draws an empty track (no runs: no bar).

Tests: core `salmon.test.ts` (unlisted page, header-closed chain: the full closed bar), `salmonBook.test.ts` (X1's
chains offline and under a feed, the undrawn-place case); ui-native `SalmonYearStrip.closed.test.tsx` (one closed run
over the whole track filled with the closed hatch in the closed colour, light and dark; no runs → no track; the Keep
tab's salmon block draws the bar, not the bare word). Mutation-checked (no-bar for region closed, the `speaks` gate,
an empty track, a colourless hatch: each fails a test).

Pictures (web, 390 px, release r-46e4355a7277): before (bare "Closed") / after (full closed bar) Slocan River
`gnis:27958` (R4, region-closed) on Jan 15, light and dark, in the lane's scratchpad `t820-shots/`.

Left: the owner's look on the next phone build (iPhone: the hatch draws red).
