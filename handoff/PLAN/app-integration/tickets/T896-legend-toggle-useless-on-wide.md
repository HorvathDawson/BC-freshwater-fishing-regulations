# T896 — The map key's fold button is useless on a wide screen

Status: IN PROGRESS — left (Claude): owner sees pictures, review, merge into app/integrate-082

Owner 2026-11-19 ~00:51 (app chat), two desktop web shots of the Trout map bottom bar (folded: an empty white bar with
only the ^ button; open: the one-line key "Closed · Own rules · Base rules · No access · Fly only · Bait ban · Not
fishable" with a ⌄ button), verbatim: "this button is useless?"

Yes: where the whole key fits on one line, folding it only leaves an empty bar. Build: when the key fits in the bar
(measured, not a fixed breakpoint), show it always and draw no fold button; where it does not fit (phones, narrow web),
keep the fold, and the folded bar is never blank (it shows the key's first line or nothing at all; no empty white
strip). Same rule on every map mode (Trout, Salmon, Conditions, Runs). Keeps T367's layout rule (the key and its control
in the bottom bar). Pictures at 1440 px and 390 px, before it lands.

## What was built (lane app/t896-legend-fold) — CHANGED OF PLAN
Owner 2026-11-19 ~01:00, looking at the phone pictures, verbatim: "does mobile even need the button?" No: a phone's key is two rows
(T396, about 22 px each), so folding saved one row and cost a control, and the fold came from a map-UI research proposal
(T377 item 2), not from an owner ask. So THE FOLD IS REMOVED EVERYWHERE (phone, narrow web, desktop).
- `MapLegend.tsx`: the fold state, the button, the T469 fold-in-the-row branch, and `MAP_KEY.fold` (core `mapKey.ts`) are gone; no
  fold preference was ever persisted. The key is always shown whole: one row from `WIDE_LEGEND` (T469), otherwise T396's two fixed
  rows (wrapping on a 360 dp phone is fine). The measuring idea (`useKeyFit`) was built and removed: nothing needs it.
- Kept: T396 (same two-row bar in Trout and Salmon: 66 px at 390 in both; 40 px at 1440), T377's tappable key, T729, T367's layout
  (the key in the bottom bar). Salmon's "DFO rules checked" line, when there is one, heads the key as before.
- Tests: T377.legend.test.tsx "(T896)": no fold button in either mode, and at 390 the whole key in two rows (the one-row desktop key is in trout-1440.png).
- Pictures: scratchpad `t896-shots/`: `trout-390.png`, `trout-1440.png`, `salmon-390.png` (the real MapLegend in Chrome over a
  stand-in for the map).
