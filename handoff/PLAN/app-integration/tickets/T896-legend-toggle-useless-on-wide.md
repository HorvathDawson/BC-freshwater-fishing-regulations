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

## What was built (lane app/t896-legend-fold)
- `shell/useKeyFit.ts`: the decision is measured. The bar's width (onLayout) against the key's width on one row (an invisible,
  non-wrapping copy of the entries, `LegendCount ghost`, not an entry: no test id, hidden from a screen reader). `keyFits`: key
  + the side padding within the bar; unknown (null) until both are measured, and unknown counts as the phone layout.
- `MapLegend.tsx` (Trout and Salmon, the only modes with a fold): fits means the key on ONE row, always shown, no fold button
  (Salmon's source line still heads it). Does not fit (phone, narrow web) keeps the two fixed rows and the fold, and the
  FOLDED bar keeps the key's first row (`LegendRows firstOnly`), never a strip with only the button. The T469 900 px breakpoint
  is gone from this bar. Conditions and Runs keys have no fold (a ramp and the runs foot), so nothing to hide there.
- T396 no-jump kept: not-fitting widths are the same two-row, same-height bar in both modes. T367 kept: key and control in the
  bottom bar. Tokens only.
- Tests: `T896.legend-fit.test.tsx` (fits: no button, one row; not fit: fold; folded never empty; unmeasured; measuring copy is
  not an entry; `keyFits`); T377's fold test now expects the first row to stay.
- Pictures (real MapLegend in Chrome at the measured widths, over a stand-in for the map; the Shell's map is not paired in a
  fixture): scratchpad `t896-shots/`: `trout-1440-fits.png` (key whole, no button), `trout-390-open.png`, `trout-390-folded.png`.
- Revised (coordinator): where the key does not fit, the fold button sits at the right end of the key's FIRST row (`LegendRows
  trailing`; that row gets the width left of the button), so folded is that one row, with no strip of its own. Salmon's source
  line, when present, still heads the key above. Pictures re-rendered.
