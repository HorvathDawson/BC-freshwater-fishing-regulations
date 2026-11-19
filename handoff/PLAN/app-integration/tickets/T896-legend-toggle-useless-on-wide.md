# T896 — The map key's fold button is useless on a wide screen

Status: IN PROGRESS (app chat; lane app/t896-legend-fold; pictures to the owner; in 0.8.2).

Owner 2026-11-19 ~00:51 (app chat), two desktop web shots of the Trout map bottom bar (folded: an empty white bar with
only the ^ button; open: the one-line key "Closed · Own rules · Base rules · No access · Fly only · Bait ban · Not
fishable" with a ⌄ button), verbatim: "this button is useless?"

Yes: where the whole key fits on one line, folding it only leaves an empty bar. Build: when the key fits in the bar
(measured, not a fixed breakpoint), show it always and draw no fold button; where it does not fit (phones, narrow web),
keep the fold, and the folded bar is never blank (it shows the key's first line or nothing at all; no empty white
strip). Same rule on every map mode (Trout, Salmon, Conditions, Runs). Keeps T367's layout rule (the key and its control
in the bottom bar). Pictures at 1440 px and 390 px, before it lands.
