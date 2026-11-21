# T991 — Sources display audit: make how sources are shown consistent across the whole app

Status: POSTPONED — owner 2026-11-21: next release, not 0.8.5.

Source: owner 2026-11-21, verbatim: "Add another ticket for a sources display audit (for next release we want to make
sources much more consistent across the board) not for this release".

## Work
- Inventory every place a source is shown: the regs pane's per-section "Sources" buttons and the T990 all-sources
  button, rule-book page links ("Rule book p.N ↗"), in-season notices (Province), DFO salmon notices, T981 local
  authority citations, the Conditions tab (gauges, forecasts, weather), the public website pages, More → Sources and
  licences, the first-run notice.
- For each: its words, its look (button, link, chip, row), where it sits, what a tap opens, and whether it names the
  authority the same way.
- Propose ONE pattern (words, look, placement, tap target) with before/after pictures; owner picks in a dialog round.
- Build the pick in one place in core + one component; every surface uses it.

## Acceptance
An inventory table in this ticket; the owner's pick recorded verbatim; every surface on the one pattern; a guard test
that no surface composes source words outside the one place.

Depends: T990. Size: M. Owner: pick the pattern.
