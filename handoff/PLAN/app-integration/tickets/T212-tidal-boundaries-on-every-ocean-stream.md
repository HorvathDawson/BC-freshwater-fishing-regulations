# T212 — tidal boundaries: a tidal split on every stream that enters the ocean (research first, no data changes yet)

Status: OPEN

Source: owner 2026-10-27 — "critical"; start in the next work window.

## Goal
Find the tidal boundary (where freshwater regulation ends and tidal/DFO tidal rules begin) on every stream that enters the
ocean — at least every NAMED one — as candidate splits. Every ocean stream's mouth is labelled as tidal (e.g.
"mouth (tidal boundary)", or a second name/alias "tidal" on the split). Nothing is changed in curated data yet: this
ticket FINDS them all and proposes them, with confidence and sources, for the owner to accept later.

## Inputs to cross-reference
- The owner's earlier DFO tidal polygon (the DFO area modified to match tidal boundaries that were found — find it in the
  repo/history; see the `tidal` diagnostics in the reach run and memory notes on tidal limits).
- Tidal-boundary text in the DFO salmon notices and the synopsis (e.g. "tidal waters", "tidal boundary signs", "below
  the CPR bridge"), and the tidal limits already curated (Fraser below Mission, Q42).
- Web search per stream where the above disagree or are silent (DFO tidal boundary descriptions, Fisheries Act
  schedules, maps).

## Output
- A candidate list: stream, proposed tidal split location (lat/lon, on the FWA line), source(s), confidence
  (high/medium/low), how the polygon and the text agree or disagree — as JSON + a short .md summary with counts.
- Shown in the curation review (like the DFO suggestions) for the owner to accept; accepted ones become curated splits
  labelled/aliased as tidal.
- Coverage: named ocean-entering streams found / total.
