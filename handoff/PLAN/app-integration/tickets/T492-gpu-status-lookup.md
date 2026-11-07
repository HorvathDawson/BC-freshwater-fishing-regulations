# T492 — Stream colours chosen on the GPU (per-day status lookup) — far future

Status: OPEN — FUTURE, parked. Owner 2026-11-07. Start only on a measured trigger (below).

T420's MapLibre patch cut feature-state work from 42 % to 2 % of CPU (owner: lag "a lot better" on the phone, 0.6.9).
What remains CPU-side: each day/mode change re-states every visible feature. The step after that: each stream feature
carries a small status index in the tiles; the colour is a GPU lookup into a per-day table (a texture / uniform), so a
day or mode change is one table upload, not a pass over every feature.

## Trigger (any one)
- The phone shows pan/zoom lag again after T420 (the Map lab / perf trace says feature-state or layout is the cost).
- Day/mode switches take > ~100 ms on a mid-range Android with the patch.
- Upstream MapLibre gains a data-driven lookup (e.g. an expression reading a texture/global state) that makes it cheap.

## Approach (to evaluate then)
- MapLibre global-state / expression features first (no fork); else a custom layer (native: CustomLayer; web: WebGL
  custom layer) drawing streams from the same tiles; else an upstream proposal.
- Keep: cascade resolved in Python (the table is pre-resolved status per section per day), colour-blind gate, one load
  path per screen.
