# T90 — The Chehalis "downstream of the bridge, May 1-31" closure still reaches 2 Harrison sections

Status: OPEN.

Source: memory `receiving-river-not-a-tributary`. The Strahler guard in `_mouths_at` (pipeline/atlas/graph/
tributaries.py) is in, yet the 2026-10-22 bundle binds `chehalis_river.r2` via `trib` to Harrison sids 59983, 59984 —
both side-channel pieces (`section_span.off_stem=1`).

## Work
Trace why the walk reaches them; extend the guard to side channels whose water is the receiving river; a stub test on
`_mouths_at`; binding diff (subtract old-vs-old noise, atlas is not deterministic).

## Acceptance
No Harrison section carries a `chehalis_river.*` rule; other bindings move only where the diff explains why.

Depends: a reach rerun + deliver (atlas rebuild via heavy.sh if the fix is in the atlas). Size: S–M. Owner: no.
