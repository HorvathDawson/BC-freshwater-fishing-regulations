# T684 — Drop the build-27 cuts on five held lines with nothing tidal below

Status: DONE (434f16264)

Source: owner 2026-11-12, T678 item C, "Drop the cuts". RULINGS E20.

## Work
- FWA lines 354087703, 354154029, 356341234, 360445710, 360861076 keep a build-27 cut with nothing tidal below it. At the next atlas build the cuts are dropped (cuts that leave nothing tidal are dropped, as T665).
- Diff the binding before and after; only these five lines change.

Depends: next atlas build. Size: S.

## Done 2026-11-12 (434f16264, with T685)
The five rows left `waters/shore_held.json`; the coast rule makes no cut on them. Built (`atlas/full_next`, T685 build 2):
354154029, 356341234, 360445710, 360861076 are one section each (no cut). 354087703's build-27 cut was already gone in
the live atlas: its boundaries are the owner's Deering Road `cut` row (681 m) and the San Juan estuary reserve area. The
binding diff changes only on these lines' sections. Live once `full_next` is promoted (see T685).
