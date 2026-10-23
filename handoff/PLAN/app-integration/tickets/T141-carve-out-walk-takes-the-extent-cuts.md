# T141 — The carve-out walk seeds from the extent's cuts; a braid cut reports its side-channel crossings

Status: OPEN

Source: T111 follow-ups (adversarial review 2026-10-23). Depends: T111 (done). Size: S–M. Rebuild: reach rerun +
binding diff.

## Problem
T111 made the tributary seeding read the cuts `resolve_extent` used (`build.seeding_cuts`). Two places still
re-derive them:
- `build.resolve_carve_outs` ("and everything upstream of it") walks with `cuts=None`, i.e. node bounds. Today that
  gives the same places except on braid side channels, where node bounds see more.
- `resolve_extent`'s `cuts` hold the principal line only (the one `_cut_at` takes); a confluence cut whose joiner
  sits on a side channel would no longer be found (none today).

## Work
1. `resolve_carve_outs` seeds from `seeding_cuts(graph, [got])`.
2. `resolve_extent` reports side-channel crossings of a cut in `cuts` (same dict shape, `kind: side_channel`).
3. Scratch reach run via heavy.sh, `binding_diff` against reaches/full; every moved section named.

## Acceptance
No-second-copy test: no reach module re-derives cuts from node bounds except `lower_bound_cuts` for an extent that
resolved none. Mutation pin. Binding diff in the ticket.
