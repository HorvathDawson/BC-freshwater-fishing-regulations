# T1040 — A stale side atlas still vouches for a proposal after its cut moves

Status: OPEN — 0.9.0 batch

Source: independent review of PR #48 (T779), 2026-11-23: a low finding, older than that PR.

## What is wrong
`proposals.side_ready` trusts a side atlas because its directory exists. The directory is `side_dir(p)`: the proposal id
plus its digest (`proposals.build`), and that digest covers the proposal's OWN edits only. An extent naming a new cut
keeps its digest when the cut point moves. The cut id comes from water, name, anchor and offset, not from coordinates,
so moving the point but keeping the anchor keeps the id. The old side atlas then still reads as ready, and the place is
bound (`apply_proposals.side_check`) and the side reach run is read at the cut's OLD position. The same holds for an
atlas named by `--build` / `CURATION_BUILD`.

## What to build
Trust a side atlas only when it was built from the cuts as they would land now. Every atlas records where each cut is
(`split_anchors.json`, `splits.anchor_digest`). Each cut the proposal's chain adds, moves or names must have the same
anchor digest in that record as in the cut file with the chain applied. Otherwise the atlas is stale and the side
rebuild is asked for again. Test: a moved-cut case, proved by mutation.
