# T1040 — A stale side atlas still vouches for a proposal after its cut moves

Status: DONE — 14f0d682e (2026-11-23)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1040 | A stale side atlas still vouches for a proposal after its cut moves (side_dir's digest does not cover the cut's position) | DONE | 14f0d682e | PR #48 independent review, 2026-11-23 |

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

## Done 2026-11-23 (14f0d682e)
I chose the atlas's own cut record over folding the split digests into `side_dir`. A digest covers only cuts that are
still proposals; a cut that already landed and then moved changes no proposal digest at all. The record covers both, and
it is per cut, so an unrelated cut landing elsewhere does not stale a side atlas.
- `proposals.stale_cuts` / `atlas_holds_chain`: each cut of the chain (`chain_cuts`: a split edit, a `split_flag`, a cut
  an extent names) has the same anchor digest in the atlas's `split_anchors.json` as in the cut file with the chain's cut
  edits made. An atlas that keeps no record is not trusted.
- `side_ready(p, props)` and the `--build` / `CURATION_BUILD` atlas in `apply_proposals.side_atlas` both require it.
  A stale one asks for the side rebuild again.
- On the served atlas, the Middle Shuswap cuts read fresh against the cut file (no false stale).
- Tests: `test_review_stale_side_atlas.py`. A new cut moved on the same anchor (same id, same `side_dir`) is not ready; a
  landed cut that moved is not ready; a label edit is still ready; an atlas with no record is not trusted; a named atlas
  is used only when it holds the cut where it lands. Five mutations, all killed.
