# T1247 — A braid loop that leaves and rejoins the same graph piece can never be pruned

Status: OPEN — its creek-less loops go with T1242's pass in 0.10.0 (E31: 4,523 of the 4,549 removed are same-piece); the route fix itself waits for T1260; owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1247 | A braid loop that leaves and rejoins the same graph piece can never be pruned | OPEN — its creek-less loops go with T1242 (E31); the route fix waits for T1260 | — | the one-line route fix with T1260 (loops with a creek) |

Source: found by T1242 part A (lane G, 2026-11-27), while measuring why 11,875 braid loops that meet the prune's
stated conditions survive the atlas. Number from the planning chat. Related: T1242 (the trial), T1176.

## The finding
A graph piece is a whole blue line between cuts, not between confluences, so a braid can leave piece M and rejoin M
itself. The nest's demand is then "the river at M must reach exit M". `nests.essential_routes` asks
`_reaches_without(graph, src=M, banned=nest, want=M)`, which walks DOWNSTREAM from M looking for M. It can never find
it (there is no cycle), answers NO, and the loop is kept as "the only route" for water that is already where it is
going. The re-home step in `prune.prune_mainstem_loops` already knows the case ("a braid that leaves a piece and
rejoins THAT SAME PIECE"). The route test does not.

Where: `pipeline/atlas/graph/nests.py` `essential_routes` (the demand loop) and `_reaches_without`.

## What the fix would remove
Measured on the 2026-11-26 live atlas, re-asked on the saved (post-prune) graph: 760 loop groups, 238.9 km, in 692
named waters. None carries a creek. Biggest: Little Qualicum River 4.3 km, Daer Creek 2.6 km, Lost Shoe Creek 2.4 km,
Riske Creek 2.1 km, Fred Nelson Creek 2.0 km. The full list is in the lane's `same_piece_bug_waters.tsv` (T1242
report). The prune has no distance limit, so a long side channel goes too, whatever its offset; the dangling-end
restore and the 5 km re-home cap still apply. Unnamed rivers' cases were not counted.

## Work
1. A demand whose exit is one of the water's own sources is already met (`want in srcs`: spare), and a test with a
   loop that leaves and rejoins one piece.
2. An atlas side build. The bindings diff per rule against live (AGENTS 63) shows the loops' km leaving their waters'
   rules, and nothing else.

## Acceptance
The 760 loops are gone in a side atlas, and the bindings diff moves nothing but their own km. The owner decides
together with T1242, because it changes the network and renumbers section ids like any atlas change.

Severity: low. Model: Opus. Size: S, plus an atlas cycle. Built by the data and release chat.

## Owner

- [ ] 2026-11-27 — Decide with the T1242 trial: fix the same-piece prune bug (760 loops, 239 km, 692 waters) in the release that takes the loop removal, or separately, or not. Not answered: he has not spoken to T1247. Covered in effect by his T1242 ruling (E31: "only remvoe the loops with no stream connecting it"): the creek-less pass removes these loops in 0.10.0.
