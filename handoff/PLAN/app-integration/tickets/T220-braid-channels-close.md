# T220 — Braid channels close: no channel line ends where the water goes on

Status: DONE — promoted 2026-10-28, live r-46be3c135e68 (tiles db853e9556df81a6.16b3906b04bf).

Source: owner 2026-10-28 screenshot, Skeena at Salvus / Gitnadoiks (MU 6-10): side channels left the river and stopped
on bars. Owner picked FIX "complete" (not "drop", no line thinning).

## Cause (two, both ours; T202 simplification ruled out: <=7.4 m z10, <=1.9 m z12-13)
1. Braid prune (`atlas/graph/prune.py`): a nest reduced to its load-bearing channels left a kept channel's stub above
   the creek it carries, and a creek whose channel went ended on the bar (its edge re-homed, its line not).
2. Zoom ladder (tile export): a channel's sections take their first zoom from their own FWA magnitude, so a
   magnitude-1 piece between z10 pieces drew from z13 — the channel stopped below z13.

## Work (4c90fe68, 64c28a2b)
- prune: `close_dangling_ends` restores the dropped pieces that carry the water on from each dangling end, in its
  own direction (a stub upstream to its river, a creek downstream to where its channel rejoins), shortest first.
  The undirected first cut handed 3 creeks a dead-end FWA channel (Williams Creek's 1100 at Lakelse) and they lost
  their river's rules (Skeena 6-10 r1, Peace 7-31, Similkameen 8-2) — fixed by following flow.
- tiles: `side_channel_run_zooms` — every connected run of side-channel sections takes the run's lowest first zoom.

## Result
- Province: 1,041 dangling ends -> 97 (998 pieces restored); the 97 are FWA side channels with no outflow edge, whose
  creeks stay re-homed onto their river. Ladder: 2,869 side-channel sections drawn earlier to match their run.
- binding_diff reaches/full -> full_next: 511 rules / 25 licensing records change, every gained section a new one
  (1,000 restored, 33 length re-cuts), every lost one a re-cut id (35); no existing section gained or lost a rule.
  One automatic length cut on the Skeena (length:360887278:114899) moved; no curated cut lost.
- 4 sites at z12 (Skeena/Fraser Hope-Agassiz/Bella Coola/Squamish): 4/1/1/0 dangling channel ends -> 0.
- Re-cut: taps (Vedder/Bella Coola/Dean/Chilliwack/Kootenay/Fraser/Elk recounts; Dean x13p2 and Kootenay x9p2 gone,
  124 tapped + 10 gone), hydrate, regs-fixture, VM fixtures, unnamed.release handles, waterGroup handles.
Shots: T220-shots/ (live app :39218, z10-z13 at the 4 sites).
