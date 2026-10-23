# R10 item 11 — BRAIDS: simplify a braid straddling a cut (place on majority side, mark straddling)

Started 2026-10-16. Branch redesign/stream-sections. Side runs only under data/generated/scratch_r10/.

## Baseline (live run data/generated/reaches/full, digest a234900ee71ae0d5)
unclassified reasons: straddles_the_cut 32 pieces / 20 rules / 13 entries; joins_a_straddling_braid 4 pieces
(Chowade r1); flows_to_the_sea_unjoined 3 (Noons, Seymour — leave). scope_unclassified 3 Peace entries (no reasons).

## Plan
1. Dump real neighbourhoods of the 36 pieces (heavy, scratch) -> choose projection (junction measures vs geometry).
2. Synthetic tests first (fail on HEAD), then extent._by_measure change + policy bump + mark carried out.
3. Mark: diagnostic + bundle table (like section_home for region straddles).
4. Side reach run scratch_r10, bindiff vs live, side bundle, impact.
5. RULINGS entry, AGENTS 12 update, commit own paths.

## Log
- Dump: data/generated/scratch_r10/braids_sub.pkl (subgraph + geoms, dump_braids.py); offline look.py/proj.py.
- DECISION: geometry projection (position.share_within), per piece, 5 m samples; joiners judged as connected
  groups and follow when everything they touch is on one side. Junction measures rejected: the edge a braid
  LEAVES by carries a measure on the braid, not the main line; networks have pieces touching no main line.
- Code (uncommitted yet): extent.STRADDLING_BRAIDS_TAKE_THEIR_MAJORITY_SIDE + _majority_side + marks plumbing;
  position.share_within; classify straddle_placed diag; build scope_straddle_placed; io STRADDLE_TABLE
  (straddle.jsonl + straddle_digest); bundle schema section_straddle + rules.straddle_crossing + read.straddles;
  cache POLICY 19. Tests: test_reach_straddling_braids.py (10; 5 fail on HEAD), test_bundle_licensing (+2).
- Side reach run started -> data/generated/scratch_r10/reaches/full (log scratch_r10/reach.log).
- DONE. Commit 37292302 (code, tests, RULINGS E15, AGENTS 12). Side run scratch_r10/reaches/full digest
  cad1c21fe6eb9ba9 (371 s); side bundle scratch_r10/bundle/bundle.sqlite (section_straddle 38 rows).
  unclassified 23 lines -> 2 (the 3 municipal sea pieces only); scope_unclassified 3 -> 0; straddle.jsonl 80 rows.
  bindiff (scratch_r10/bindiff.json): 19 rules, gains only. Impact: open->closed only (Ash r1, Dean r3/r4,
  Morice r4, Zymoetz r1, Peace r5 + walked tribs). Slow real-atlas tests 25 pass; no-data targeted 1,379 pass.
- NOT DONE: answers/UI do not yet read section_straddle (read.straddles is the hook). Not promoted.
- Promote (orchestrator): python -m pipeline build --force reach
