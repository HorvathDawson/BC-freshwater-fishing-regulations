# T679 — Each decision in its own stage (everything but tidal)

Status: IN PROGRESS — D2 and D6 built in c9488453c (owner 2026-11-12); D1 ponds, D3 nearer row, D4 word-read guard, D5 DFO confluence cuts and gauge placement #4 built in 9b0d6f4ae (+ f7f4fcfee), promoted with `full_t678b` (a5fe40636) and released in r-0a9930fb8826; the 11 other-line gauge cuts decided (RULINGS P49, T682); plan steps 1–3 done (#9 e1f1a268c, #2 8bc6eadbe, #4 b3a3938de); #14 done (9b0d6f4ae); lane L7 2026-11-20: #10 ece8f7a29, #11 7774bcbab, #12 rulings a493cf12d (+ 59e9589cc), #13 61f2bd774, step-10 code 5339c18ef (#1 and #6 behind switches, #15) and 94842bada (#18 dead code), all proved no output change; #12 derive from data in the 0.10.0 re-cut (owner 2026-11-27); left (Claude): wave 4 flips #1/#6 with the atlas rebuild and diffs, #18's continuation / places / part point, and L8's steps 5 (#8, #16, #17)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T679 | Each decision in its own stage (everything but tidal): 18 findings, owner D2–D6 answered 2026-11-12 | IN PROGRESS | c9488453c, 9b0d6f4ae, f7f4fcfee, e1f1a268c, 8bc6eadbe, b3a3938de, ece8f7a29, 7774bcbab, a493cf12d, 61f2bd774, 5339c18ef, 94842bada | owner 2026-11-11 "look for other things like that too"; D1–D6 + gauge placement released (r-0a9930fb8826); other-line gauge cuts decided (P49/T682); L7 2026-11-20: #10/#11/#12 … (detail: ticket file) |

Source: owner 2026-11-11, about tidal: "we want to avoid it being a weird post processing step", "the cuts used for
tidal should happen where cuts happen etc.", then: "Look for other things like that too". Tidal itself is T678; this
ticket is every other stage. It mentions tidal only where another topic touches it.

The audit (read-only, `redesign/stream-sections` at 6c3bc9588) found 18 findings: 9 can change what the angler sees,
9 are maintenance only; D1–D6 were how a regulation is READ, for the owner.

## Owner answers
Owner 2026-11-11: **D1 (#5, Q154) - ANSWERED: yes, river for both.** Whole-river rules and "in streams" rules both apply on a river's unnamed widening pond (`reach/extent.py:826`).

Owner 2026-11-12: D1 ponds: built then reverted (the bundle refused it: 161 rule keys mixed lake and stream); goes into the next atlas build instead (16,152 ponds change; 97 rules gain, 32 lose). D2 partial salmon coverage: map and bar both give the species verdict. D3 nearer row wins: ignore the loops (receivers with flow-back removed), next build, diff shown. D4 words read at build time: leave, add a guard that stops the build and names the row when a row that matched before stops matching. D5 DFO confluence cuts: same as provincial, the joining water stays out unless DFO's words add tributaries; diff before it ships. D6: Python reads DFO rows tolerantly too (an unreadable row reads "check", is reported, and no longer stops a release). Gauge placement (#4, held): "One rule, accept the moves" - atlas placement wins; 29 of 458 shown gauges move at the next build; list reported.

Owner 2026-11-20 09:59 (dialog), #8: "Ranking's rule, list first (Recommended)": along the water, a lake on the way ends it; the list of gauges that move goes to the owner before it ships.
Owner 2026-11-20 10:45 (dialog), after the list (`T679-shots/own-gauge-moves.csv`) was sent: **"Ship it"**. `packed.SHIP_OWN_GAUGE_FLAG` is ON (merge of lane/l8-shrink); `flow-own/1` ships with the next data release, which rebuilds the bundle for `section_own`.
Owner 2026-11-20 11:02 (dialog), after the blocker measure (`T679-shots/own-gauge-blockers.csv`: 448 of 1,060 moves behind unnamed ponds of no water): **"Named lakes only (Recommended)"**: `own_distances` walks through an unnamed pond that belongs to no registry water, at any size; a named lake still ends it. The lead-gauge ranking reads the same walk, so it changes with it. Ships with #8.
Owner 2026-11-20 11:11 (dialog): #1 and #6 **"Turn both on (Recommended)"** in the wave-4 rebuild, diff first (stop if anything unexpected moves). #12 not asked: lane L7's default stands (lists stay in code).

## Done
- D2 (#7) + D6 (c9488453c; RULINGS P48, D6): one salmon source (`salmonDayStatus` -> `salmonChainStatus` read the map runs
  `m`; 0 chain-days differed on r-483b295de57c); Python reads DFO rows tolerantly (`feed.type_rows`; `live.type_page` stays strict).
- D1 ponds (`flowing.fold_widening_ponds`, 16,119 ponds), D3 (`item_exit_down` + `nearer.NEARER_IGNORES_LOOPS`), D4
  (`reach.word_reads` + `regulations/word_reads.json`, 69 reads), D5 (`build.DFO_JOINING_WATER_STAYS_OUT`), #3 gauges
  (`place.nodes_at_cuts`): 9b0d6f4ae (+ f7f4fcfee), promoted with `full_t678b` (a5fe40636), released in r-0a9930fb8826;
  the 11 other-line gauge cuts decided (RULINGS P49, T682).
- Plan steps: #9 e1f1a268c, #2 8bc6eadbe, #4 b3a3938de, #14 9b0d6f4ae.
- Lane L7 2026-11-20 (each proved no output change): #10 one licensing pass (ece8f7a29); #11 in-season ranges to
  `data/generated/inseason/resolved.json` (7774bcbab); #12 rulings to `regulations/under_closure_calls.json` (a493cf12d,
  59e9589cc); #13 `waters/added_stream_calls.json` (61f2bd774); #15 `atlas.sidecars` stays as the supported label-only
  path, must equal the build's (5339c18ef); #18 dead `_in_bc` / "dropped" count gone (94842bada).
- Lane L8: #17 `export.read.group_runs` / `grp_runs` (`ui-rules-answers.json` byte-identical); #8 `section_own` /
  `flow-own/1` (`packed.SHIP_OWN_GAUGE_FLAG` ON, ships with the next data release; app 0.8.4 reads it; list
  `T679-shots/own-gauge-moves.csv`), then "Named lakes only" (`panels.own_distances` walks through an ownerless LAKE
  polygon, `panels.ownerless_pond`; fe5dbbe7d): moved pairs 611 (118 gauges, 78 waters), lead sets changed 0; #16 paint/6
  `SectionAccess.mostly`, on the T686 switch (`packed.SHIP_PACKED_FORMATS`; measured ON: 3,597 sections, +282 B gz).
- Test hygiene (lane LF, 0043ad017): `test_export_codec.py` big-export compare uses `_same`.

## Left
Findings still open (from the audit table: # | What | Where | Runs in | Belongs in | Risk):
| 1 | "Outside B.C." has two rules. The reach counts a section as outside when it has `out_of_bc` OR lies in no region polygon. The tiles drop only `out_of_bc`, and the tributary walk reads only `out_of_bc`. Border slivers are drawn and tappable, carry no rules, and a tap says "outside B.C.". They are also missing from `undrawn.json`, so `bundle/undrawn.py` cannot check them. | `atlas/reach/outside.py:48-69`; `deliver/tiles/export.py:244`; `atlas/graph/tributaries.py:988` | reach + tiles | atlas: one flag, stamped at the border cut (`splits/border.py`) | wrong / disagree |
| 6 | A section of no registry water still gets a name. The tiles label it from the graph node, the bundle's span words do the same, and gazetted wetlands in no item are kept. The map names water that opens as unnamed when tapped. | `deliver/tiles/export.py:138-140, 576`; `deliver/bundle/spans.py:340, 431` | tiles + bundle | registry: every named node is in an item, or carries no name | disagree |
| 18 | Small duplicates. `continuation.py` holds a topology fact, with a watershed-code check that works around added-stream orientation; it belongs in the atlas. Places are de-duplicated in tiles (3 dp) but not in the bundle (4 dp), and `build.py:413` prints a dead "dropped" count. A part has two "where is it" points (`part_geo.mid_sid` vs `release/part_line.py`). `position.nearest_pieces` returns `{}` when `attach` was not called (no caller does that today), and should raise. | `deliver/bundle/continuation.py:31, 140`; `tiles/export.py:968-972`; `bundle/build.py:378, 413`; `bundle/topology.py`; `atlas/reach/position.py:30-60` | various | as said | maint |
| 12 | The export keeps entry-id lists because the bundle lacks the fields: `UNDRAWN_PART_TRIBUTARIES`, `EXCEPTED_RIVERS_CREEKS_CLOSED`. Owner rulings sit in code: `RELEASE_UNDER_CLOSURE_KNOWN`, `QUOTA_UNDER_CLOSURE_KNOWN`. Tests re-derive them from the corpus. | `deliver/export/closures.py:116…, 658, 666` | export | the bundle carries `tributary_excludes` and a rule's tributary scope; the rulings move to curated data | maint |

- #1 built behind `border.ONE_OUTSIDE_FLAG` (off; `mark_regionless` in `build_registry`): live atlas (probe 2026-11-20)
  1,019 regionless nodes, 203 of them not yet `out_of_bc` (lake 71, stream 58, wetland 74) — what the flip moves outside.
- #6 built behind `registry.build.NAMED_NODES_ARE_IN_ITEMS` (off; `clear_unheld_names`): live atlas 42 named nodes in no
  water (stream 38, lake 4; e.g. Serpentine River, Salmon River, Katzie Slough, Kootenay River pieces) — check those in
  the wave-4 diff before flipping. Both measured on every build in `step10_report.json`.
- #15's full-data run waits for the wave-4 atlas.
- #18 left for wave 4 (output changes, L8's files): continuation's topology into the atlas (`bundle/continuation.links` +
  `goes_on` as an atlas sidecar, the bundle keeping the book pairs and the owner gate), one place-dedupe precision (tiles
  3 dp vs bundle 4 dp), one "where is it" point per part (`part_geo.mid_sid` vs `release/part_line.py`).
- #12 LEFT: `UNDRAWN_PART_TRIBUTARIES` / `EXCEPTED_RIVERS_CREEKS_CLOSED` need the bundle to carry a rule's own tributary
  scope (it drops `includes_tributaries: false`) and `tributary_excludes` (kept out on purpose, `bundle/rules/row.py:84`)
  — the owner, 2026-11-27: derive from data, in the 0.10.0 re-cut (## Owner).
- Touches tidal (for T678, not planned here): the reach applies outside and not-fishing first, then tidal
  (`reach/build.py:455-462`); `bundle/access.py` measures against tile geometry, which drops tidal pieces.

### Wave 4 must run (one rebuild)
1. Flip `border.ONE_OUTSIDE_FLAG` and `registry.build.NAMED_NODES_ARE_IN_ITEMS` to True (their tests in
   `test_t679_step10.py` flip with them) — or leave either off on the owner's word.
2. The side atlas build (heavy.sh), then `python -m pipeline.atlas.sidecars --build <side> --check <new dir>` (#15: must
   print "the two writers agree") and `ATLAS_BUILD=<side> pytest -m slow pipeline/tests/test_sidecars_agree.py`.
3. `build_parity.py` live → side, reach on the side atlas, `binding_diff` against the live reach run; attribute every
   change to #1 (sections newly outside: the regionless count in `step10_report.json`), #6 (labels only), T926 / T780 and
   the curated edits since the live atlas (tidal_limits.json, name_variants.json are ahead of it).
4. Tiles: the drawn set against `undrawn.json` (#1 drops the regionless slivers from the stream layer).

Depends: T678 (share its atlas build). Size: L (split per plan step when started). Owner: D2–D6.

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
owner 2026-11-11 "look for other things like that too"; D1–D6 + gauge placement released (r-0a9930fb8826); other-line gauge cuts decided (P49/T682); L7 2026-11-20: #10/#11/#12 rulings/#13/#15 done, #1/#6 behind switches, all no output change; L8: #17 built on lane/l8-shrink (answers byte-identical), #8 built + "Named lakes only" (lane/l8-blockers), SHIP_OWN_GAUGE_FLAG ON (owner 10:45 "Ship it"; ships with the next data release), #16 paint/6 built on the T686 switch; owner: see ## Owner; left (Claude): wave 4 flips #1/#6 with the rebuild, #18 rest

## Owner

- [x] 2026-11-20 — Should the bundle carry each rule's own tributary scope and the designations' `tributary_excludes` (yes lets the export derive `UNDRAWN_PART_TRIBUTARIES` and `EXCEPTED_RIVERS_CREEKS_CLOSED` instead of two id lists in code; it changes the bundle and export output)? Default: leave the two lists in code. → "what lists? i generally would say to derive from data but i dont understand what must carry tributary scope implies"; after the explanation: "derive from data but in 0.10" (2026-11-27). Supersedes the 11-20 default (lists stay in code); on the 0.10.0 list (the re-cut chat).
