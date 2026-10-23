# R13 item R: one uniform braid method in the BUILD step (research only)

2026-10-16. Nothing in the pipeline, curated data or live outputs was changed. Data comes from atlas/full
(2026-10-10), the same vintage as the live reach run and bundle. Scripts and JSON are in `r13_braids/`:

- `census_r13.py`: every curated and gauge cut × every side channel of the host's WSC.
- `sim_r13.py`: end to end on the R10 subgraph. It uses the real `split_graph_at` and the real
  `resolve_extent`.

## Verdict

**FULL SPLIT, gated by topology.** In one sentence: *the river network decides WHETHER a channel is split,
and the cut line decides WHERE.*

- Only a channel network that touches the main line on both sides of a cut is split.
- It is split where the ±100 m averaged-tangent perpendicular crosses it.
- A network that does not cross is re-attached whole to its side. This is the "simplify a few" case, and
  it is part of the same rule, not a second one.

Extents then need no braid logic. E15 (majority side, straddle marks) is deleted.

Simulated on all 68 R10 cases:

- **0 braid pieces are unplaced.**
- **0 marks**, down from 59.
- **Every danger row is fixed.**
- The changed metres match r12's tangent minority to within 2 m on every row.

## 1. What exists today (a finding that changes the cost)

`anchors.resolve_split_defs` **already sweeps a perpendicular across braids** (`perpendicular_cut`,
`_spans`, ±50 m, half-length 1.5 km). It has three gaps, and the 18 straddlers come from them:

| gap | effect |
|---|---|
| Point and gauge anchors only | Confluence and lake-offset cuts are never swept: Goat Creek, Lamprey Creek, Halfway ±5 km, McDonell Lake +3 km. |
| It tests only the split's **target scope** (the `gnis_id` for 367 of 409 curated splits) | Unnamed side channels have no GNIS, so they are never tested. Gauges are scoped by WSC, so they do sweep. Today the sweep has cut **42 channels: 41 at gauges and 1 curated** (Alouette 216th; that braid has since been pruned). |
| No topology check | **4 of today's gauge braid cuts are bend artifacts** on one-sided networks. One is at 08KG001, where the crossing projects 3.1 km from the gauge. |

So the full split is mostly **extending a mechanism that already exists**, not new machinery. `extent._cut_at`
already expects "SEVERAL BLUE LINES — the mainstem plus every side channel the perpendicular sweep cut".

## 2. The three uniform policies, end to end

| | (1) FULL SPLIT (topology-gated) | (2) FULL SIMPLIFICATION | (3) FULL ONE-BRACKET in the build |
|---|---|---|---|
| Rule | A straddling network: each member the line crosses is cut. A network the line misses has its junctions re-homed to its side. | Collapse every braid at a cut onto the main stem. | A straddling network: its minority-side junctions are re-homed to the majority side (E15's side, decided in the build). |
| What it really is | – | There are only three ways to collapse: delete the braid (rejected in E15, "never deleted"), snap the cut to a junction (r12 A1: moves book landmarks a median 363 m and up to 1.56 km), or merge into a main section (which needs a side at the cut, so it is (1) or (3)). **It is not a separate policy.** | (1)'s re-home pass applied to every network. |
| Build change | A new `splits/braid_sweep.py`. It runs between `resolve_split_defs` and `split_graph_at` (build.py ~748), and again after the cut for junctions. It replaces the anchors sweep. | – | The same module, re-home pass only. |
| Deterministic | Yes. It is pure geometry and topology with sorted iteration. The ~50-block atlas noise is unrelated. | – | Yes |
| Sections | **+20 curated** (19 bound, plus Pend-d'Oreille at Seven Mile, which is not yet bound) and **−4 gauge** (the bend artifacts). **Net +16** of 1.96 M. Today's 9 legitimate gauge braid cuts move by about 10–30 m (±50 → ±100), so 9 ids change. | – | 0 |
| Danger rows (8 braids) | **All fixed.** The cut is exact by the tangent, which never disagreed with projection on the majority side (r12, 0 of 38 flips). | – | **None fixed.** The minority stays wrong: about 6.6 km on closure rows, including 1.7 km of the Site C dam no-fishing zone reading open. |
| Reach code removed | E15 entirely (see §4) | – | E15 entirely |
| Reach code added | One generic rule: a cut is a boundary on every line it crosses. Pieces bounded by a window cut take its side, and adjacency across a window cut is not adjacency. About 30 lines, with no braid notion. | – | None |
| Tributary walk | Tributaries entering a braid's upper half now walk with that half. This is correct, but it is a binding change, so it must be diffed. | – | Junctions move up to about 2.5 km (Peace 5110). Tributaries of the braid then walk with the wrong side. |

**(2) collapses into (1) or (3), so the owner's "split + simplify a few" is (1).** The braids that (1) does
*not* cut (junction at the cut, missed by the line) are simplified, by the same rule, in the build step.

## 3. The rule's cases on real data (census of 806 bindable cuts × 457 channels; `classify.py`)

| case | bound | rows (curated / gauge) | handling under (1) | examples |
|---|---|---|---|---|
| Straddling network, a member is crossed | 18 braids | 21 / 9 | Cut each crossed member at the tangent crossing. Uncrossed members of the network settle by topology. | Adams ×2 cuts, Peace 5110/13560 (dam + 1,200 m signs: 3 pieces each), Chowade 6-channel network, Morice 5, Thompson 3, Dean 2 |
| Junction at the cut (crossing ≤10 m from the channel end, or no crossing) | 2 | 2 / 0 | **No cut.** The junction edge is re-homed to the host piece on the channel's side. | Atnarko 225339, Zymoetz 245483 (crossing 0 m from its end). r12's two false straddles. |
| FWA edge anomaly (an edge's coordinate lies 0.5–2 km from the channel end it joins) | 3 | 3 / 2 | Re-home by the channel's **end geometry** projected onto the host. | South Atnarko 224406, Salmon 354087999 (Kay Ck + 08HD007), Chowade 359002030, Fraser 08MF075, Kitsumkalum +1.5 km |
| Line crosses a **one-sided** network (a bend: the crossing projects 600–1,600 m along the host) | 0 rules affected | 8 / 4 | **Not cut** (the gate). Today's 4 gauge cuts of this kind are dropped. | Campbell ×4, South Atnarko 240665, Squamish 226040, Zymagotitz, Zymoetz at the lower entrance (1.58 km) |
| Bulge: both junctions on one side, the line crosses twice | 2 | 2 / 1 | Not cut. The loop is on its junctions' side, as today. | Serpentine at 168th St: 2,844 m (48 % geometrically above the street) and 1,435 m (40 % below) |
| Multi-crossing on a straddling channel (k ≥ 2) | 0 | 0 | Take the crossing nearest the cut point (the existing anchors rule). Pinned by a fixture. | – |
| Host is a side channel (curation) | 0 | 1 | Fix the split: Exchamsiks resolves onto blk 360216952. | – |
| Network bend in the line's choice (±50 vs ±100) | – | 4 rows differ, all gated out | ±100 m (r12: the best of ±50, ±100 and ±200) | – |

**Without the gate the sweep is wrong.** The pure-geometry variant of the simulation (`sim_r13_geom.json`)
does three things:

- It cut South Atnarko 240665 across a bend.
- That stranded a 310 m piece.
- It would also cut Campbell's two channels at both signs.

The topology gate is what makes the rule safe.

**Edges must be made consistent after the cut.** FWA wires braids with duplicate edges, some at the
channel's measure 0 or L. In the simulation, 8 edges (Adams, Chowade, Morice ×2, Peace ×2) joined a
braid's upper half to the main line below the cut. Fix, in the same build pass:

- An edge's **to** end is placed by `at_measure`, which is exact.
- Its **from** end is re-homed to the piece on that same side.

With this pass the simulation had 0 leaks. Without it, 14 pieces stayed unplaced.

## 4. Simulation (`sim_r13.py topo` → `sim_r13_topo.json`)

| variant | unplaced pieces (68 cases) | marks |
|---|---|---|
| today: atlas as is, E15 on | 3 (Noons ×2 and Seymour, sea-unjoined, which pre-date this) | 59 |
| split atlas, today's reach code unchanged | 3 | 83 (E15 "projects" the halves at share 1.0) |
| split atlas, E15 removed, no reach rule | **86** | 0 |
| **split atlas, E15 removed, plus the generic cut-on-every-line rule** | **3 + 1** | **0** |

The extra one is **Ash r1, 354088143:161**. It is a second outlet of Elsie Lake, and the cut there is the
lake itself (a lake cut, not a line cut). It needs a generic lake rule, not a braid rule: a channel draining
a lake that is the window's cut lies on the lake's downstream side.

Changed placements: 24 rule × braid rows. 6,147 m are gained and 5,906 m are lost. They match r12 §C:

| braid / rule | today in m → split in m | meaning |
|---|---|---|
| Peace 359012853, r5 Halfway closure (Mar–Jun) | 0 → 1,392 | 1.4 km now closed |
| Peace 5110 / 13560, entry scope (dam zone, downstream row) | 5,935 → 3,455 (+641 dam row); 13560 0 → 990 in the dam zone | the dam no-fishing zone is now closed |
| Adams r1 (Sep–Oct, platform window) | 0 → 717 | closed |
| Chowade r1 above the bridge | 043: 0 → 734; 635: 1,016 → 581 | the under-closure and the over-closure both fixed |
| Skeena winter closure r3 (Cedarvale) | 0 → 488 | closed |
| Morice r4/r5 (Lamprey) | 366: 2,400 → 1,954; 368: 0 → 78 | over-closure fixed |
| Dean r3/r9/r10 / r4 | +223 / 645 → 421 | – |
| Thompson r5, Atnarko r12, Chuckwalla r2, Bulkley r5, Zymoetz r1 | −186, −67, +102, +92, −33 | – |

## 5. Code that goes and code that comes

**Goes:**

- `extent.STRADDLING_BRAIDS_TAKE_THEIR_MAJORITY_SIDE`
- `_majority_side`
- the `marks` plumbing through `_by_measure` / `_half` / `_between_across_lines` / `resolve_extent`, and
  `_placed` / `straddle_placed`
- the `joins_a_straddling_braid` branch
- `position.share_within` and `SHARE_STEP_M` (`nearest_pieces` stays, for watershed parts)
- classify's `straddle_placed` diagnostic and build's `scope_straddle_placed`
- io `STRADDLE_TABLE` (`straddle.jsonl`, `straddle_digest`)
- bundle `section_straddle`, `rules.straddle_crossing`, `read.straddles` (never read by answers or the UI)
- `test_reach_straddling_braids.py` (rewritten) and its `test_bundle_licensing` case
- the anchors sweep (`perpendicular_cut` / `_spans` move into the new module)

**Comes:**

- `splits/braid_sweep.py`: the gate, the cut, junction re-homing and edge consistency.
- `SectionBoundary.reversed`: set where the channel is digitized against the host. None of the 19 gated
  cuts needs it; the only case, South Atnarko 240665, is gated out. It is kept for safety.
- The generic cut-on-every-line rule in `_by_measure`.
- A **build gate** in the reach: `straddles_the_cut` = 0, or the build fails and names the pieces.

`_flow_reach`, `_bracket` and the component fixpoint stay. They place tributaries and detached pieces (D5),
not braids. Cache POLICY bump. RULINGS E15 is superseded by a new E-row.

## 6. Tests (no-data CI fixtures, synthetic LineStrings)

1. Braid across one cut: 2 pieces, exact sides for up / down / between, 0 unplaced, 0 marks.
2. Braid across both cuts of a `between`: 3 pieces, the middle one inside.
3. Network with an uncrossed partner: the partner follows by topology.
4. Junction ≤10 m from the cut: no cut, junction re-homed, no sliver.
5. A bent host with the line crossing a one-sided network: no cut.
6. Bulge: no cut, the loop on its junctions' side.
7. A spurious FWA edge (the coordinate is far from the channel end): re-homed by end geometry.
8. A reversed channel.
9. Determinism: shuffled chain order gives identical SplitPoints.
10. The gate: an uncut straddler fails the build.

**Mutation pins** (memory: checks that launder): with walls off, test 1 must fail; with the gate off, test 5
must fail.

The slow real-atlas test pins the 18 braids' sides and the 8 danger rows from §4.

## 7. Migration (one change set, after the owner says go)

1. Write the module and fixtures. Fixtures fail on HEAD.
2. Delete E15 and add the cut-on-every-line rule, the reach gate and the POLICY bump.
3. **Side atlas build** (heavy.sh, about 21 min) into `data/generated/atlas/r13`. Check the parity diff
   against live **minus the old-vs-old noise set** (atlas not deterministic). Expected:

   - +20 / −4 braid sections
   - 9 gauge braid ids shifted
   - about 13 junction / edge re-homes
   - nothing else

4. **Side reach run** on the side atlas. Diff the per-rule bound sets against live (split-fix-changes-trib-walk).
   Every change must be one of the 24 rows in §4, or a tributary of a re-cut braid half. Also re-check
   `between` / `upstream_of` through cuts on line ends (end-cut-halves).
5. Fix the 2 curation items: Exchamsiks host, and the Ash lake-outlet rule (or accept Ash unplaced).
6. Rebuild bundle, tiles (human-run, about 1 h), answers and the page snapshot from the **same atlas**
   (tiles-bundle-same-vintage). Handles renumber with every atlas rebuild anyway, and the digests gate the
   pairing.
7. Re-snap the page and diff its states. Only sections on Peace, Adams, Chowade, Dean, Morice, Skeena,
   Bulkley, Chuckwalla, Thompson, Zymoetz and Atnarko / Bella Coola may move.
8. Promote everything together. Add the RULINGS row and the AGENTS 12 update.

**Owner decisions needed:**

- (a) Accept the invented line across the floodplain (up to 1 km on the Peace). The census shows it is
  safe only behind the topology gate.
- (b) Bulges stay on their junctions' side (Serpentine ×2), or they are cut too, which adds 4 sections and
  needs the `reversed` flag.
