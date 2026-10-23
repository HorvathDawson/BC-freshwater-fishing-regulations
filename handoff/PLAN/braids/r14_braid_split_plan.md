# R14 item 7: the uniform braid split in the build step (plan)

2026-10-16. Research only: nothing in the pipeline, curated data or live outputs changed. The data is
atlas/full (2026-10-10), the same vintage as the live bundle (handle digest d768d6e135e62b99).

- Scripts and JSON: `handoff/PLAN/r14_braids/` (`extract.py` is the heavy step; then `census.py`,
  `canals.py`, `build_page.py`).
- Review page: `scratchpad/braid-review/index.html`.

Builds on r13_braids_uniform.md. Everything there stands unless this file says otherwise.

## The rule (owner, 2026-10-16)

> Any piece that attaches to both sides of a split is cut along the median tangent.

Made exact:

1. **Cut line.** The perpendicular at the cut point to the host chord from M−100 m to M+100 m (the
   "median tangent"). It is clipped at its first re-crossing of the host line, so it never reaches across
   a meander.
2. **Attaches to both sides.** A piece qualifies when it lies on a simple path, through non-host lines,
   from a host piece below M to a host piece above M. This is computed as the biconnected block of a
   virtual LO–HI edge, so dangling tributaries of a braid never qualify. A lake ON the host line counts
   as host.
3. **Only the host's own water is cut.** The piece must be in the host's registry item. A straddling
   network that is another water's (Stamp/Ash, Alouette 216th, Kootenay CPR, Duncan/Lardeau, Mitchell,
   Babine, Young Creek, Chehalis: 8 curated cases) is left alone, because the host's rules never reach it.
4. **Where a qualifying piece is cut.** At each crossing of the line that lies ≥ 10 m from the piece's
   ends.
   - **Multi-crossings** on one piece: cut at the crossing nearest the cut point only. A second crossing
     on the same piece means a bulge, and one cut already separates the sides.
   - **Network** (several qualifying pieces crossed): cut each one once. Uncrossed members settle by
     topology.
5. **Junction at the cut, or the line misses** (no crossing, or only within 10 m of an end): no cut. The
   junction edge is re-homed to the host piece on the channel's side, decided by the channel's END
   GEOMETRY projected on the host, never by FWA edge x/y. Four curated cases have FWA edge coordinates
   0.5–2 km off.
6. **One-sided networks are never cut**, even when the line crosses them (bends, bulges, the Serpentine
   ditches). This is the topology gate from r13. Without it the geometry cuts across bends (South
   Atnarko 240665, Campbell).
7. **Edge-consistency pass**, in the same module, after the cut. For every edge touching a cut piece:
   - the `to` end is placed by `at_measure`;
   - the `from` end is re-homed to the piece on the same side.
   r13 sim: 0 leaks with this pass, 14 unplaced pieces without it.

**Scope.** The rule runs on curated and gauge cuts. Automatic length cuts are exempt for now:

- 325 of them have a straddler.
- No rule binds any of them.
- 33 of them need a line longer than 800 m.

Running the rule there would add about 265 sections and change no answer (open question Q2).

## Census (`r14_braids/census.py`, wider than r13)

r13 only tested lines carrying the host's watershed code. This census walks EVERY connected line (any
name, any code) within 3 km of every cut. It found **no curated braid that r13 missed**.

| outcome | curated | gauge | length |
|---|---|---|---|
| cut by the rule | 19 | 6 | 265 |
| junction at the cut, or the line misses (re-home) | 6 | 7 | 60 |
| one-sided, the line crosses it (not cut) | 12 | 7 | 291 |
| another water's (not cut) | 8 | 3 | 70 |

**Curated risk:**

- **8 high** (a closure moves ≥ 300 m): Peace Halfway, Adams ×2, Chowade ×2, Skeena Cedarvale, Morice
  Lamprey, Atnarko South Atnarko.
- **7 medium**.
- **4 low**.
- **6 not bound yet**: Pend-d'Oreille Seven Mile, Squamish/Ashlu, Kitsumkalum +1.5 km, Exchamsiks, and
  the Peace 1,200 m signs ×2. The Peace dam-zone rows bind the entry scope, not these split ids, so the
  page shows only r1–r4 on them.

**Degenerate cases flagged on the page:**

- `two_cuts` (7): Adams, Peace. The middle piece takes the window.
- `edge_anomaly` (4): Salmon/Kay, South Atnarko, Squamish/Ashlu, Kitsumkalum.
- `far` (3): Peace 5110 at 982 m, Exchamsiks, Peace Halfway.
- `junction_at_cut` (2): Atnarko campsite, Zymoetz upper canyon.
- `sliver` (1).

**Gauge cuts:** 12 of 13 are already cut today by the anchors sweep at ±50 m. At ±100 m those cut points
move a few metres to tens of metres, so their section ids change.

## Serpentine (owner asked)

**The 168th St "loops" exist in FWA, and they are drainage ditches.** blk 355995609 (2,844 m) and
355996889 (1,435 m) are FWA EDGE_TYPE **2300 "Manmade – Secondary Flow"**, joined to the river by 1450
flow connectors. They are part of a rectangular ditch grid, as the page's map shows.

- Each ditch touches the river on ONE side of 168th St, so the rule does not cut them.
- They are in the Serpentine item only because FWA gives them the river's watershed code.

**The canal (manmade 329292019, 4.24 ha, 2.36 km × 18 m)** is a straight cut across a meander, fed and
drained by the Serpentine.

- It belongs to no water.
- The river carries a lake boundary onto it, so r2/r3 stop at it. Only zone rules apply there.

**Two tiny unnamed lakes also cut the Serpentine:**

- 329293584, on the main line at 26.45 km.
- 329292861, on a ditch.

## Canals: every polygon that carries a river's own flow (`r14_braids/canals.py`)

21,784 lake nodes are fed and drained by the same stream item (directly, or through ≤ 2 unnamed lines).

- **Manmade:** 60 unnamed polygons. **Only the Serpentine canal is a channel** (×131 longer than wide;
  the next is ×11.5). The Vedder Canal and the Sumas Lake Canal are already stream items (AGENTS 55).
- **Lakes layer:** 18,460 unnamed polygons sit on main lines, side channels or loops. 339 of them are
  ≤ 30 m wide and ≥ 20× longer than wide (74 at ≥ 40×). These are river reaches FWA drew as lakes.

**Proposed rule (A, do it):** an UNNAMED FWA **manmade** polygon fed and drained by ONE stream item
(≤ 2 unnamed connectors), with length/width ≥ 20 (L = perimeter/2, W = 2·area/perimeter), joins that
item.

- Implement it in `registry/flowing.py` as a precedence-6 join. It works like rule 1 for the Vedder
  Canal, and the F6 lake-boundary drop applies.
- It picks up exactly 1 polygon today.
- It never folds a NAMED lake. That keeps lake-must-not-answer-to-its-river intact: that rule is about a
  real lake's NAME, and this one is about an unnamed polygon's FLOW. A fold is decided by structure and
  needs no name match.

**Option B (owner decides):** extend the same rule to the 339 narrow unnamed lakes-layer polygons. This
reverses the stated position (flowing.py: "16,053 unnamed lakes are threaded by a named creek, and those
are lakes") for the river-shaped ones only. It needs a sample review first: the page shows 130 maps.

## Build change

**New: `pipeline/atlas/splits/braid_sweep.py`.** It runs between `resolve_split_defs` and
`split_graph_at` (build.py ~748).

- Input: resolved curated and gauge cuts, the graph, geometries, and the registry membership by blk/wsc
  that the gate needs. The item is not built yet at this point, so membership is the host WSC plus the
  blk refs the item builder will use, computed the same way.
- Output: extra `SplitPoint`s carrying the cut's OWN split id and `concern: "braid cut"`, plus the
  junction re-homes.
- The edge-consistency pass runs right after `split_graph_at`.
- Pure geometry and topology, sorted iteration, no clocks.

**Goes:**

- The anchors sweep (`perpendicular_cut`, `_spans`, the ±50 m window).
- E15 in full: `extent.STRADDLING_BRAIDS_TAKE_THEIR_MAJORITY_SIDE`, `_majority_side`, the `marks`
  plumbing, `straddle_placed`, `position.share_within`, `SHARE_STEP_M`, io `STRADDLE_TABLE`, bundle
  `section_straddle` / `rules.straddle_crossing` / `read.straddles`.
- `test_reach_straddling_braids.py` is rewritten.
- This is the r13 §5 list, unchanged.

**Comes, in the reach:**

- One generic rule in `_by_measure`: a cut is a boundary on every line it crosses, and adjacency across
  a window cut is not adjacency.
- Gate: `straddles_the_cut` = 0, or the build fails and names the pieces.
- `POLICY_VERSION` bump. A RULINGS E-row supersedes E15, and AGENTS 12 is updated.
- Plus Ash r1 (Elsie Lake second outlet): the generic "a channel draining the window's cut lake is on
  its downstream side" rule.

## Tests (no-data fixtures, synthetic lines)

1. One braid across one cut: 2 pieces, exact sides.
2. One braid across both cuts of a `between`: 3 pieces, the middle one inside.
3. Network with an uncrossed partner: the partner follows by topology.
4. Junction ≤ 10 m from the cut: no cut, junction re-homed, no sliver.
5. Bent host: the line crosses a one-sided network, and nothing is cut.
6. Bulge (Serpentine-shaped): no cut.
7. Edge whose x/y is 1 km from the channel end: re-homed by end geometry.
8. Straddling network of ANOTHER item: not cut.
9. Line re-crossing the host before the braid: clipped, braid not cut.
10. Multi-crossing on one straddling piece: cut once, at the nearest crossing.
11. Shuffled chain order: identical `SplitPoint`s.
12. Reach gate: an uncut straddler fails the build.

**Mutation pins:**

- Gate off → test 5 fails.
- Edge pass off → test 1 leaks.
- The item check off → test 8 fails.

**Slow, on the real atlas:** pin the 19 curated cut cases' piece sides, and the 8 high rows' metres
from the page.

## Rollout (one change set, after the owner says go)

1. Write the module and fixtures. The fixtures must fail on HEAD.
2. Remove E15. Add the reach rule, the gate and the POLICY bump. Add the canal fold (rule A).
3. Side atlas build (heavy.sh, ~21 min) to `atlas/r14`. Parity against live, minus the old-vs-old noise
   set. Expected:
   - +19 curated braid pieces;
   - ±10 gauge braid ids;
   - ~13 junction re-homes;
   - the Serpentine canal joins `gnis:24978`;
   - nothing else.
4. Side reach run. Diff the per-rule bound sets against live (split-fix-changes-trib-walk). Every change
   must be:
   - a row on the page's rule tables;
   - a tributary of a re-cut braid half; or
   - the canal gaining Serpentine r2/r3/r4.
   Re-check `between` / `upstream_of` through end cuts (end-cut-halves).
5. Curation:
   - Exchamsiks host blk;
   - accept or curate Ash.
6. Bundle, tiles (human-run, ~1 h), answers and the page snapshot, all from the same atlas.
7. Re-snap the page and diff its states. Only Peace, Adams, Chowade, Dean, Morice, Skeena, Bulkley,
   Chuckwalla, Thompson, Zymoetz, Atnarko/Bella Coola and Serpentine may move.
8. Promote together.

**Effort:** about 2 days of code and tests, plus about half a day of side builds and diffs (one atlas
build, one reach run). Tiles take 1 hour and are human-run.

## Open questions for the owner

1. **Serpentine ditches.** The 2300 ditches (and similar manmade-secondary lines elsewhere) are in a
   river item only because they share its watershed code. Keep them as the river (they take their
   junction side today, the rule leaves them whole), or drop EDGE_TYPE 2300-only lines from stream items
   so they carry zone rules only?
2. **Automatic length cuts.** Should the rule run on them too: about 265 extra sections, no answer
   changes, and 33 lines over 800 m?
3. **Option B.** Fold the 339 river-shaped unnamed lakes into their rivers? This includes the
   Serpentine's two tiny lakes.
4. **Long lines.** Accept a cut line drawn up to about 1 km across the floodplain (Peace 5110 at 982 m,
   Exchamsiks 1.75 km)? Or cap it, e.g. at 1,000 m, with anything beyond it re-homed instead of cut?
