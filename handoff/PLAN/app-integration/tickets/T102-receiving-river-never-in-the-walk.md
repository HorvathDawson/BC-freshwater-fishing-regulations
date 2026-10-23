# T102 — The receiving river is never walked into (parent-code check in the walk, not only in seeding)

Status: DONE — part B 3a68fb9f + d24171b8 (one entry check `may_enter`; reach rerun digest identical, binding diff vs reaches/full EMPTY). Part A done (95e78dd5: `THE_WALK_NEVER_ENTERS_A_PARENT` + one `_refusal` for walk and seeds; seed-only check removed; POLICY_VERSION 23) — reach rerun binding diff vs reaches/full EMPTY (0 rules, 0 unresolved, 0 licensing); REVIEWED (b44461d5: code_runs now takes the parent rule too; rerun digest identical, diff EMPTY); open: owner call on order guard for equal codes (see Owner questions). Raised 2026-10-22 from the owner's question on T90 ("I thought the tributary logic handled the Harrison,
using the WSC").

## Why
- The tributary walk (pipeline/atlas/graph/tributaries.py) keeps the receiving river out by STREAM ORDER
  (`_breaks_strahler`: "a bigger river cannot be a tributary"); the WSC there only OVERRIDES that in favour of a genuine
  tributary (line ~729). Nothing in the walk says "a water whose code is a strict ancestor of the reach's code is the
  river the reach drains into — never enter it".
- The receiving river's SIDE CHANNELS carry its code at a small order, so they pass the order test. T90 (ca9aa451)
  closed the one way they got in (mouth seeding) with `THE_RECEIVING_RIVER_IS_NEVER_A_SEED`, but "only seeding asks
  this; the walk itself is untouched".
- A braid that rejoins the receiving river can still let the walk climb into it the same way.

## Owner decision (2026-10-22)
"Shouldn't we only use the wsc check parent/child to handle it during the walk? no walking into a parent? to keep it
consistent and then remove your added check?" — Yes:
- ONE rule, in the walk: never step into a water whose watershed code is a strict ancestor of the reach's code. A seed is
  the walk's first step, so the same check covers seeding.
- REMOVE `THE_RECEIVING_RIVER_IS_NEVER_A_SEED` (T90's seeding-only check) — superseded.
- WSC decides parent/child wherever both codes are known; the Strahler/order guard stays only as the fallback where a
  code is missing (out of B.C., added streams with no code).
- KEEP `MOUTH_SEEDS_ONLY_AT_THE_CUT`: it fixes a different bug (at a natural mouth the old seeding matched SIBLING
  waters joining the receiving river at the same measure; a parent check does not stop siblings).
- Respect `code_runs` (Dewar Lake ruling: follow the code).

## Work
- Implement the rule above test-first; fixtures: a braid into the receiving river's side channel (blocked), a genuine
  tributary whose order looks too big (allowed by code), a no-code water (order fallback), the Chehalis/Harrison case
  (still blocked with the seed check removed). Mutation pins: drop the parent check → the side channel binds; put the
  order guard first again → the code-wins case fails.
- Reach rerun into a side dir + per-rule binding diff; list every rule that moves, with names and why, before promoting.

## Part B — one entry check, used everywhere (owner 2026-10-22)
"Then we could make the check logic (with the edge type check etc) a single function used in multiple spots."
- After part A lands: gather every acceptance rule the walk and the seeding apply — parent code (WSC), edge type,
  the order fallback, the entry's own water, barriers, lakes, code_runs, within-area — into ONE function
  (e.g. `may_enter(src, dst, reach) -> reason | None`) in tributaries.py, and call it at every step of the walk, at
  every seed, and from any other place that re-implements a subset (find them: the T57 sweep tool's checks, panels'
  tributary-ish walks, reach extent code). Each refusal returns a named reason, so diagnostics read one vocabulary.
- Pure refactor: the reach binding diff against part A's output must be EMPTY (proof of no behaviour change). A test
  asserts no other module re-implements the rules (a grep pin, like the one-pathway checks).

## Acceptance
Binding diff reviewed; 0 sections of a receiving river (by code) bound by any tributary walk; tests pinned.
Rebuild: reach + deliver, no atlas.

## As built — part B (2026-10-23, lane-T117)
- `tributaries.may_enter(ctx, src, dst, edge, via=FLOW|SEED|RUN|UP) -> reason | None` is the ONE place a
  walk's entry rules live, asked in this order: `carved_out` (blocked), `code_divide` (a code run's cut),
  `river_continuing` (at the reach's boundary: a mainstem edge above a stream, the through line / next
  lake of a boundary lake — or `river_passing_through` for a lake mid-reach, walked never collected),
  `own_water` (at the boundary), `not_in_graph`, `barrier`, then `_refusal`'s `receiving_river` (code) /
  `bigger_river` (order where the code cannot say). `REFUSALS` lists them. Its context is a `Reach`
  (`reach_context(graph, reach, blocked=, own=, guarded=)`: reach, codes, blocked, own, cuts, the lakes
  entered mid-reach, a per-lake through-line cache).
- `tributaries_of_reach` keeps only the books (seen / out / stack) and asks `may_enter` at every step
  (`FLOW`), every piece of a lake's code run (`RUN`: never judged by order) and every seed: `_mouths_at`
  keeps the seeding's own topology (`MOUTH_SEEDS_ONLY_AT_THE_CUT`) and asks `may_enter(via=SEED)` for the
  rest — so a seed now also passes the carve-out / own-water / barrier questions inside the one function
  (it did, in the walk's seed loop, before). One nuance: a seed's order guard now follows `guarded` like
  every step (it was always on); `guarded=False` exists only in tests.
- The T57 sweep (`tools/unnamed_trib_sweep.py`, d24171b8) no longer keeps its own `_breaks_strahler` /
  `_through_blks`: `explain` asks `may_enter` for the miss's one step through a graph view of its handle
  `Topo` (`TopoGraph`, a looser context: no carve-outs, own water or code divides), and `REASONS =
  tributaries.REFUSALS + the post-walk stops`. Reason names follow the walk: `strahler` → `bigger_river`,
  `through_lake` → `river_continuing`; `receiving_river` is new. The committed `t57_trib_sweep.json` is
  regenerated on the current bundle (see T117 As built).
- The other places that re-implemented a subset (`subtree`, the review-app picker, the QGIS layer, the
  gauge shed, the stem, the harness) are T117's list and are done there.
- Tests: `test_may_enter.py` — the whole verdict table, which questions each `via` asks, a lake
  mid-reach; mutations M1 (`may_enter` always None → the walk climbs everything: no rule left in it), M2
  (refuse SEED → the cut's creek goes), M3 (refuse RUN → the run piece goes), M4 (AST pin: no walk body in
  tributaries.py names a rule — `is_barrier`, `MAINSTEM_EDGE_KINDS`, `_refusal`, the lake helpers, the
  switches, `stream_order`, `wsc`), M5 (reason/order swapped → the table fails).
- Reach rerun (scratch, snapshot of 3a68fb9f): digest `81e726a3e6fbad3b` = reaches/full;
  `binding_diff`: 0 rules, 0 unresolved, 0 licensing changed. Pure refactor, POLICY_VERSION unchanged.
- Rebuild: none for part B on its own (no binding moves); ships with T117's reach + deliver.

## Adversarial review — part B (2026-10-23, lane-T117)
- Re-derived the old walk against `may_enter` question by question: the step, the boundary ("river
  continuing" / passing through), own water, the code run and the seed all ask what they asked before.
  Two deliberate differences, both harmless and stated: a seed now also meets `CODE_DIVIDE` (it never did),
  and a seed's order guard follows `guarded` (only tests pass False). Builder's T102B diff (empty) re-read.
- The defect found was in T117's use of `may_enter` (`subtree` refused a code run's divide but never took
  the run from its own lake) — fixed in a4b54933, reach rerun digest unchanged, see T117 "Adversarial
  review". Part B's own tests were mutated (no code divide, own water off the boundary, a run piece judged
  by order, the main-outlet tie-break) and each failed the named test.
- `REASONS == T.REFUSALS + (…)` in the sweep test pins the vocabulary, not behaviour; the behaviour pin is
  `test_each_stop_the_walk_makes_on_purpose_explains_a_miss_and_nothing_else_does` (now with
  `receiving_river`).

## Owner questions
- (from part A, still open) Equal watershed codes: the order guard decides (28,359 equal-code edges of
  the live graph it refuses are floodplain pieces). Default built: unchanged — order decides where WSC is
  silent.

## Left / follow-ups
- `build_panels.candidates` (the gauge-panel reading propagation) walks the graph with its own rules
  (area ratio, branch points); it propagates readings, it does not collect a tributary set, so it was
  left on its own rules. Say if it should step through `may_enter` too.
