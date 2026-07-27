# 11 — Implementation Plan (ordered, de-risk first)

Build order is chosen so the **riskiest assumptions are proven before code depends on them**,
and so each step is testable in isolation (`10`). Everything lands in `pipeline/sections/`
(scaffolded) and outputs to `output/pipeline/v2/`; the legacy pipeline keeps running until
cutover.

## Phase 0 — Spikes ✅ DONE (see `10` for numbers)

Outcome: **keep both the WSC-hierarchy filter and the 2300 barrier.**
- **S1**: SCC condensation is a no-op (FWA is a DAG); the WSC-descendant filter stops the
  Chehalis→Harrison leak. Keep excluded-WSC logic (reframed as WSC-subtree).
- **S2**: lake-collapse does NOT sever the Kootenay↔Columbia canal (bypasses the lake); keep
  the `EDGE_TYPE=2300` barrier.
- **S3/S4**: BLK-atom sections + lake-cut are de-risked; handle the 0.69% all-2-point BLKs
  with interpolation in `cutting.py`.

Remaining spike work during build: paste the exact leak numbers into the two
`test_graph.py` regression fixtures and un-skip them once the guarded walk (`tributaries.py`)
exists.

## Phase 1 — blk-chains + names (`blk_chains.py`, `names.py`, `cutting.py`) ✅ DONE

Implemented + validated (Adams River: 100% fid coverage, name tuples correct; 15 tests).
upstream-inherited names deferred (needs the graph). Serialization is pickle for now
(GeoParquet later). Original plan below for reference.

### Phase 1 (original)

- Merge streams by BLK via `FWADataAccessor`; route spans; `WaterbodyRun`s; aggregates.
- `(name, source)` tuples (port `propagate_names_by_watershed` + `annotate_unnamed_context`,
  tagged).
- `cutting.endpoint_id` + `substring_cut` + `section_id` helpers.
- Tests: `test_blk_chains`, `test_names`, `test_cutting` (substring + id stability).
- **Verify:** run on one watershed group; eyeball a dozen BLKs; confirm S4 structural numbers.

## Phase 2 — the inverted graph, lakes as nodes, guards, geometry sidecar (`graph.py`) ✅ DONE

The **single inverted graph**: node = a stream **piece** (a BLK cut at its lake-runs) or a
**lake** (one per `wbk`); edge = "flows into" (`confluence`/`lake_in`/`lake_out`) at a
confluence measure; `ancestors(guarded=True)` = tributary closure.

- **Lakes as nodes, in the combine** (from the fid `wbk`-run; only `lakes∪manmade`, NOT
  wetlands). A BLK through a lake → below-lake piece + lake node + above-lake piece, so the
  **up/down-of-lake sections come for free** (Adams). A lake's `up_adj`=inlets, `down_adj`=
  outlet(s); `export_lake_io` just reads adjacency. Lake name = `GNIS_NAME_1` else a threading
  river; `through_names` metadata records the threading rivers.
- **Guards at build:** WSC-descendant filter on stream→stream edges (only 7/8243 Adams edges
  dropped, all mis-picks); distinct `EDGE_TYPE`s preserved so a 2300 piece is `is_barrier` and
  `ancestors(guarded=True)` stops at it. (With lakes as nodes the Columbia canal drains into a
  lake node → the 2300 barrier is the operative guard there.)
- **Geometry off the graph:** `build_section_geometries` writes a `node_id → geom` sidecar;
  `StreamNode` holds no geometry (adds `through_names`).
- Validated on the Adams extent: **10821 nodes (9254 pieces + 1567 lakes) / 10929 edges**,
  integrity OK; Adams Lake outlet = Lower Adams, inlet = Upper Adams (~600 BLKs split, max 8).
- Tests ✅: mainstem-one-node, lake split, no-wetland-split, WSC filter, 2300 barrier, plus the
  two real-data regressions (Chehalis/Harrison, Columbia/Kootenay), un-skipped and green.

## Phase 3 — curated sectionizer (`anchors.py`, `sectionizer.py`) — NON-lake only ✅ CORE DONE

Lakes are already split (Phase 2); this handles only authored boundaries (see `04`). **Applied
to the graph BEFORE any tributary walk** so a curated section is a first-class node — that is
what makes "tributaries of X between A and B" expressible (see Phase 4).

- `anchors.resolve_split_defs` — `point`/`line` anchors → `SplitPoint(blk, route_measure)` on
  the target `blk`/`gnis`/`wsc` blue line(s), proximity-gated. (`lake`/`mu_boundary`/
  `confluence` anchors still to add — they need lake/MU/graph context.)
- `sectionizer.split_graph_at` — subdivide the piece node containing each split measure into
  P_low (keeps id) + P_high (`"{blk}:{int(M)}"`), rewire incoming tributary edges by
  `at_measure`, add a `continuation` edge P_high→P_low, split the sidecar geometry, and set
  `location_identifier` from the two bounds (04 table). `build.py` runs this right after the
  graph build, before persisting.
- ✅ Validated: synthetic `test_sectionizer` (section nodes + labels + `between`), and on real
  Adams geometry a mid-fid `point` cut splits 11705 m → 5853 + 5853 with a continuation edge.
- **Remaining:** `location_identifier` uniqueness-within-gnis validation; lake/confluence/
  mu_boundary anchor resolvers; author a real `splits.json`.

## Phase 4 — tributaries roll-up (`tributaries.py`) ✅ CORE DONE

The flow guards already live in the graph (`ancestors(guarded=True)`). This layer rolls the
guarded ancestor set up to sections:
- `tributary_node_ids(graph, node)` — the guarded closure of a section.
- `tributaries_between(graph, section)` — for "tributaries of X **between A and B**": the
  ancestors of the A–B section MINUS the upstream mainstem entering at its upper bound (its
  incoming `continuation`/`lake_out` edge and that subtree). Leaves only the side tributaries
  joining inside the reach. Depends on Phase 3 having made A–B a node.
- ✅ Validated in `test_sectionizer` (Mid Creek only, not the upper mainstem/Up Creek).
- **Remaining:** roll node ids → section ids at the `match` layer; seed-set cache; golden
  parity vs legacy on the known rivers.

## Phase 5 — match (reuse `matching/` + new `match` step)

- Resolve regs → sections (simple gnis/name + location; complex overrides → section_id).
- Retire duplicate-override split rows (Adams) in favor of splits + section targets.
- Tributary expansion via `tributary_section_ids`; `effective_includes_tributaries` ported.
- **MU overlay:** emit `mu_id → base_reg_set` (07); sections stay single-reg-set.
- Tests: `test_section_regs`; retarget `test_feature_resolver_dispatch`,
  `test_tributary_override`.
- **Verify:** golden parity of reg assignments vs legacy.

## Phase 6 — bundle + tiles + client (`06`)

- Section geometry → PMTiles keyed by `section_id`; `under_lake_sections`; lakes by wbk.
- Tiny `boot.json` (search + labels); lazy `reg_sets`/`regulations`/`mu_overlay`.
- Highlight by `section_id` feature-state → drop the fid lists + `/api/resolve` fid chain.
- Rebuild mobile SQLite on the `section_id` spine (drop `fids`/`polys` tables).
- Tests: retarget sharder tests to section shards; payload-size assertion (boot ≪ old tier0).
- **Verify:** webapp against `v2/` behind a flag; measure boot payload delta.

## Phase 7 — validate in parallel + cut over

- Full-build sanity gates (`10`); diff counts/coverage vs prod; client smoke on hard cases.
- Flip `deploy` target / bump `SECTION_VERSION`. Rollback = previous version on R2.
- Retire legacy graph/atlas/enrich stream path once stable; `graphify update .`.

## Next concrete steps (Phases 0–4 core done)

1. **Finish Phase 3:** remaining anchor resolvers (`lake`/`confluence`/`mu_boundary`),
   `location_identifier` uniqueness validation, author a real `splits.json`.
2. **Phase 5 — match onto sections** (+ MU overlay): resolve regs → section ids using name +
   `location_identifier`; tributary expansion via `tributaries_between`/`tributary_node_ids`.
3. **Phase 6/7** — bundle/tiles/client on the `section_id` spine; validate + cut over.

Code in place: `stream_sections/` — blk-chains + names + inverted graph **with lakes as nodes,
both guards, geometry sidecar** (Phases 1–2); **curated sectionizer + tributary roll-up incl.
`between`** (Phases 3–4 core). Tests in `stream_sections/tests/` (28 pass).
