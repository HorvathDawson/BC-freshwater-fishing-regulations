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

## Phase 2 — the inverted graph (`graph.py`) ✅ DONE (constructed + validated)

Replaced the fine per-confluence segment graph with the **single inverted graph**: node = a
stream (BLK), edge = "flows into" at a confluence measure; `ancestors()` = tributary closure.
Validated on the Adams extent (8393 nodes / 8243 edges, integrity OK; Adams = 1 node / 435
tributaries). Node carries `fwa_watershed_code` + member fids so the Phase-4 walk can apply
the **WSC-descendant filter** and the **2300 barrier** (both kept per Phase 0).

Remaining: Phase 3 splits BLK nodes into section nodes (lakes + curated splits) and promotes
lakes to nodes; the two real-data regression tests need the guarded walk.
- Tests: `test_graph` (mainstem = one node; tributary flows-into at measure; ancestors); the
  two regressions skipped until the guarded walk exists.

## Phase 3 — splits + sectionizer (`anchors.py`, `splits.py`, `sectionizer.py`)

- Author an initial `pipeline/matching/splits.json` (start with Adams lake split + Wigwam fid
  split as fixtures); anchor resolvers → `SplitPoint`; write `splits.resolved.json`.
- Cut BLK chains at lakes + splits → `Section`s with cut geometry, bounds,
  `location_identifier`, per-section minzoom. Coverage + uniqueness validation.
- Tests: `test_splits`, `test_sectionizer` (Adams two-section, label table, coverage).
- **Verify:** Adams shows the two real sections; Wigwam splits at the divide.

## Phase 4 — tributaries (`tributaries.py`)

- Guarded ancestor walk over `StreamGraph` (`graph.ancestors` + WSC-descendant filter + 2300
  barrier + lake barrier) → `tributary_node_ids`; seed-set cache.
- Tests: `test_tributaries` (lake barrier stop, tributary_only, no backtracking, cache).
- **Verify:** golden parity of tributary sets vs legacy on the known rivers (minus corrected
  leaks).

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

## First concrete steps (right now)

1. **Run Phase 0 spikes**, paste results into `test_graph.py` + `10`'s spike table.
2. If green: implement `cutting.py` + `blk_chains.py` (Phase 1), un-skip their tests.
3. Author the seed `splits.json` (Adams, Wigwam) so Phase 3 has fixtures early.

Code in place: `stream_sections/` (S1–S2 + the inverted graph implemented; splits/sectionizer/
tributaries stubbed) and `stream_sections/tests/`.
