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

## Phase 3 — curated sectionizer (`anchors.py`, `splits.py`, `sectionizer.py`) — NON-lake only

Lakes are already split (Phase 2), so this handles only authored boundaries (see `04`).
- Author `splits.json` (falls/bridges/MU boundaries); anchor resolvers → `SplitPoint` →
  `splits.resolved.json`; cut a stream-piece node at curated splits → `Section`s with cut
  geometry, bounds, `location_identifier`, minzoom. Coverage + uniqueness validation.
- Tests: `test_splits` (anchor → `(blk, measure)`), `test_sectionizer` (label table, coverage).
- **Verify:** Adams already shows Lower Adams | Adams Lake | Upper Adams (Phase 2); a curated
  split (e.g. a falls) adds a further boundary within a piece.

## Phase 4 — tributaries roll-up (`tributaries.py`)

- The flow guards already live in the graph (`ancestors(guarded=True)`). This step only
  aggregates the guarded ancestor node set to **section** ids and caches seed sets.
- Tests: `test_tributaries` (`tributary_only` excludes the named section; cache).
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

## Next concrete steps (Phases 0–2 done)

1. **Phase 3 — curated sectionizer:** implement `anchors.py` (resolve a cut line/polygon ∩
   channels → `SplitPoint`, proximity-gated) + `sectionizer.py` (cut a stream-piece node,
   auto `location_identifier`). Author a seed `splits.json` (a falls/bridge fixture) — NOT a
   lake split (lakes are Phase 2).
2. **Phase 4 — tributary roll-up:** aggregate `ancestors(guarded=True)` to section ids + cache.
3. Then Phase 5 (match onto sections + MU overlay).

Code in place: `stream_sections/` — blk-chains + names + the inverted graph **with lakes as
nodes, both guards, and the geometry sidecar** (Phases 1–2 done); `anchors`/`splits`/
`sectionizer`/`tributaries` stubbed. Tests in `stream_sections/tests/` (23 pass).
