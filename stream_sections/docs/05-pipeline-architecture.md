# 05 — New Pipeline Architecture (clean-slate)

Not a modification of the old flow — a from-scratch DAG of small, independently re-runnable
steps, each with a typed input/output artifact. **Build into a new folder
(`output/pipeline/v2/`); cut over by swapping the deploy target once validated.** No
in-place migration; the old pipeline keeps running until v2 is proven.

## Design principles

1. **One artifact per step, content-versioned.** Each step writes a typed file plus a small
   `*.meta.json` with an input hash. A step re-runs only if an input changed → cheap partial
   rebuilds (the user's "easier to run partial changes on updates").
2. **Geometry and regulations stay decoupled until the very end.** The section geometry
   build is regulation-free (like today's atlas); regs join by `section_id` at bundle time.
   This lets a regulation-only change (new synopsis) skip the entire geometry rebuild.
3. **`section_id` is the single spine.** Tiles, shards, mobile DB, search all key on it.
4. **Fail loud, validate per step** (uniqueness of `location_identifier`, no orphan
   under-lake segments, section coverage == BLK coverage).

## The DAG

```
                 ┌─ extract (PDF→rows) ─ parse (LLM→parsed regs) ─┐
 fetch (raw) ─┬──┤                                                 │
              │  └─ matching data (overrides, display names, splits)│
              │                                                     │
              ├─ blk-chains ─ graph ─ border ─ splits ─ tributaries ┼─ match ─ bundle ─ deploy
              │   (03 S1-2)   (S4-5,  (out-of- (section- (guarded    │  (08)    (06)
              │               lakes   BC flag) izer +    ancestors,  │
              │               split)           pickup)   EXCEPT)     │
              └─ overlays (zones, admin, towns, anglerinfo, hydro) ──┘
```

**Ordering note (important):** lakes split the BLK **inside** `graph` (from the fid `wbk`-runs);
`border` and curated `splits` run **after** the graph exists (they subdivide piece nodes), and
they run **before** any tributary walk so a section is a real node. The artifact chain is
`blk_chains.pkl → graph.pkl (+ geometries.pkl) → [border+splits mutate graph in place] →
tributaries/match`.

### Steps (geometry chain)

| Step | Reads | Writes | Notes |
|------|-------|--------|-------|
| `fetch` | remote | `data/*.gpkg` | unchanged from today |
| `blk-chains` | streams gpkg (via `FWADataAccessor`) | `v2/blk_chains.pkl` | 03 Step 1–2: merge by BLK, route measures, `(name,source)` tuples |
| `graph` | blk_chains (+ fid incidence) | `v2/graph.pkl` + `geometries.pkl` | 03 Step 4–5: inverted graph (nodes=stream pieces/lakes, edges=flows-into); **lakes split the BLK here**; geometry-free graph + `node_id→geom` sidecar |
| `border` | graph + WMU outline | (mutates `graph.pkl`) | split cross-border BLKs at the BC outline; flag `out_of_bc` pieces (geometry kept, not a barrier) |
| `splits` | graph + `splits.json` | (mutates `graph.pkl`) + `splits.resolved.json` | 04: resolve anchors → cut piece nodes; **proximity pickup**; `location_identifier`; `concern` |
| `overlays` | atlas polygons, zones, towns, anglerinfo/hydro dbs | `v2/overlays.pkl` | zone polygons, nearby-towns index, bathymetry/markers keyed to wbk/section |

### Steps (content chain) — reused, lightly adapted

| Step | Reads | Writes | Notes |
|------|-------|--------|-------|
| `extract` | synopsis PDF | `parsing/synopsis_raw_data.json` | unchanged |
| `parse` | raw rows | `parsing/synopsis_parsed.json` | unchanged (Gemini) |

### Convergence

| Step | Reads | Writes | Notes |
|------|-------|--------|-------|
| `match` | sections, parsed regs, overrides, splits, overlays | `v2/section_regs.pkl` | 08: resolve regs→sections (simple/complex), zone_reg_map, base/provincial regs, tributary reg propagation over the section graph |
| `bundle` | section_regs + section_geom | `v2/deploy/*` | 06: `section_id` PMTiles, tiny search bootstrap, lazy reg chunks, mobile SQLite |
| `deploy` | bundle | R2 | swap `SHARD_VERSION`/path once validated |

## Why this is faster / simpler than today

- The FWA micro-fids are consumed once (`blk-chains`/`graph`) to derive the small inverted
  graph, then never touched again. Tributary reachability is precomputed **per
  section** and reused across all regs — no per-reg BFS over the giant graph.
- A regulation-text change re-runs only `parse → match → bundle → deploy`; geometry steps are
  cache-hit. A split-definition change re-runs only `sections → match → bundle`. Today a
  change forces monolithic `atlas/tiles/enrich` rebuilds of 5.4 GB artifacts.
- One spine (`section_id`) collapses today's fid/blk/reach/reg-set/wbk id sprawl at the
  delivery boundary (fids still exist inside `sections.pkl` for provenance, but never ship).

## Validation & cutover

- **Per-step tests** (port + extend `pipeline/tests/`): blk-chain contiguity, name-tuple
  provenance, lake-node collapse (no orphans), directionality/no-backtracking (Adams+Fraser),
  section coverage == input coverage, `location_identifier` uniqueness, substring cut
  accuracy.
- **End-to-end golden checks**: for a sample of known regs, assert the section they land on
  and their tributary set match hand-verified expectations (Adams, Fraser, Similkameen,
  Wigwam, Seabird, a `tributary_only` lake reg).
- **Parallel run**: build `v2/` alongside prod; diff coverage/counts; load the webapp against
  `v2/` behind a flag; only then flip `deploy`.
- **Rollback**: path-versioned artifacts + `SHARD_VERSION` env means cutover is a pointer
  swap and rollback is the reverse.

## Suggested build order when picking up

1. `blk-chains` + `graph` (03 S1–S5) — the inverted graph (DONE); de-risks connectivity.
2. `sections` (03 S5–6) + `splits.json` schema (04).
3. `match` (08) on top of sections.
4. `bundle`/tiles/storage (06).
5. Client changes (06) + mobile rebuild.
6. Validate parallel, cut over, retire old pipeline.
