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
              ├─ blk-chains ─ topology ─ sections ──────────────────┼─ match ─ bundle ─ deploy
              │   (03 S1)     (03 S3-4)  (03 S5-6)                   │  (08)    (06)
              └─ overlays (zones, admin, towns, anglerinfo, hydro) ──┘
```

### Steps (geometry chain)

| Step | Reads | Writes | Notes |
|------|-------|--------|-------|
| `fetch` | remote | `data/*.gpkg` | unchanged from today |
| `blk-chains` | streams gpkg (via `FWADataAccessor`) | `v2/blk_chains.pkl` | 03 Step 1–2: merge by BLK, route measures, `(name,source)` tuples |
| `topology` | blk_chains | `v2/topology.pkl` | 03 Step 3–4: contracted graph, lake nodes, barriers |
| `sections` | topology + `splits.json` | `v2/sections.pkl`, `v2/section_geom.*` | 03 Step 5–6: cut geometry, tributary_section_ids, per-section minzoom |
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

- The **2.37 GB micro-graph is consumed once** (`blk-chains`/`topology`) to derive the small
  contracted graph, then never touched again. Tributary reachability is precomputed **per
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

1. `blk-chains` + `topology` (03 S1–4) — de-risks the graph + directionality assumption.
2. `sections` (03 S5–6) + `splits.json` schema (04).
3. `match` (08) on top of sections.
4. `bundle`/tiles/storage (06).
5. Client changes (06) + mobile rebuild.
6. Validate parallel, cut over, retire old pipeline.
