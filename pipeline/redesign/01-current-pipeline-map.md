# 01 — Current Pipeline Map (LEGACY REFERENCE)

> ⚠️ **This documents the OLD pipeline we are replacing.** We are NOT preserving its
> structure — the redesign is clean-slate (see `03`/`05`). Use this only to (a) understand
> what logic/data exists to **port** (name propagation, tributary rules, curated overrides,
> tests) and (b) cross-check `08`'s list of what must actually carry over. Do not treat
> "must reproduce" language below as the current plan.

Verified against source on 2026-07-25.

## Two chains that converge in `enrich`

`pipeline/__main__.py` is the sole orchestrator. Steps dispatch by name but always run in a
fixed canonical order. `all` = `atlas → tiles → enrich`. `parse`, `anglerinfo`,
`hydro-match` are expensive/side-channel and run explicitly.

```
CONTENT CHAIN (PDF → structured regs)          GEOMETRY CHAIN (FWA → atlas → tiles)
  extraction/extract_synopsis.py                 graph/graph_builder.py  (2.37 GB gpickle)
    → synopsis_raw_data.json                       → __main__ _step_atlas
  parsing/parser.py (Gemini)                       → atlas/atlas.pkl (5.4 GB, _ATLAS_VERSION=12)
    → synopsis_parsed.json                         → __main__ _step_tiles
  matching/ (match_table.json + overrides)         → deploy/freshwater_atlas.pmtiles (770 MB)
                     \                             /            + layer_manifest.json
                      → __main__ _step_enrich  ←
                         enrichment/builder.py  (5 phases)
                         → deploy/ shards (R2) + mobile SQLite
```

## Stage artifacts (paths from `config.yaml`)

| Stage | Produces | Format / size |
|-------|----------|---------------|
| graph | `graph/fwa_bc_primal_full.gpickle` | directed igraph pickle, **2.37 GB** |
| atlas | `atlas/atlas.pkl` | frozen dataclasses, **5.4 GB** |
| tiles | `deploy/freshwater_atlas.pmtiles` (**770 MB**), `layer_manifest.json` | PMTiles |
| enrich | `deploy/tier0.json` (**24.9 MB**), `shards/v2/{fids,reaches,polys}/*`, `mobile/vN/regulations.sqlite`, `poly_reaches.json` (20 MB) | JSON + SQLite |

## Graph build — `pipeline/graph/graph_builder.py` (`FWAPrimalGraphIGraph`)

Directed igraph, **edges stored reversed** so `reverse_adj` walks **upstream**. Vertices =
stream endpoints keyed by rounded coord strings; edges = stream segments carrying
`linear_feature_id, fwa_watershed_code, gnis_name/id, waterbody_key, stream_order,
stream_magnitude, feature_code, blue_line_key, edge_type, watershed_code_50k`.

Build steps (`run()`):
1. `build()` — construct graph; skip `999-999999*` watershed codes.
2. `preprocess_graph()` — iteratively delete spurious **order-1, unnamed, non-`9`-WSC** edges into roots.
3. `propagate_names_by_watershed()` — BLK backfill: unnamed edge whose `(wsc, blk)` maps to exactly one name inherits it.
4. `annotate_unnamed_context()` — for still-unnamed edges, store `wc_gnis_names` + `inherited_gnis_names` via **upstream BFS along same-WSC edges only**, stopping at the first named edge, draining all equidistant branches.
5. `filter_unnamed_depth(threshold=2)` — *only with `-u` flag*. Distance metric = WSC-hops from nearest named stream; drop unnamed edges ≥ threshold and everything upstream of them.
6. `export()` — pickle `{graph, node_coords, edge_attrs}`, `edge_attrs` keyed by fid.

## Atlas — `pipeline/atlas/freshwater_atlas.py`

Immutable, **regulation-free** spatial index of every BC water + admin feature, keyed by
stable fids/wbks. Reads gpickle + `bc_fisheries_data.gpkg`. Classifies under-lake streams
as a separate class. Assigns **minzoom** per feature:
- Streams: grouped by **blue_line_key**, group max `stream_magnitude` scored against
  `PERCENTILES {5:100, 6:99.99, 7:99.97, 8:99, 10:95, 11:0}` (`_assign_stream_minzooms`, ~L1090). Default 12.
- Polygons/admin: area thresholds.

Record types (`atlas/models.py`): `StreamRecord, PolygonRecord, AdminRecord, PointRecord, RoadRecord`. Versioned by `_ATLAS_VERSION`.

## Tiles — `pipeline/tiles/tile_exporter.py`

**Pure IO, zero geographic logic** — all geometry decisions already baked into atlas.
atlas records → per-layer `.geojsonseq` → single `tippecanoe` → `.pmtiles`
(`--minimum-zoom=4 --maximum-zoom=12`). Stream feature props: `fid, display_name, blk,
stream_order, fwa_watershed_code, watershed_code_50k`. **No regulation data in tiles** —
regs join client-side by fid/wbk. Effective minzoom = `max(feature_minzoom, layer_floor)`
where layer floors live in `layer_manifest.py`.

## Enrichment — `pipeline/enrichment/builder.py::build()` (5 phases)

1. **Phase 1 `loader.load_and_merge`** — merge raw synopsis + match_table + overrides +
   parse session → `RegulationRecord`s. Deterministic `reg_id = R{zone}_{NAME}_{mus}`;
   collision disambiguation.
2. **Phase 2 `feature_resolver.resolve_features`** — join each reg to atlas features by ID
   type (gnis_ids → simple path; waterbody_keys / wscs / blks / lfids / poly_ids /
   admin_targets → override-only complex path). Zone pruning via `only_within_zones`.
   Emits `ResolvedRegulation{tributary_stream_seeds, lake_outlet_fids}` +
   `FeatureAssignment` (phase=2). Under-lake fids excluded from stream seeds.
3. **Phase 3 `tributary_enricher.TributaryEnricherV2.enrich_tributaries`** — loads gpickle,
   BFS upstream from seeds. See below. Assigns fids (phase=3).
4. **Phase 4 `base_reg_assigner.assign_base_regulations`** — zone-wide / provincial base
   regs by MU polygon intersection; routes `direct / admin / zone_wide`.
5. **Phase 5 `reach_builder.build_regulation_index`** — groups streams into **reaches** by
   `(wsc, display_name, sorted reg_set)`, applies `DisplayNameResolver`, builds
   `name_variants` with provenance, `search_index`, returns 6-key dict:
   `{regulations, reg_sets, reaches, reach_segments, poly_reaches, search_index}`.

Deploy: one in-memory dict → **two serializers** (`r2_sharder.py` 4096-bucket JSON shards +
`tier0.json`; `mobile_sharder.py` SQLite + FTS5). `shard_version` couples them.

## Tributary BFS — `pipeline/enrichment/tributary_enricher.py`

- **Stream seeds**: fids used directly as BFS start edges.
- **Lake seeds**: `waterbody_key → outlet stream fids → BFS start edges`.
- **Excluded WSCs**: seed's `fwa_watershed_code` + all parent codes (`_get_parent_wscs`)
  excluded to prevent backtracking down the mainstem.
- **Lake barriers**: stop at next regulated lake (`_build_lake_barriers` = all regulated
  wbks minus self) to avoid over-propagation.
- **Edge type 2300** (`EXCLUDED_EDGE_TYPES`): may traverse through consecutive 2300 edges
  but cannot exit a 2300 edge back into a non-2300 edge. Missing `edge_type` raises.

`effective_includes_tributaries(parsed)` (`feature_resolver.py` L33-61): `tributary_only`
always expands; otherwise expands iff ≥1 rule is effectively tributary-scoped
(`None` rule inherits entry flag). Determines whether Phase 3 runs for a reg.

## Frontend consumption

There is **no graph shipped to the frontend**. Tributary relationships are denormalized
into a flat **`tributary_reg_ids[]`** array per reach. The client joins fid→reach shard,
reads reg_sets, and renders `source:'tributary'` aliases as "Tributary of X".

## Hand splits today (the thing being replaced)

- Section language (`upstream of X`, `downstream of Y`) is captured as **free text only**:
  `ParsedRule.location_text`, `ParsedEntry.entry_location_text`, and a placeholder
  `Landmark{fid: Optional[str]=None}` in `matching/reg_models.py` — an explicit seam for a
  future spatial-resolution pass that **does not exist yet**.
- Real splits are faked: `overrides.json` has `ADAMS RIVER (upstream of Adams Lake)` and
  `(downstream of Adams Lake)` both pointing at `gnis_ids:["39257"]` — same whole river.
- Where geometry IS split, it's hand-listed `blue_line_keys` / `linear_feature_ids` /
  `fwa_watershed_codes` in `overrides.json` (e.g. Wigwam River notes the divide is
  "approximately at Linear Feature ID 706869683").

## Data sources

FWA (`FWA_STREAM_NETWORKS_SP`, lakes/wetlands/manmade polys) is the spine; GNIS names ride
inside FWA attributes; regulation PDF via pdfplumber; DataBC/DFO/OSM overlays; AnglerInfo +
HYDAT joined at enrich time. All consolidated into `data/bc_fisheries_data.gpkg`.
