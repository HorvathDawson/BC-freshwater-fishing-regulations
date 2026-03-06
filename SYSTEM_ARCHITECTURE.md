# System Architecture — BC Freshwater Fishing Regulations

## High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA ACQUISITION                                │
│                                                                         │
│  BC DataBC WFS APIs ──► fetch_data.py ──► bc_fisheries_data.gpkg       │
│  OSM Boundaries ─────┘   (paginated)      (unified GeoPackage)         │
│                                                                         │
│  BC Synopsis PDF ──► extract_synopsis ──► parse_synopsis (Gemini LLM)  │
│                      (pdfplumber)          → parsed_results.json        │
│                      → row_images/*.png    → regs per waterbody         │
└───────────────────────────────┬─────────────────────┬───────────────────┘
                                │                     │
                                ▼                     │
┌───────────────────────────────────────────┐         │
│           FWA PIPELINE                     │         │
│                                            │         │
│  bc_fisheries_data.gpkg                    │         │
│    │                                       │         │
│    ├──► graph_builder ──► .gpickle         │         │
│    │    (igraph network)                   │         │
│    │                                       │         │
│    └──► metadata_builder ──► .pickle       │         │
│         (FeatureType enum, zone membership)│         │
│              │                             │         │
│              ▼                             │         │
│         MetadataGazetteer                  │         │
│         (name lookups, spatial queries)    │         │
└──────────────┬────────────────────────────┘         │
               │                                       │
               ▼                                       ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     REGULATION MAPPING PIPELINE                         │
│                                                                         │
│  RegulationPipeline (regulation_pipeline.py)                           │
│    │                                                                    │
│    ├── init: Gazetteer + Linker + ScopeFilter + TributaryEnricher      │
│    │         + RegulationMapper                                         │
│    │                                                                    │
│    ├── process_regulations() ──► RegulationMapper.run()                │
│    │                              │                                     │
│    │   ┌──────────────────────────┤                                     │
│    │   │  Phase 1: Synopsis regs  │                                     │
│    │   │   link_waterbody() ──────┤                                     │
│    │   │   scope_filter ──────────┤                                     │
│    │   │   tributary_enricher ────┤                                     │
│    │   │                          │                                     │
│    │   │  Phase 2: Provincial     │ ──► feature_to_regs                │
│    │   │   (admin spatial match)  │     {fwa_id → [reg_ids]}           │
│    │   │                          │                                     │
│    │   │  Phase 2.5: Zone regs    │                                     │
│    │   │   (zone membership)      │                                     │
│    │   │                          │                                     │
│    │   │  Phase 3: Merge          │                                     │
│    │   │   feature_merger ────────┘                                     │
│    │   │   → MergedGroups                                               │
│    │   │                                                                │
│    │   └──► PipelineResult                                             │
│    │         (merged_groups, regulation_details, stats, ...)            │
│    │                                                                    │
│    └── export_geography(PipelineResult)                                │
│          │                                                              │
│          ├── CanonicalDataStore ◄── single source of truth             │
│          │     yield_features()                                         │
│          │       ├── _build_stream_canonical()                         │
│          │       ├── _build_polygon_canonical()                        │
│          │       ├── _merge_same_waterbody_polygons() ◄── zone merge  │
│          │       └── _merge_same_regulation_features() ◄── admin merge│
│          │                                                              │
│          ├── GeoArtifactGenerator (geo_exporter.py)                    │
│          │     ├── export_gpkg() → .gpkg (via ogr2ogr)                │
│          │     └── export_pmtiles() → .pmtiles (via tippecanoe)       │
│          │                                                              │
│          └── SearchIndexBuilder (search_exporter.py)                   │
│                └── → waterbody_data.json (orjson)                      │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          FRONTEND (React + Vite)                        │
│                                                                         │
│  Cloudflare R2 Bucket                                                  │
│    ├── regulations_merged.pmtiles ──► MapLibre GL (vector tiles)       │
│    ├── waterbody_data.json ──► waterbodyDataService                    │
│    └── row_images/*.png ──► SourceImageViewer                          │
│                                                                         │
│  Components:                                                            │
│    Map.tsx ◄── main map + feature interaction (click/hover/highlight)   │
│    SearchBar.tsx ◄── Fuse.js fuzzy search over waterbodies             │
│    InfoPanel.tsx ◄── regulation details panel                          │
│    DisambiguationMenu.tsx ◄── overlapping feature picker               │
│                                                                         │
│  Services:                                                              │
│    waterbodyDataService.ts ◄── loads + decodes waterbody_data.json     │
│    regulationsService.ts ◄── regulation lookup by ID                   │
│                                                                         │
│  Map layers (styles.ts):                                                │
│    streams (blue) │ lakes (light-blue) │ wetlands (green)              │
│    manmade (purple) │ ungazetted (amber) │ admin areas (fill)          │
│    + highlight/selection overlays per feature type                      │
└─────────────────────────────────────────────────────────────────────────┘
```

## Module Reference

### Data Acquisition

| Module | Purpose |
|--------|---------|
| `data/fetch_data.py` | Downloads BC GIS data (WFS paginated) + OSM boundaries → `bc_fisheries_data.gpkg` |
| `data/data_extractor.py` | `FWADataAccessor` — read-only GPKG accessor via `pyogrio`+Arrow |
| `synopsis_pipeline/extract_synopsis.py` | PDF → row extraction via `pdfplumber` + `DBSCAN` clustering |
| `synopsis_pipeline/parse_synopsis.py` | LLM parsing of rows via Google Gemini → `parsed_results.json` |
| `synopsis_pipeline/models.py` | Shared `attrs` models: `WaterbodyRow`, `ParsedWaterbody`, etc. |

### FWA Pipeline

| Module | Purpose |
|--------|---------|
| `fwa_pipeline/graph_builder.py` | Builds `igraph` primal graph of FWA stream network → `.gpickle` |
| `fwa_pipeline/metadata_builder.py` | Feature metadata + `FeatureType` enum + zone assignment → `.pickle` |
| `fwa_pipeline/metadata_gazetteer.py` | `MetadataGazetteer` — name lookups, spatial queries over metadata |

### Regulation Mapping (Core)

| Module | Lines | Purpose |
|--------|-------|---------|
| `regulation_pipeline.py` | ~210 | Top-level orchestrator: init → process → export |
| `regulation_mapper.py` | ~1,424 | Core mapper: link → scope → enrich → map → merge |
| `linker.py` | ~1,350 | `WaterbodyLinker.link_waterbody()`: name → FWA features |
| `linking_corrections.py` | ~2,256 | Manual corrections: DirectMatch, Skip, NameVariation, Admin |
| `regulation_types.py` | ~300 | Shared types: `MergedGroup`, `PipelineResult`, enums |
| `regulation_resolvers.py` | ~569 | Pure functions: feature index, match resolution, ID generation |
| `feature_merger.py` | ~372 | Groups features by physical identity + regulation set |
| `scope_filter.py` | ~100 | Spatial scope constraints (MVP: defaults to WHOLE_SYSTEM) |
| `tributary_enricher.py` | ~200 | Graph BFS for upstream tributaries |
| `provincial_base_regulations.py` | ~545 | Province-wide regulations with admin polygon targets |
| `zone_base_regulations.py` | ~4,140 | Zone-level default regulations from synopsis preambles |
| `admin_target.py` | ~30 | `AdminTarget` NamedTuple for polygon matching |

### Regulation Mapping (Export)

| Module | Lines | Purpose |
|--------|-------|---------|
| `canonical_store.py` | ~1,087 | **Single source of truth**: geometry loading, admin clipping, zoom thresholds, `yield_features()`, polygon zone-merge, stream admin-merge |
| `geo_exporter.py` | ~503 | IO: GPKG (ogr2ogr) + PMTiles (tippecanoe with `--generate-ids --buffer=10`) |
| `geometry_utils.py` | ~150 | Stateless helpers: `round_coords`, `merge_lines`, `extract_geoms` |
| `search_exporter.py` | ~250 | `waterbody_data.json` builder for frontend search + regulation detail |

## Key Data Structures

```
FWAFeature (gazetteer)
  ├── fwa_id, gnis_name, gnis_id
  ├── blue_line_key, waterbody_key, fwa_watershed_code
  ├── zones, mgmt_units, geometry_type
  └── inherited_gnis_names (from graph traversal)

LinkingResult (linker → mapper)
  ├── status: LinkStatus enum
  ├── matched_features: List[FWAFeature]
  ├── link_method: str
  └── admin_match: Optional[AdminDirectMatch]

MergedGroup (merger → canonical store → exporters)
  ├── group_id, feature_ids, regulation_ids
  ├── feature_type, gnis_name, display_name_override
  ├── inherited_gnis_name, name_variants
  ├── waterbody_key, blue_line_key, fwa_watershed_code
  └── zones, mgmt_units, region_names

PipelineResult (mapper → pipeline → exporters)
  ├── merged_groups: Dict[str, MergedGroup]
  ├── feature_to_regs: Dict[str, List[str]]
  ├── regulation_details: Dict[str, Dict]
  ├── admin_area_reg_map, admin_regulation_ids
  └── stats: RegulationMappingStats

Canonical Feature Dict (store → exporters)
  ├── geometry (Shapely), feature_type, group_id
  ├── frontend_group_id (MD5 hash for highlight grouping)
  ├── regulation_ids, regulation_count
  ├── display_name, gnis_name, name_variants
  ├── zones, mgmt_units, region_name
  └── tippecanoe:minzoom, area_sqm/length_m
```

## Post-Merge Pipeline

```
MergedGroups (from feature_merger)
       │
       ▼
CanonicalDataStore.yield_features()
       │
       ├── Streams ──► _build_stream_canonical()
       │                  ├── admin boundary clipping (split in/out)
       │                  └── zoom scoring (BLK stats, magnitude, order)
       │               ──► _merge_same_regulation_features()
       │                  └── groups by (BLK, WBK, regulation_ids)
       │                      merges admin-split segments back together
       │
       ├── Polygons ──► _build_polygon_canonical()
       │                  └── unary_union within group, area + zoom calc
       │               ──► _merge_same_waterbody_polygons()
       │                  └── groups by waterbody_key only
       │                      unions geometry + regulation_ids across zones
       │
       ├── Ungazetted ──► _build_ungazetted_canonical()
       │                  └── point geometry from manual coordinates
       │
       ▼
  Hydrated feature dicts
       │
       ├──► GeoArtifactGenerator
       │      ├── GPKG (all columns, EPSG:3005)
       │      └── PMTiles (lean columns, EPSG:4326, tippecanoe)
       │
       └──► SearchIndexBuilder
              └── waterbody_data.json (grouped by WBK, Fuse.js-ready)
```

## Frontend Architecture

```
waterbody_data.json ──► waterbodyDataService.ts
                          ├── .waterbodies[] → SearchBar (Fuse.js index)
                          ├── .reg_sets{} → dedup'd regulation ID sets
                          ├── .compact{} → unnamed feature metadata
                          └── .regulations{} → full reg details

PMTiles ──► MapLibre GL JS (via pmtiles protocol)
              ├── Layers: streams, lakes, wetlands, manmade, ungazetted, regions
              ├── Highlight layers (per type, filter by frontend_group_id)
              └── Selection layers (per type, filter by frontend_group_id)

User Interaction:
  Click → queryRenderedFeatures()
       → read frontend_group_id from tile properties
       → set highlight filter = ['==', 'frontend_group_id', id]
       → lookup reg_set from waterbodyDataService
       → show InfoPanel with regulation details
       → update URL ?f=<id>

  Search → Fuse.js fuzzy match on waterbody names
        → flyTo() map location from bbox
        → set highlight via frontend_group_id
```

## CLI Entry Points

```bash
# Full pipeline
python -m regulation_mapping.regulation_pipeline [--include-zones]

# Linker coverage test
python -m regulation_mapping.linker [-m] [--export-not-found PATH]

# FWA metadata build
python -m fwa_pipeline.metadata_builder

# Synopsis extraction
python -m synopsis_pipeline.extract_synopsis
python -m synopsis_pipeline.parse_synopsis
```
