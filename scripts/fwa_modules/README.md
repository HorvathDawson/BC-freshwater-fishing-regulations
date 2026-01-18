# FWA Processing Modules

Stream and waterbody data processing pipeline for BC Freshwater Atlas (FWA). Builds network graph with tributary relationships, extracts metadata with zone assignments, splits features into zone-based layers, and creates search indices.

## 📁 Active Modules

| Module | Purpose |
|--------|---------|
| `graph_builder.py` | Build stream network graph with tributary enrichment |
| `metadata_builder.py` | Extract metadata from graph and add zone assignments |
| `geo_splitter.py` | Split all features into zone-based GeoPackage layers |
| `index_builder.py` | Build searchable JSON index for web application |
| `enrich_kml_points.py` | Match KML labeled points to waterbody polygons |

## 🔄 Processing Pipeline

### 1. Graph Builder (`graph_builder.py`)

**Builds directed network graph of BC stream system with tributary relationships.**

**Input**: 
- FWA Stream Networks (2.6M segments)
- FWA Lakes (386K polygons)
- FWA Watershed Codes

**Output**: `fwa_bc_primal_full.gpickle` (~2.5GB)

**Graph Structure**:
- **Nodes**: Stream endpoints (x,y coordinates)
- **Edges**: Stream segments (LINEAR_FEATURE_ID)
- **Direction**: Downstream flow (v→u)

**Processing**:
1. **Build** - Parallel load of stream layers, filter ditches/bad codes, propagate names by watershed code
2. **Preprocess** - Remove spurious order-1 tailwater edges, clean isolated nodes
3. **Filter Unnamed** (optional) - Remove unnamed streams N+ systems from named waterbodies via BFS
4. **Enrich Tributaries** - DFS traversal to populate `stream_tributary_of` and `lake_tributary_of`
5. **Export** - Save as pickle/GraphML

**Usage**:
```bash
# Build full BC graph
python graph_builder.py

# Build with unnamed filtering
python graph_builder.py -u -t 2

# Extract specific watershed
python graph_builder.py "Guichon Creek"
```

### 2. Metadata Builder (`metadata_builder.py`)

**Extracts all graph attributes and adds zone assignments for fast lookups.**

**Input**:
- Graph pickle (from step 1)
- Wildlife Management Units (WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg)
- FWA_BC.gdb (lakes, wetlands, manmade polygons)

**Output**: `stream_metadata.pickle` (~200-300MB)

**Metadata Structure**:
```python
{
  'zone_metadata': {...},  # Zone info, mgmt units, bounds
  'streams': {
    '169001958': {
      'gnis_name': 'Guichon Creek',
      'stream_tributary_of': 'Thompson River',
      'lake_tributary_of': 'Nicola Lake',
      'zones': ['1', '2'],
      'mgmt_units': ['1-15', '2-3'],
      'cross_boundary': True,
      ...
    }
  },
  'lakes': {...},
  'wetlands': {...},
  'manmade': {...}
}
```

**Processing**:
1. **Load Zones** - Build spatial index (STRtree) for fast point-in-polygon queries
2. **Extract Stream Metadata** - Read all graph edge attributes, assign zones based on endpoints
3. **Process Polygons** - Extract lake/wetland/manmade metadata, assign zones by centroid
4. **Save** - Pickle metadata table for fast loading

**Usage**:
```bash
python metadata_builder.py
```

### 3. Geo Splitter (`geo_splitter.py`)

**Splits all waterbody features into zone-based GeoPackage layers.**

**Input**:
- stream_metadata.pickle (from step 2)
- fwa_bc_primal_full.gpickle (from step 1)
- FWA_BC.gdb (for polygon geometries)
- WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg (for zone boundaries)

**Output**: `waterbodies_by_zone.gpkg` (zone-organized layers)

**Layer Structure**:
```
zone_1_boundaries    (management unit polygons)
zone_1_streams       (LineStrings from graph)
zone_1_lakes         (polygons)
zone_1_wetlands      (polygons)
zone_1_manmade       (polygons)
zone_2_boundaries
zone_2_streams
...
```

**Processing**:
1. **Load Metadata & Graph** - Load preprocessed data
2. **Create Stream Geometries** - Reconstruct LineStrings from graph node coordinates
3. **Filter Polygons** - Load and filter lakes/wetlands/manmade by zone
4. **Add Zone Boundaries** - Include management unit outlines
5. **Write to GeoPackage** - Incremental writing with memory management

**Usage**:
```bash
python geo_splitter.py
```

### 4. Index Builder (`index_builder.py`)

**Creates searchable JSON index for web application.**

**Input**:
- stream_metadata.pickle (from step 2)
- enriched_kml_points.json (from KML enricher)

**Output**: `waterbody_index.json`

**Index Structure**:
```python
{
  '1': {  # Zone number
    'guichon creek': [
      {
        'type': 'stream',
        'linear_feature_id': '169001958',
        'gnis_name': 'Guichon Creek',
        'zones': ['1', '2'],
        ...
      }
    ],
    'mamit lake': [...]
  },
  '2': {...}
}
```

**Processing**:
1. **Load Metadata** - Read from pickle
2. **Normalize & Index** - Group by zone and normalized name
3. **Add KML Points** - Include labeled points
4. **Export** - JSON files for web app

**Usage**:
```bash
python index_builder.py
```

### 5. KML Point Enricher (`enrich_kml_points.py`)

**Matches user-labeled KML points to waterbody polygons.**

**Input**:
- KML file with labeled points (data/labelled/unnamed_lakes.kml)
- FWA_BC.gdb (lakes, wetlands, manmade polygons)

**Output**: `enriched_kml_points.json`

**Processing**: Spatial join of KML points to polygons, extract WATERBODY_KEY

**Usage**:
```bash
python enrich_kml_points.py
```

## 📊 Data Flow

```
FWA Stream Networks ──┐
FWA Lakes ────────────┼──> 1. graph_builder.py ──> fwa_bc_primal_full.gpickle
FWA Watershed Codes ──┘                                      │
                                                             │
Wildlife Mgmt Units ──┐                                      │
FWA_BC.gdb ───────────┼──> 2. metadata_builder.py ──────────┴──> stream_metadata.pickle
                      │                                                    │
                      │                                                    │
                      │                                                    ├──> 3. geo_splitter.py ──> waterbodies_by_zone.gpkg
                      │                                                    │         (zone-based layers)
                      │                                                    │
                      └──> 4. index_builder.py ───────────────────────────┴──> waterbody_index.json
                             (with enriched_kml_points.json)                     (searchable index)
```

## 🎯 Key Outputs

| File | Size | Purpose |
|------|------|---------|
| `fwa_bc_primal_full.gpickle` | ~2.5GB | Network graph with tributary enrichment |
| `stream_metadata.pickle` | ~250MB | All feature metadata + zone assignments |
| `waterbodies_by_zone.gpkg` | ~2-4GB | Zone-based feature layers for GIS viewing |
| `waterbody_index.json` | ~50-100MB | Searchable index for web application |
| `enriched_kml_points.json` | <1MB | KML points matched to waterbodies |

## 📋 Execution Order

```bash
# 1. Build graph (one-time, ~2-3 hours)
python graph_builder.py -u -t 2

# 2. Extract metadata with zones (~30-60 min)
python metadata_builder.py

# 3. Split features by zone (~1-2 hours)
python geo_splitter.py

# 4. Build search index (optional, ~5-10 min)
python index_builder.py

# 5. Enrich KML points (as needed, ~2-3 min)
python enrich_kml_points.py
```

## 🔍 Metadata Structure

### Stream Metadata
- `linear_feature_id` - Segment ID
- `gnis_name` - Stream name
- `stream_tributary_of` - Parent stream
- `lake_tributary_of` - Lake it flows into
- `fwa_watershed_code` - Watershed hierarchy
- `stream_order` - Strahler order
- `zones` - Wildlife management zones (e.g., `['1', '2']`)
- `mgmt_units` - Management units (e.g., `['1-15', '2-3']`)
- `cross_boundary` - Crosses zone boundary (bool)

### Polygon Metadata (Lakes/Wetlands/Manmade)
- `waterbody_key` - Unique ID
- `gnis_name` - Feature name
- `feature_type` - `'lakes'`, `'wetlands'`, or `'manmade'`
- `zones` - Wildlife management zones
- `mgmt_units` - Management units

### Zone Metadata
- `zone_number` - Zone ID (e.g., `'1'`)
- `mgmt_units` - List of management units
- `mgmt_unit_details` - Full details per unit (name, region, bounds)

## ⚙️ Command Options

### graph_builder.py
```bash
-p, --pickle PATH      # Load existing graph
-l, --limit N          # Limit layers for testing
-d, --debug            # Mark search roots
-u, --filter-unnamed   # Filter unnamed streams
-t, --threshold N      # Unnamed filter threshold (default: 2)
```

### metadata_builder.py
```bash
--graph-path PATH      # Graph pickle location
--zones-path PATH      # Wildlife units GeoPackage
--lakes-gdb-path PATH  # FWA_BC.gdb location
--output-path PATH     # Output pickle location
```

### geo_splitter.py
```bash
--metadata-path PATH   # stream_metadata.pickle location
--graph-path PATH      # Graph pickle location
--lakes-gdb-path PATH  # FWA_BC.gdb location
--zones-path PATH      # Wildlife units GeoPackage
--output-path PATH     # Output GeoPackage location
```

### index_builder.py
```bash
--metadata-path PATH    # stream_metadata.pickle location
--kml-points-path PATH  # enriched_kml_points.json location
--output-path PATH      # Output JSON location
```

## 🚀 Performance

- **graph_builder.py**: 2-3 hours for full BC (14 parallel workers)
- **metadata_builder.py**: 30-60 minutes (spatial index acceleration)
- **geo_splitter.py**: 1-2 hours for full BC (processes one zone at a time)
- **index_builder.py**: 5-10 minutes (simple dictionary iteration)
- **Memory**: Peak ~10-15GB RAM during graph building and zone splitting
- **Storage**: ~5-7GB total for all outputs
- `shapely` - Geometry operations
- `pandas` - Data manipulation

## 📚 Related Files

- `scripts/tests/test_graph_builder.py` - Comprehensive test suite
- `scripts/output/fwa_preprocessing/` - Processed geodatabases
- `scripts/fwa_modules/output/` - Graph outputs and logs
│  │  ┌──────────────────────────────────────────────┐         │ │
│  │  │         Mamit Lake (WATERBODY_POLY_ID)       │         │ │
│  │  │  ╔════════════════════════════════════════╗  │         │ │
│  │  │  ║  Stream segments with ≥50% length     ║  │         │ │
│  │  │  ║  inside lake polygon                  ║  │         │ │
│  │  │  ║  (both endpoints in polygon)          ║  │         │ │
│  │  │  ╚════════════════════════════════════════╝  │         │ │
│  │  └──────────────────────────────────────────────┘         │ │
│  │  Result: 12 lake segments → "Mamit Lake"                  │ │
│  │                                                            │ │
│  │  Phase 2: Waterbody Streams (WATERBODY_KEY-based)         │ │
│  │  ┌──────────────────────────────────────────────┐         │ │
│  │  │  All streams with lake's WATERBODY_KEY       │         │ │
│  │  │  (streams touching or inside lake)           │         │ │
│  │  │                                              │         │ │
│  │  │  WATERBODY_KEY = 329176279                   │         │ │
│  │  │  ┌────────┐  ┌────────┐  ┌────────┐         │         │ │
│  │  │  │ Segment│  │ Segment│  │ Guichon│         │         │ │
│  │  │  │   1    │  │   2    │  │ Creek  │         │         │ │
│  │  │  └────────┘  └────────┘  └────────┘         │         │ │
│  │  └──────────────────────────────────────────────┘         │ │
│  │  Result: 27 waterbody streams → "Mamit Lake"              │ │
│  │  (24 overrides of previous assignments)                   │ │
│  │                                                            │ │
│  │  Phase 3: BFS Upstream Tributaries                        │ │
│  │  ┌──────────────────────────────────────────────┐         │ │
│  │  │  BFS from 27 lake waterbody streams          │         │ │
│  │  │                                              │         │ │
│  │  │         Lake Zone (current_stream = None)    │         │ │
│  │  │              ┌────────┐                      │         │ │
│  │  │              │Unnamed │ → Assign to lake ✓   │         │ │
│  │  │              │Trib A  │                      │         │ │
│  │  │              └────┬───┘                      │         │ │
│  │  │                   │                          │         │ │
│  │  │         Named Stream System                  │         │ │
│  │  │         (current_stream = "Guichon Creek")   │         │ │
│  │  │              ┌────┴───┐                      │         │ │
│  │  │              │Guichon │ → Assign to lake ✓   │         │ │
│  │  │              │ Creek  │                      │         │ │
│  │  │              └────┬───┘                      │         │ │
│  │  │                   │                          │         │ │
│  │  │              ┌────┴───┐                      │         │ │
│  │  │              │Unnamed │ → Keep as Guichon ✗  │         │ │
│  │  │              │Trib B  │   (stop BFS)         │         │ │
│  │  │              └────────┘                      │         │ │
│  │  └──────────────────────────────────────────────┘         │ │
│  │                                                            │ │
│  │  BFS Traversal Rules:                                     │ │
│  │  • Continue through: Unnamed streams in lake zone         │ │
│  │  • Continue through: Named streams in lake waterbody      │ │
│  │  • STOP at: Unnamed tributaries of named streams          │ │
│  │  • STOP at: Named tributaries (different stream system)   │ │
│  │                                                            │ │
│  │  Result: 1,080 lake tributaries (791 overrides)           │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  Output: tributary_assignments.json                            │
│          lake_segments.json                                    │
│          Total: 18,424 tributary relationships                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PHASE 4: Zone Splitting                      │
│                      (zone_splitting.py)                        │
│                                                                 │
│  1. Spatial Join by Wildlife Management Zones                  │
│     • Streams → 8 zone layers                                  │
│     • Lakes → 8 zone layers                                    │
│     • Wetlands → 8 zone layers                                 │
│     • Manmade → 8 zone layers                                  │
│                                                                 │
│  2. Add Tributary Data to Streams                              │
│     • TRIBUTARY_OF: Parent stream/lake name                    │
│     • LAKE_POLY_ID: Lake polygon ID (segments only)            │
│                                                                 │
│  3. Memory Optimization                                        │
│     • Process one zone at a time                               │
│     • Write incrementally to output GDB                        │
│     • Reduced parallelism for large datasets (>1M features)    │
│                                                                 │
│  Output: FWA_Zone_Grouped.gdb                                  │
│          • STREAMS_ZONE_{1-8}                                  │
│          • LAKES_ZONE_{1-8}                                    │
│          • WETLANDS_ZONE_{1-8}                                 │
│          • MANMADE_ZONE_{1-8}                                  │
│          • POINTS_ZONE_{1-8}                                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│               PHASE 5: Index Building (Optional)                │
│                     (index_builder.py)                          │
│                                                                 │
│  1. Build Spatial Index                                        │
│     • R-tree index for fast lookups                            │
│     • Bounding boxes for all waterbodies                       │
│                                                                 │
│  2. Create Feature Index                                       │
│     • Map LINEAR_FEATURE_ID → waterbody name                   │
│     • Map WATERBODY_POLY_ID → lake/wetland name                │
│                                                                 │
│  Output: waterbody_index.json                                  │
│          (for web application map search)                      │
└─────────────────────────────────────────────────────────────────┘
```

## 🔍 Key Concepts

### Stream Network Graph

The network analyzer builds a directed graph where:
- **Nodes**: Stream endpoints identified by `DOWNSTREAM_ROUTE_MEASURE`
- **Edges**: Two types
  1. **Stream edges**: Actual stream segments with geometry and attributes
  2. **Connection edges**: Links between streams (downstream node → upstream node)
- **Direction**: Flow is downstream (following decreasing route measure)

```
Example:
  Stream A (feature 12345)
    DOWNSTREAM_ROUTE_MEASURE: 1000
    UPSTREAM_ROUTE_MEASURE: 2000
    DOWNSTREAM_LINEAR_ID: 54321 (next segment)

  Creates:
    Node 1000 ──[Stream Edge 12345]──> Node 2000
    Node 2000 ──[Connection Edge]────> Node 3000 (start of segment 54321)
```

### WATERBODY_KEY vs WATERBODY_POLY_ID

- **WATERBODY_KEY**: Integer field on streams indicating they touch/are inside a waterbody
  - Used for: Finding ALL streams associated with a lake (27 for Mamit Lake)
  - Purpose: Lake tributary detection via BFS
  
- **WATERBODY_POLY_ID**: Polygon ID for waterbodies (lakes, wetlands, manmade)
  - Used for: Identifying which specific lake polygon a stream segment is in
  - Purpose: Lake segment detection (≥50% length inside polygon)

### Lake Segment vs Lake Tributary

- **Lake Segment**: Stream with both endpoints inside lake polygon
  - Example: 701303105 (Guichon Creek segment physically in Mamit Lake)
  - Gets: `LAKE_POLY_ID = 700089332`
  
- **Lake Tributary**: Stream flowing into lake from outside
  - Example: 701304361 (unnamed stream flowing into Mamit Lake)
  - Gets: `TRIBUTARY_OF = "Mamit Lake"`, `LAKE_POLY_ID = None`

### Tributary Assignment Rules

1. **Named streams**: Tributary of parent stream in watershed hierarchy
   ```
   Rey Creek (100-...-383667) → Tributary of Guichon Creek (100-...-296261)
   ```

2. **Unnamed streams**: Inherit name from watershed code
   ```
   Unnamed (100-...-383667-456789) → Rey Creek Tributary
   ```

3. **Named streams through lakes**: Entire stream assigned to lake
   ```
   Guichon Creek segments upstream of Mamit Lake → Tributary of Mamit Lake
   ```

4. **Unnamed streams in lake zone**: Assigned to lake
   ```
   Unnamed stream with WATERBODY_KEY=329176279 → Tributary of Mamit Lake
   ```

5. **Unnamed tributaries of named streams**: Remain with named stream
   ```
   Unnamed tributary of Guichon Creek upstream of lake → Tributary of Guichon Creek
   (NOT assigned to lake)
   ```

## 🛠️ Usage

### Run Full Pipeline
```bash
python fwa_preprocessing_v2.py
```

### Run with Test Data (5 layers only)
```bash
python fwa_preprocessing_v2.py --test-mode
```

### Skip Specific Phases
```bash
# Skip Phase 1 (use existing cleaned streams)
python fwa_preprocessing_v2.py --skip-phase 1

# Skip Phase 1-3 (only run zone splitting)
python fwa_preprocessing_v2.py --skip-phase 1 --skip-phase 2 --skip-phase 3

# Build index only
python fwa_preprocessing_v2.py --build-index --skip-phase 1 --skip-phase 2 --skip-phase 3 --skip-phase 4
```

### Adjust Parallelism
```bash
# Use 8 cores for index building
python fwa_preprocessing_v2.py --build-index --cores 8
```

## 📊 Performance

| Phase | Features Processed | Typical Runtime | Memory Usage |
|-------|-------------------|-----------------|--------------|
| Phase 1 | 2.6M streams | 15-20 min | 8-12 GB |
| Phase 2 | 62 KML points | 2-3 min | 4-6 GB |
| Phase 3 | 2.6M streams | 30-40 min | 10-15 GB |
| Phase 4 | 7.7M features | 60-90 min | 12-20 GB |
| Phase 5 | 386K waterbodies | 10-15 min | 4-6 GB |

**Total Pipeline**: ~2-3 hours for full BC dataset

## 🐛 Debugging

### Test Framework

Run tests to verify tributary assignment logic:
```bash
pytest tests/test_guichon_tributaries.py -v
```

Tests verify:
- ✓ Named stream tributaries (Rey Creek → Guichon Creek)
- ✓ Unnamed stream tributaries (unnamed → Rey Creek)
- ✓ Lake segment detection (Guichon Creek segments in Mamit Lake)
- ✓ Lake tributary assignment (unnamed streams → Mamit Lake)
- ✓ Named stream inheritance (Guichon Creek upstream → Mamit Lake)

### Memory Issues

If Phase 4 gets terminated due to memory:
1. Reduce parallelism (automatic for datasets >1M features)
2. Process zones sequentially instead of in parallel
3. Increase system swap space
4. Run on machine with more RAM (32GB+ recommended)

### Output Validation

Check output GDB:
```bash
# List layers
ogrinfo output/fwa_preprocessing/FWA_Zone_Grouped.gdb

# Check stream count by zone
ogrinfo -sql "SELECT COUNT(*) FROM STREAMS_ZONE_1" output/fwa_preprocessing/FWA_Zone_Grouped.gdb

# Verify tributary data
ogrinfo -sql "SELECT LINEAR_FEATURE_ID, TRIBUTARY_OF, LAKE_POLY_ID FROM STREAMS_ZONE_1 LIMIT 10" output/fwa_preprocessing/FWA_Zone_Grouped.gdb
```

## 📝 Data Model

### StreamEdge (models.py)
```python
@dataclass
class StreamEdge:
    linear_feature_id: int
    gnis_name: Optional[str]
    is_named: bool
    waterbody_key: Optional[int]  # NEW: For lake association
    edge_type: int
    watershed_code: str
    # ... geometry and route measure fields
```

### TributaryAssignment (models.py)
```python
@dataclass
class TributaryAssignment:
    linear_feature_id: int
    tributary_of: Optional[str]
    lake_poly_id: Optional[int]  # Only for lake segments
```

### Output Columns (streams)
- `LINEAR_FEATURE_ID`: Unique stream segment ID
- `GNIS_NAME`: Official stream name (or inherited name)
- `TRIBUTARY_OF`: Parent stream or lake name
- `LAKE_POLY_ID`: Lake polygon ID (if segment is in lake)
- `WATERBODY_KEY`: Waterbody association key
- `FWA_WATERSHED_CODE`: Hierarchical watershed code
- `EDGE_TYPE`: Stream type (1000=normal, 1200=canal, 1410=stream in wetland, etc.)
- `ZONE_GROUP`: Wildlife management zone (1-8)

## 🔗 Related Documentation

- [FWA Processing Guide](../FWA_PROCESSING_GUIDE.md) - Overall processing strategy
- [Inlet/Outlet Design](../INLET_OUTLET_DESIGN.md) - Lake tributary detection approach
- [Pipeline README](../README.md) - Full pipeline documentation

## 📜 License

Part of BC Freshwater Fishing Regulations project.
