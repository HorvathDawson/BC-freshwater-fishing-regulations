# FWA Processing Modules

This directory contains the core modules for processing BC Freshwater Atlas (FWA) data to create a comprehensive stream and waterbody database with tributary relationships and lake assignments.

## 📁 Current Module Status

| Module | Status | Purpose |
|--------|--------|---------|
| `graph_builder.py` | ✅ **Active** | Builds stream network graph with tributary enrichment |
| `index_builder.py` | ✅ **Active** | Builds searchable JSON index for web application |
| `__init__.py` | ✅ **Active** | Package initialization |

### Deprecated/Removed Modules
The following modules were part of the old implementation and have been removed:
- ~~`models.py`~~ - Data structures (deprecated in favor of graph-based approach)
- ~~`utils.py`~~ - Shared utilities (deprecated)
- ~~`kml_enrichment.py`~~ - KML point enrichment (deprecated)
- ~~`fwa_preprocessing.py`~~ - Old preprocessing script (deprecated)

## 🔄 Current Processing Pipeline

### Phase 1: Graph Building (`graph_builder.py`)

**Purpose**: Build a directed graph representing the BC stream network with full tributary relationships.

**Input Data Sources**:
- FWA Stream Networks (2.6M stream segments)
- FWA Lakes (386K lake polygons)
- FWA Watershed Codes (hierarchical watershed identifiers)

**Graph Structure**:
```
Nodes: Stream segment endpoints (x,y coordinates rounded to 3 decimals)
Edges: Stream segments (keyed by LINEAR_FEATURE_ID)
Direction: Flows downstream (v → u, where v=upstream, u=downstream)
Type: NetworkX MultiDiGraph (allows parallel edges)
```

**Processing Steps**:

1. **Build Graph** (`build()`)
   - Parallel layer processing (14 workers)
   - Filter ditches and invalid watershed codes
   - Name propagation by FWA_WATERSHED_CODE
   - Lake name enrichment via WATERBODY_KEY lookup
   - Creates nodes at stream endpoints
   - Adds edges with attributes: `gnis_name`, `lake_name`, `fwa_watershed_code`, `stream_order`, etc.

2. **Preprocess Graph** (`preprocess_graph()`)
   - Remove spurious stream order 1 edges to roots (apparent tailwaters)
   - Clean up isolated nodes
   - Graph integrity validation

3. **Filter Unnamed Depth** (`filter_unnamed_depth()`)
   - BFS from named streams to compute distance
   - Remove unnamed streams N+ systems away from named waterbodies
   - Includes all upstream segments of removed edges
   - Exports removed edge data to JSON

4. **Enrich Tributaries** (`enrich_tributaries()`)
   - DFS traversal from outlet roots upstream
   - Populates two separate fields:
     - `stream_tributary_of`: Named stream this is tributary to
     - `lake_tributary_of`: Lake this stream flows into
   - Handles lake context propagation
   - Named streams stop lake tributary propagation

5. **Filter Watershed** (`filter_watershed()`)
   - Extract specific watershed by stream name
   - Uses connected component analysis

6. **Export** (`export()`)
   - Exports to pickle (.gpickle) - primary format
   - Attempts GraphML export (may fail on large graphs)

**Key Features**:
- Memory-efficient parallel processing
- Handles BC's full 2.6M stream network
- Separate tracking of stream vs lake tributary relationships
- Watershed hierarchy preservation
- Lake tributary propagation upstream

**Output**:
- Graph files: `.gpickle` (pickle), `.graphml` (if successful)
- Removed edge logs: `output/removed_unnamed_depth_edges.json`
- Spurious edge logs: `output/removed_spurious_edges.json`

### Phase 2: Index Building (`index_builder.py`)

**Purpose**: Build searchable JSON index from processed geodatabase for web application.

**Index Structure**:
```python
index[zone][normalized_name] = [list of features]
```

**Processing Steps**:

1. **Process Polygon Layers**
   - Lakes, wetlands, manmade waterbodies
   - Indexed by GNIS_NAME
   - Parallel processing by layer

2. **Process Stream Layers**
   - Indexed by both GNIS_NAME and TRIBUTARY_OF
   - Links streams to parent streams

3. **Process Labeled Points**
   - KML points linked to containing polygons
   - Enables search for unnamed lakes

**Features Indexed**:
- Streams (by name and tributary relationship)
- Lakes (by name)
- Wetlands (by name)
- Manmade waterbodies (by name)
- Labeled points (linked to polygons)

**Output**:
- `feature_regulation_index.json` - searchable index for web app

## 🎯 Implementation Checklist

### ✅ Completed
- [x] Graph-based stream network representation
- [x] Parallel processing for large datasets
- [x] Stream tributary enrichment (DFS-based)
- [x] Lake tributary enrichment (separate field)
- [x] Unnamed stream depth filtering
- [x] Spurious edge removal
- [x] Graph export (pickle + GraphML)
- [x] Searchable index building
- [x] Multi-zone support
- [x] Comprehensive test suite (Guichon Creek)

### 🚧 In Progress / Planned
- [ ] Integration with web application
- [ ] Zone-based graph splitting for web delivery
- [ ] Optimize memory usage for province-wide processing
- [ ] Add more watershed test cases
- [ ] Performance profiling and optimization

## 📊 Performance Notes

**Memory Usage**:
- Province-wide graph: ~8-12 GB RAM
- Parallel workers: 14 (configurable)
- Periodic garbage collection to manage memory

**Processing Time** (approximate):
- Build graph: 10-20 minutes (all layers)
- Enrich tributaries: 5-10 minutes
- Filter unnamed depth: 5-10 minutes
- Export: 2-5 minutes

## 🧪 Testing

Test file: `scripts/tests/test_graph_builder.py`

Tests cover:
- Graph construction
- Tributary assignment (stream and lake)
- Lake tributary propagation
- Named stream behavior
- Guichon Creek watershed (includes Mamit Lake)

Run tests:
```bash
pytest scripts/tests/test_graph_builder.py -v -s
```

## 📝 Usage Example

```python
from fwa_modules.graph_builder import FWAPrimalGraph

# Build graph for specific watershed
builder = FWAPrimalGraph()
builder.validate_paths()
builder.load_lakes()
builder.build(layers=["GUIC"])  # Guichon Creek only
builder.preprocess_graph()
builder.enrich_tributaries()
builder.export("guichon_creek.graphml")

# Or build province-wide
builder.build()  # All layers
builder.filter_unnamed_depth(threshold=2)
builder.enrich_tributaries()
builder.export("bc_full.graphml")
```

## 🔗 Dependencies

- `networkx` - Graph data structure and algorithms
- `geopandas` - Spatial data processing
- `fiona` - Reading GDB layers
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
