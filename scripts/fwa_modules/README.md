# FWA Processing Modules

This directory contains the core modules for processing BC Freshwater Atlas (FWA) data to create a comprehensive stream and waterbody database with tributary relationships and lake assignments.

## 📁 Module Overview

| Module | Purpose | Key Functions |
|--------|---------|---------------|
| `models.py` | Data structures for streams, lakes, and tributary relationships | `StreamEdge`, `TributaryAssignment` |
| `stream_preprocessing.py` | Clean and merge stream segments | `process_streams()` |
| `kml_enrichment.py` | Link KML points to waterbody polygons | `enrich_points()` |
| `network_analysis.py` | Build stream network graph and assign tributaries | `StreamNetworkAnalyzer` |
| `zone_splitting.py` | Split features by wildlife management zones | `split_by_zones()` |
| `index_builder.py` | Build spatial index for web application | `build_waterbody_index()` |
| `utils.py` | Shared utilities for spatial operations | `parallel_spatial_join()` |

## 🔄 Processing Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                         RAW FWA DATA                            │
│  • Stream Network (2.6M features)                               │
│  • Lakes (386K polygons)                                        │
│  • Wetlands (4.7M polygons)                                     │
│  • Manmade Waterbodies (28K polygons)                           │
│  • KML Points (unnamed lakes from user labeling)               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PHASE 1: Stream Preprocessing                │
│                   (stream_preprocessing.py)                     │
│                                                                 │
│  1. Remove Noise                                                │
│     • Filter unnamed streams >1 level from named streams        │
│                                                                 │
│  2. Merge Braided Streams                                       │
│     • Combine segments with same watershed code                │
│     • Preserve longest segment as representative               │
│                                                                 │
│  Output: cleaned_streams.gdb                                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                 PHASE 2: KML Point Enrichment                   │
│                    (kml_enrichment.py)                          │
│                                                                 │
│  1. Spatial Join                                                │
│     • Match KML points to containing waterbody polygons         │
│                                                                 │
│  2. Warning Log                                                 │
│     • Flag points outside all waterbody polygons                │
│     • Report points matching named lakes (expected: unnamed)    │
│                                                                 │
│  Output: streams with WATERBODY_POLY_ID enriched                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  PHASE 3: Network Analysis                      │
│                   (network_analysis.py)                         │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  STEP 1: Build Stream Network Graph                       │ │
│  │                                                            │ │
│  │    ┌────────┐        ┌────────┐        ┌────────┐        │ │
│  │    │ Stream │─────>  │ Stream │─────>  │ Stream │        │ │
│  │    │   A    │        │   B    │        │   C    │        │ │
│  │    └────────┘        └────────┘        └────────┘        │ │
│  │         │                  │                  │           │ │
│  │         └──────────┬───────┴──────────────────┘           │ │
│  │                    │                                       │ │
│  │              Connection Edges                              │ │
│  │    (link downstream node to upstream node)                │ │
│  │                                                            │ │
│  │  • Nodes: Stream endpoints (DOWNSTREAM_ROUTE_MEASURE)     │ │
│  │  • Edges: Stream segments + Connection edges              │ │
│  │  • Direction: Flows downstream (following route measure)  │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  STEP 2: Assign Tributaries via Watershed Hierarchy       │ │
│  │                                                            │ │
│  │    Watershed Code Hierarchy:                              │ │
│  │    100-190442-244975-296261         (Guichon Creek)       │ │
│  │       └─ 100-190442-244975-296261-383667 (Rey Creek)      │ │
│  │             └─ 100-190442-244975-296261-383667-456789     │ │
│  │                                        (Unnamed tributary) │ │
│  │                                                            │ │
│  │  Logic:                                                    │ │
│  │  • Named stream: Find parent by removing last segment     │ │
│  │  • Unnamed stream: Inherit name from watershed code       │ │
│  │  • Unnamed tributary: "Tributary of [Parent Name]"        │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  STEP 3: Lake Tributary Assignment (3 Phases)             │ │
│  │                                                            │ │
│  │  Phase 1: Lake Segments (Polygon-based)                   │ │
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
