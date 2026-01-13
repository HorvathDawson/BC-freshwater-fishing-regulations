# FWA Preprocessing Script - Comprehensive Guide

## Overview
This script processes BC Freshwater Atlas (FWA) data to create a searchable database of waterbodies for fishing regulations lookup.

## Data Flow

```
┌─────────────────┐
│  INPUT DATA     │
├─────────────────┤
│ • Streams (246  │     ┌──────────────────┐
│   layers, 4.9M) │────>│ Step 1: Loading  │
│ • Lakes         │     └──────────────────┘
│ • Wetlands      │              │
│ • Manmade WBs   │              ▼
│ • KML Points    │     ┌──────────────────┐
│ • Wildlife      │     │ Step 2: Enrich   │
│   Zones         │     │ KML Points       │
└─────────────────┘     └──────────────────┘
                                 │
                                 ▼
                        ┌──────────────────┐
                        │ Step 3: Enrich   │
                        │ Stream Names     │
                        └──────────────────┘
                                 │
                                 ▼
                        ┌──────────────────┐
                        │ Step 4: Split by │
                        │ Wildlife Zones   │
                        └──────────────────┘
                                 │
                                 ▼
                        ┌──────────────────┐
                        │ Step 5 (optional)│
                        │ Build Index JSON │
                        └──────────────────┘
                                 │
                                 ▼
                        ┌──────────────────┐
                        │ OUTPUT           │
                        │ • Zone GDB layers│
                        │ • Search index   │
                        └──────────────────┘
```

## Key Functions Explained

### 1. Data Loading (`load_*` methods)

#### `load_streams_raw()`
**Purpose:** Load ~4.9M stream features from 246 separate GDB layers

**Memory Optimization:**
- **Problem:** Loading all layers at once would exceed available RAM
- **Solution:** Batch processing with incremental concatenation
  1. Load 5-15 layers in parallel
  2. Concatenate batch immediately
  3. Merge with main GeoDataFrame
  4. Free batch memory with `gc.collect()`
  5. Repeat for next batch

**Why This Works:**
- Peak memory = size of largest batch, not all data
- Avoids holding 246 GeoDataFrames in memory simultaneously

#### `load_lakes()`, `load_wetlands()`, `load_manmade()`
**Purpose:** Load waterbody polygons for:
1. Stream name correction (lakes touching streams)
2. Linking user-labeled points to polygons

### 2. Enrichment Logic

#### `enrich_kml_points()`
**Purpose:** Link user-labeled points to containing waterbody polygons

**Process:**
1. Spatial join points with lakes → assign `LAKE_POLY_ID`
2. Spatial join points with wetlands → assign `WETLAND_POLY_ID`
3. Spatial join points with manmade → assign `MANMADE_POLY_ID`
4. Convert IDs to nullable integers (`Int64`) to handle NaN
5. **Error Logging:** Save points that don't match ANY polygon to CSV

**Quality Checks:**
- Warns if points match NAMED polygons (expected: unnamed)
- Logs unmatched points for manual review

#### `enrich_streams()` - THE MOST COMPLEX FUNCTION
**Purpose:** Assign names to unnamed streams using tributary logic

##### Phase 1: River Tributary Naming (Fast)
**Strategy:** Use watershed code hierarchy without spatial joins

**How Watershed Codes Work:**
```
"200"                    → Fraser River (depth 1)
"200-ABC123"             → Major tributary (depth 2)
"200-ABC123-XYZ456"      → Minor tributary (depth 3)
"200-ABC123-XYZ456-000000" → Includes padding (cleaned out)
```

**Algorithm:**
1. Clean codes: `"200-ABC123-000000"` → `"200-ABC123"`
2. Get parent: `"200-ABC123-XYZ456"` → parent is `"200-ABC123"`
3. If parent has a name ("Fraser River"), child becomes "Fraser River Tributary"

**Optimization:**
```python
# BAD (slow): Iterate through 4.9M rows
for idx, row in streams.iterrows():
    if row['parent_code'] in named_streams:
        streams.at[idx, 'name'] = named_streams[row['parent_code']] + " Tributary"

# GOOD (fast): Vectorized pandas operation
name_map = pd.Series(named_streams)
parents = streams['parent_code'].map(name_map)
streams['name'] = parents + " Tributary"
```

**Speed Difference:** 100x+ faster with vectorization

##### Phase 2: Lake Tributary Correction (Spatial)
**Problem:** Some "River Tributaries" actually flow into lakes, not rivers

**Solution:** Spatial join streams with lakes
1. Filter candidates: Only check streams ending in " Tributary"
2. Process by lake depth (deepest first) to get most specific matches
3. For each depth level:
   - Filter streams deeper than current lake depth
   - Spatial join: find streams touching lakes
   - Rename: "Fraser River Tributary" → "Adams Lake Tributary"
4. Track completed watershed codes to avoid double-processing

**Why Process by Depth?**
- A stream might touch multiple lakes at different hierarchy levels
- Want most specific (deepest) lake match
- Processing deepest first ensures we get "Outlet Creek Lake Tributary" not "Big Lake Tributary"

**Early Termination:**
- Skip depth level if no candidate streams remain
- Saves time on unnecessary spatial joins

### 3. Spatial Processing

#### `parallel_spatial_join()`
**Purpose:** Speed up spatial joins using multiple CPU cores

**Strategy:**
1. Split target data into N chunks (N = CPU cores)
2. Each worker performs spatial join on its chunk
3. Concatenate results

**Speedup:** Near-linear with core count (15 cores = ~12x faster)

#### `split_and_save()`
**Purpose:** Split all data by wildlife management zones for efficient querying

**Why Split by Zone?**
- Web app users search by location
- Loading one zone (10k features) is much faster than all BC (4.9M features)
- Each zone becomes a separate GDB layer

**Optimization:**
1. **Batch CRS transformation:** Convert all datasets once, not per-zone
2. **Reuse wildlife geometry:** Don't re-read for each zone
3. **Single spatial join:** Join all data to zones once, then filter by zone
4. **Periodic cleanup:** `gc.collect()` every 10 zones to prevent memory creep

**Output Structure:**
```
FWA_Zone_Grouped.gdb/
├── WILDLIFE_MGMT_UNITS (all zones)
├── ZONE_OUTLINE_1 (zone boundary)
├── STREAMS_ZONE_1
├── LAKES_ZONE_1
├── WETLANDS_ZONE_1
├── MANMADE_ZONE_1
├── LABELED_POINTS_ZONE_1
├── ZONE_OUTLINE_2
├── STREAMS_ZONE_2
...
```

### 4. Index Building

#### `build_waterbody_index()`
**Purpose:** Create JSON lookup for web app searches

**Index Structure:**
```json
{
  "1": {  // Zone 1
    "adams lake": [
      {
        "type": "lake",
        "gnis_name": "Adams Lake",
        "layer": "LAKES_ZONE_1",
        "feature_id": "42",
        "attributes": {...}
      },
      {
        "type": "stream",
        "gnis_name": "Adams Lake Tributary",
        "layer": "STREAMS_ZONE_1",
        "feature_id": "123",
        "attributes": {...}
      },
      {
        "type": "point",
        "gnis_name": "Adams Lake",
        "linked_via_point": true,
        "point_name_used": "Adams Lake",
        "attributes": {...}
      }
    ]
  }
}
```

**Name Normalization:**
- Removes quotes, parentheses, extra spaces
- Lowercase for case-insensitive search
- `"Adams Lake (North Arm)"` → `"adams lake"`

**Point-Polygon Linking:**
- If a KML point has `LAKE_POLY_ID = 42`, we:
  1. Find the lake polygon with `WATERBODY_POLY_ID = 42`
  2. Add that polygon to the index under the point's name
  3. Flag as `linked_via_point: true`
- This allows searching unnamed lakes by their user-provided names

## Performance Characteristics

### Memory Usage
- **Peak:** ~8-12 GB during stream loading (test mode: ~1 GB)
- **Optimizations:** Batch processing, incremental concatenation, gc.collect()

### Processing Time (Full Dataset)
- **Stream Loading:** 10-15 mins (parallel)
- **Point Enrichment:** 1-2 mins
- **Stream Enrichment:** 15-25 mins (spatial joins are slow)
- **Zone Splitting:** 20-30 mins
- **Index Building:** 5-10 mins
- **Total:** 50-80 minutes on 16-core machine

### Test Mode (5 layers)
- **Total Time:** 2-5 minutes
- Use for development and testing

## Common Issues & Solutions

### Issue: Out of Memory
**Symptoms:** Process killed, `MemoryError`
**Solutions:**
1. Use test mode: `--test-mode`
2. Reduce batch size in `load_streams_raw()`
3. Increase WSL swap space
4. Process fewer feature types: `--skip-wetlands --skip-manmade`

### Issue: GDB Lock Errors
**Symptoms:** "Cannot delete GDB" on Windows
**Solution:** Script retries 3 times with 2-second delays. If still fails, manually delete output GDB.

### Issue: No Lake Tributaries Found
**Symptoms:** `Corrected 0 tributary systems`
**Cause:** Spatial join indentation error (should be inside loop)
**Fix:** Ensure `join_result = parallel_spatial_join(...)` is indented inside `for lake_depth in unique_depths:` loop

### Issue: Slow Spatial Joins
**Symptoms:** Long wait times at "Starting parallel spatial join"
**Solutions:**
1. Check candidate filtering - should reduce features before join
2. Ensure early termination working (`if current_candidates.empty: continue`)
3. Monitor CPU usage - should be near 100% during parallel sections

## Command Reference

```bash
# Full processing with index
python fwa_preprocessing.py --build-index

# Test mode (fast, 5 layers only)
python fwa_preprocessing.py --test-mode

# Process only specific features
python fwa_preprocessing.py --skip-streams --build-index

# Rebuild index from existing GDB
python fwa_preprocessing.py --build-index-only

# Process lakes and points only
python fwa_preprocessing.py --skip-streams --skip-wetlands --skip-manmade --build-index
```

## Data Structures

### Streams GeoDataFrame
```python
{
    'FWA_WATERSHED_CODE': '200-ABC123-XYZ456-000000',  # Hierarchical ID
    'GNIS_NAME': 'Fraser River Tributary',             # Name (or None)
    'LINEAR_FEATURE_ID': 123456,                       # Unique ID
    'geometry': LineString(...),                       # Stream path
    'clean_code': '200-ABC123-XYZ456',                 # Cleaned code
    'parent_code': '200-ABC123',                       # Parent watershed
    'depth': 3                                         # Hierarchy level
}
```

### KML Points GeoDataFrame
```python
{
    'Name': 'Unnamed Lake #23',            # User-provided name
    'geometry': Point(...),                # GPS coordinates
    'LAKE_POLY_ID': 42,                   # Matched lake (or NaN)
    'WETLAND_POLY_ID': None,              # Matched wetland (or NaN)
    'MANMADE_POLY_ID': None               # Matched manmade (or NaN)
}
```

### Lake/Wetland/Manmade GeoDataFrame
```python
{
    'WATERBODY_POLY_ID': 42,              # Unique polygon ID
    'GNIS_NAME_1': 'Adams Lake',          # Primary name
    'GNIS_NAME_2': None,                  # Alternate name
    'FWA_WATERSHED_CODE': '200-ABC123',   # Watershed code
    'WATERBODY_KEY': 'LAKE_42',          # Deduplication key
    'geometry': Polygon(...)              # Waterbody outline
}
```

## Code Style Notes

### Vectorization Pattern
```python
# Instead of loops, use pandas vectorized operations:
df['result'] = df['column'].map(lookup_dict)
mask = (df['A'] > 5) & (df['B'].str.contains('pattern'))
df.loc[mask, 'C'] = new_value
```

### Nullable Integer Types
```python
# Use Int64 (capital I) for integer columns that might have NaN
df['id_column'] = df['id_column'].astype('Int64')  # Allows NaN
# NOT int64 (lowercase) - that throws error on NaN
```

### Memory Management
```python
# After large operations, free memory explicitly:
del large_dataframe
gc.collect()

# Batch processing pattern:
for batch in batches:
    process(batch)
    del batch
    gc.collect()
```

## Architecture Decisions

### Why GDB Output?
- **Pros:** Single file, multiple layers, widely supported
- **Cons:** Proprietary format, Windows file lock issues
- **Alternative:** GeoPackage (.gpkg) - consider for future

### Why Zone-Based Splitting?
- **Pros:** Fast queries (load only relevant zone), parallel web requests
- **Cons:** Data duplication at zone boundaries, larger total file size
- **Decision:** Speed over storage - storage is cheap, user time is not

### Why JSON Index?
- **Pros:** Fast lookups, simple to parse, human-readable
- **Cons:** Large file size, must load entirely into memory
- **Decision:** Acceptable for ~50MB index file, enables instant search

### Why BC Albers (EPSG:3005)?
- **Pros:** Official BC projection, accurate distances/areas, meters units
- **Cons:** Not WGS84 (web standard)
- **Decision:** Transform to WGS84 at display time in web app

## Future Improvements

1. **Streaming Index:** Load index on-demand rather than all at once
2. **Spatial Index:** Add R-tree index for faster spatial queries
3. **Incremental Updates:** Support updating only changed zones
4. **Database Backend:** Consider PostGIS for better query capabilities
5. **Caching:** Cache intermediate results (enriched streams) to skip reprocessing
