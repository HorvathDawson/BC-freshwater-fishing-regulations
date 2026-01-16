# FWA Preprocessing Refactor Plan

## Overview
Simplify the preprocessing pipeline by removing complex network analysis and returning to the proven watershed code hierarchy approach. Keep the door open for future network enhancements while making the current implementation maintainable and debuggable.

---

## Core Principles

1. **NEVER rename streams** - Original GNIS_NAME stays intact
2. **Use TRIBUTARY_OF field** - New field to track parent waterbody relationships
3. **Leverage WATERBODY_KEY** - Streams with same key are inside the same waterbody (no geometry needed)
4. **Watershed hierarchy first** - Use FWA_WATERSHED_CODE parent/child relationships
5. **Spatial joins only when needed** - For lake tributary detection where WATERBODY_KEY isn't enough
6. **Keep it simple** - No graphs, no BFS, no route measures (for now)

---

## Architecture Changes

### Remove
- `fwa_modules/network_analysis.py` - Complex graph-based tributary assignment
- `fwa_modules/stream_preprocessing.py` - Braid merging, tailwater detection
- `fwa_modules/zone_splitting.py` - Over-engineered zone logic
- Complex caching with hash-based invalidation
- Route measure outlet detection
- NetworkX dependency

### Keep
- `fwa_modules/models.py` - But simplify dataclasses
- `fwa_modules/utils.py` - Basic helper functions
- `fwa_modules/kml_enrichment.py` - KML point processing
- Parallel processing infrastructure
- Basic test mode

### Modify
- `fwa_preprocessing.py` - Merge back to single-file approach with clear phases
- `tests/test_guichon_tributaries.py` - Update to check TRIBUTARY_OF instead of renamed GNIS_NAME

---

## New Data Model (models.py)

```python
from dataclasses import dataclass
from typing import Optional
import geopandas as gpd

@dataclass
class StreamSegment:
    """Simplified stream segment - just attributes, no network logic."""
    linear_feature_id: int
    gnis_name: Optional[str]
    fwa_watershed_code: str
    waterbody_key: Optional[int]
    tributary_of: Optional[str] = None  # NEW: Parent waterbody name
    lake_poly_id: Optional[int] = None  # NEW: If tributary of lake
    geometry: any = None
    
    @property
    def is_named(self) -> bool:
        """Check if stream has an official GNIS name."""
        return bool(self.gnis_name and self.gnis_name.strip())
    
    @property
    def is_inside_lake(self) -> bool:
        """Check if stream is inside a lake (via WATERBODY_KEY)."""
        return self.waterbody_key is not None


@dataclass
class LakePolygon:
    """Lake polygon with basic attributes."""
    waterbody_poly_id: int
    waterbody_key: Optional[int]
    gnis_name: Optional[str]
    geometry: any = None


@dataclass
class ProcessingStats:
    """Track statistics for logging."""
    total_streams: int = 0
    originally_named: int = 0
    inherited_same_code: int = 0  # Unnamed streams with same code as named
    river_tributaries: int = 0    # Assigned via parent watershed code
    lake_tributaries: int = 0     # Assigned via lake association
    streams_inside_lakes: int = 0 # Detected via WATERBODY_KEY
```

---

## Processing Pipeline

### Phase 1: Load Raw Data
```
1.1 Load streams (parallel, all 246 layers OR first 5 in test mode)
1.2 Load lakes (FWA_LAKES_POLY)
1.3 Load wetlands (FWA_WETLANDS_POLY) 
1.4 Load manmade waterbodies (FWA_MANMADE_WATERBODIES_POLY)
1.5 Load KML points (unnamed_lakes.kml)
1.6 Load wildlife zones (WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg)
```

**Key Points:**
- Streams: Keep only essential columns (LINEAR_FEATURE_ID, GNIS_NAME, FWA_WATERSHED_CODE, WATERBODY_KEY, geometry)
- Memory optimization: Load in batches, concatenate incrementally
- Deduplication: Drop duplicates by LINEAR_FEATURE_ID after loading all layers

---

### Phase 2: Watershed Hierarchy Enrichment

```python
# STEP 1: Parse watershed codes
streams['clean_code'] = clean_watershed_code(FWA_WATERSHED_CODE)
streams['parent_code'] = get_parent_code(clean_code)

# STEP 2: Build name lookup from named streams
name_map = {clean_code: GNIS_NAME for named streams}

# STEP 3: Inherit names for unnamed streams with SAME watershed code
# Example: "200-ABC123" named "Campbell River"
#          "200-ABC123" unnamed → inherits "Campbell River"
for unnamed stream:
    if clean_code in name_map:
        GNIS_NAME = name_map[clean_code]
        TRIBUTARY_OF = None  # Not a tributary, it IS the river

# STEP 4: Assign river tributary relationships
# Example: "200-ABC123" named "Campbell River"
#          "200-ABC123-XYZ456" unnamed → TRIBUTARY_OF = "Campbell River"
for ALL streams (named and unnamed):
    parent_name = name_map.get(parent_code)
    if parent_name:
        TRIBUTARY_OF = parent_name
        
# Only rename UNNAMED streams
for unnamed stream with TRIBUTARY_OF:
    GNIS_NAME = f"{TRIBUTARY_OF} Tributary"
```

**Key Logic:**
- Named streams: Keep original name, set TRIBUTARY_OF to parent
- Unnamed streams with same code: Inherit name, no TRIBUTARY_OF (they ARE the river)
- Unnamed streams with parent code: Set TRIBUTARY_OF, rename to "X Tributary"

---

### Phase 3: Lake Tributary Detection

```python
# METHOD 1: WATERBODY_KEY matching (FASTEST)
# Streams with same WATERBODY_KEY as a lake are INSIDE that lake
lake_key_map = {WATERBODY_KEY: GNIS_NAME for named lakes}

for stream with WATERBODY_KEY:
    lake_name = lake_key_map.get(WATERBODY_KEY)
    if lake_name:
        TRIBUTARY_OF = lake_name
        LAKE_POLY_ID = corresponding poly_id
        
        # Only rename if originally unnamed
        if originally_unnamed:
            GNIS_NAME = f"{lake_name} Tributary"

# METHOD 2: Spatial intersection (BACKUP for streams without WATERBODY_KEY)
# Only check streams that:
# - Are named OR have river tributary assignment
# - DON'T have WATERBODY_KEY match
# - Touch a named lake

candidate_streams = streams with (GNIS_NAME or TRIBUTARY_OF) AND no WATERBODY_KEY match
named_lakes = lakes with GNIS_NAME

for each lake depth (deepest first):
    touching_streams = spatial_join(candidate_streams, lakes_at_depth)
    
    for stream in touching_streams:
        TRIBUTARY_OF = lake_name
        LAKE_POLY_ID = lake poly_id
        
        # Only rename if originally unnamed
        if originally_unnamed:
            GNIS_NAME = f"{lake_name} Tributary"
```

**Priority:**
1. WATERBODY_KEY matching (definitive, no geometry needed)
2. Spatial intersection (backup, computationally expensive)

**Outlet Detection:**
- For future: Use route measures or network analysis
- For now: Streams touching lakes become tributaries (keep it simple)
- Known issue: Guichon Creek outlet might be marked as Mamit Lake tributary
- Solution: Accept this limitation for MVP, fix in network analysis phase

---

### Phase 4: Filter and Clean

```python
# Keep only streams that are:
# 1. Originally named (official GNIS)
# 2. Inherited name from same watershed code
# 3. Got tributary name from enrichment

final_streams = streams where GNIS_NAME is not null and not empty

# Drop temporary columns
drop: clean_code, parent_code, depth (if used)

# Keep essential columns
keep: LINEAR_FEATURE_ID, GNIS_NAME, TRIBUTARY_OF, LAKE_POLY_ID, FWA_WATERSHED_CODE, WATERBODY_KEY, geometry
```

---

### Phase 5: Zone Splitting

```python
# Simplified zone splitting (no complex logic)
wildlife = load wildlife zones
zone_field = "WILDLIFE_MANAGEMENT_UNIT_ID" (or similar)

# Create ZONE_GROUP from zone code
wildlife['ZONE_GROUP'] = extract_zone_number(zone_field)

# Spatial join all datasets to zones
joined_streams = spatial_join(streams, wildlife)
joined_lakes = spatial_join(lakes, wildlife)
joined_wetlands = spatial_join(wetlands, wildlife)
joined_manmade = spatial_join(manmade, wildlife)
joined_points = spatial_join(kml_points, wildlife)

# Save by zone
for each ZONE_GROUP:
    filter joined data by ZONE_GROUP
    deduplicate (streams by LINEAR_FEATURE_ID, polygons by WATERBODY_POLY_ID)
    save to GDB:
        - STREAMS_ZONE_<zone>
        - LAKES_ZONE_<zone>
        - WETLANDS_ZONE_<zone>
        - MANMADE_ZONE_<zone>
        - LABELED_POINTS_ZONE_<zone>
        - ZONE_OUTLINE_<zone>
```

**Test Mode:**
- NO zone splitting (too slow for testing)
- Save to single GPKG instead of GDB
- Layers: STREAMS, LAKES (optional: WETLANDS, MANMADE, POINTS)

---

### Phase 6: Index Building (Optional)

Keep existing parallel indexing logic - it works well.

---

## Test Updates

### test_guichon_tributaries.py Changes

**Before (checking renamed GNIS_NAME):**
```python
assert "Mamit Lake" not in stream.GNIS_NAME
```

**After (checking TRIBUTARY_OF field):**
```python
# Stream 701305409 - Guichon Creek outlet
assert stream.GNIS_NAME == "Guichon Creek"
assert stream.TRIBUTARY_OF != "Mamit Lake"  # Should be river parent, not lake

# Stream 701305652 - Should be Gypsum Lake tributary
assert stream.TRIBUTARY_OF in ["Gypsum Lake", "Guichon Creek"]

# Stream 701305931 - Braid, should match main channel
main_stream = streams[streams.LINEAR_FEATURE_ID == 701305928]
assert stream.TRIBUTARY_OF == main_stream.TRIBUTARY_OF
```

**Caching:**
- Remove two-stage caching (too complex for MVP)
- Simple pickle cache: `guichon_streams.pkl` and `guichon_lakes.pkl`
- Invalidate if source files change (mtime check)

---

## File Structure

```
scripts/
├── fwa_preprocessing.py           # Main pipeline (simplified, ~600 lines)
├── fwa_modules/
│   ├── __init__.py
│   ├── models.py                  # Dataclasses (NEW: StreamSegment, LakePolygon)
│   ├── utils.py                   # Watershed code parsing, normalize_name
│   └── kml_enrichment.py          # KML point processing (unchanged)
├── tests/
│   └── test_guichon_tributaries.py  # Updated to check TRIBUTARY_OF
└── output/
    └── fwa_preprocessing/
        ├── FWA_Zone_Grouped.gdb     # Full mode output
        ├── guichon_test.gpkg        # Test mode output
        └── waterbody_index.json     # Optional index
```

---

## Migration Steps

### Step 1: Create new models.py
- Define StreamSegment, LakePolygon, ProcessingStats
- Remove NetworkGraph, BFSState, etc.

### Step 2: Refactor fwa_preprocessing.py
- Merge logic from original fwa_preprocessing_old.py
- Keep parallel loading infrastructure
- Simplify enrich_streams() method
- Remove network_analysis.py dependency
- Add TRIBUTARY_OF field throughout

### Step 3: Update test_guichon_tributaries.py
- Check TRIBUTARY_OF instead of GNIS_NAME
- Remove two-stage caching
- Simplify to basic pickle caching
- Ensure test values still work

### Step 4: Test and validate
- Run test mode on GUIC watershed
- Verify tributary assignments
- Check known streams (701305409, 701305652, 701305931)
- Compare output with original working version

### Step 5: Cleanup
- Archive network_analysis.py (don't delete, might need later)
- Archive stream_preprocessing.py
- Archive zone_splitting.py
- Update README.md with new simplified logic

---

## Expected Test Results (After Refactor)

### Stream 701305409 (Guichon Creek outlet)
```
LINEAR_FEATURE_ID: 701305409
GNIS_NAME: "Guichon Creek"
TRIBUTARY_OF: <parent watershed name or NULL>
LAKE_POLY_ID: NULL
WATERBODY_KEY: NULL (or different from Mamit Lake)
```
**Expected:** NOT marked as Mamit Lake tributary

### Stream 701305652 (Gypsum Lake tributary)
```
LINEAR_FEATURE_ID: 701305652
GNIS_NAME: "Gypsum Lake Tributary" (if originally unnamed)
TRIBUTARY_OF: "Gypsum Lake" OR "Guichon Creek" (flexible)
LAKE_POLY_ID: <Gypsum Lake poly_id> (if lake tributary)
```

### Stream 701305931 (Braid segment)
```
LINEAR_FEATURE_ID: 701305931
GNIS_NAME: "Guichon Creek" OR "Guichon Creek Tributary"
TRIBUTARY_OF: <same as 701305928>
LAKE_POLY_ID: NULL
```
**Expected:** Same tributary assignment as main channel (701305928)

---

## Future Enhancements (Post-MVP)

### Network Analysis Module (Optional)
When ready to add network logic:

1. Build graph from FWA_WATERSHED_CODE + route measures
2. Detect outlets using route measure comparison
3. Handle braids and distributaries
4. Improve lake tributary detection
5. Add tailwater classification

**Key:** Keep as SEPARATE optional phase
- Phase 3a: Basic tributary assignment (watershed hierarchy)
- Phase 3b: Network refinement (graph-based corrections)

### Braid Detection (Optional)
- Use EDGE_TYPE field (1100 = braid)
- Group braids by BLUE_LINE_KEY
- Assign same tributary as main channel

### Tailwater Detection (Optional)
- Streams immediately downstream of lakes
- Use route measures or network traversal
- Mark as special type for regulations

---

## Success Criteria

✅ Tests pass with expected TRIBUTARY_OF values
✅ No streams renamed (GNIS_NAME preserved)
✅ TRIBUTARY_OF field correctly populated
✅ Lake tributary detection works via WATERBODY_KEY
✅ Code is <800 lines and readable
✅ Test mode runs in <2 minutes
✅ Known issues documented and accepted for MVP
✅ Easy to add network analysis later

---

## Known Limitations (Accept for MVP)

1. **Outlet streams might be marked as lake tributaries**
   - Guichon Creek outlet → might say "Mamit Lake Tributary"
   - Fix: Add network analysis later
   - Workaround: TRIBUTARY_OF field shows true relationship

2. **Braids might have inconsistent names**
   - EDGE_TYPE 1100 not used yet
   - Fix: Add braid detection later

3. **Spatial joins are slow**
   - No optimization for now
   - Fix: Add spatial indexing later

4. **Test mode doesn't cache**
   - Simple file mtime check only
   - Fix: Add proper caching if needed

---

## Timeline Estimate

- **Step 1** (models.py): 30 minutes
- **Step 2** (fwa_preprocessing.py): 2-3 hours
- **Step 3** (tests): 1 hour
- **Step 4** (validation): 1 hour
- **Step 5** (cleanup): 30 minutes

**Total: ~5-6 hours**

---

## Questions to Resolve

1. Should streams inside lakes (WATERBODY_KEY match) be filtered out entirely?
2. How to handle unnamed streams inside unnamed lakes?
3. Should test mode save to GPKG or GDB?
4. Keep wildlife zone code extraction logic or simplify?
5. Archive network_analysis.py or delete completely?
