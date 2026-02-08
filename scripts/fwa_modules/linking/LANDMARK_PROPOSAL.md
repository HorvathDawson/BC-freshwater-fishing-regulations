# Landmark System - Spatial Scope Filtering

## Overview

**Problem**: Regulations reference spatial scopes like "upstream of km 42" or "within Upper West Arm zone". We need to filter FWA features to match these constraints.

**Solution**: Two-phase approach:
1. **Matching**: Find the base waterbody (e.g., "WIGWAM RIVER" → all Wigwam segments)
2. **Scope Filtering**: Refine features using landmarks (e.g., keep only upstream segments)

**Key Design**: Different landmark types for different spatial reference scenarios (points, segments, polygons, buffers).

---

## Architecture

### Phase 1: Match Base Waterbody

**Goal**: Identify which waterbody the regulation is talking about

**Strategies**:
- **DirectMatch**: Manual GNIS/watershed code lookup
- **MetadataMatch**: BC Lakes Database lookup
- **FuzzyMatch**: Name similarity matching

**Example**: "WIGWAM RIVER (upstream of km 42)" → Match finds ALL Wigwam River segments

### Phase 2: Apply Scope Filtering  

**Goal**: Refine matched features using spatial constraints parsed from regulation name

**Process**:
1. Parse constraint from regulation name (e.g., "(upstream of km 42)")
2. Lookup landmark by alias fuzzy matching
3. Use landmark to filter features (graph traversal, polygon intersection, etc.)
4. Return refined feature set

**Example**: "WIGWAM RIVER (upstream of km 42)" → Scope filters to ONLY upstream segments

---

## Landmark Types

### 1. PointLandmark - GPS Coordinates

**Use**: Road markers, bridges, dams, waterfalls  
**How**: Finds nearest stream segment, traverses up/downstream

```python
"LANDMARK_WIGWAM_KM42_R4": PointLandmark(
    coordinates=[-115.xxxx, 49.xxxx],
    aliases=["km 42 on Bighorn (Ram) Forest Service Road", 
             "access road adjacent to km 42",
             "Forest Service recreation site adjacent to km 42"],
    linked_waterbodies=["2311"]  # Wigwam River
)
```

### 2. SegmentLandmark - FWA Segment Reference

**Use**: Confluences, named reaches  
**How**: Uses segment start/end as reference point

```python
"LANDMARK_ATNARKO_TALCHAKO_CONFLUENCE": SegmentLandmark(
    linear_feature_id="<segment_id>",
    position="start",  # confluence point
    aliases=["confluence of Atnarko and Talchako"],
    linked_waterbodies=["11611", "17209"]
)
```

### 3. PolygonLandmark - Custom Boundaries

**Use**: Lake zones, management areas  
**How**: Tests spatial relationships (within/intersects/outside)

```python
"LANDMARK_KOOTENAY_UPPER_WEST_ARM": PolygonLandmark(
    polygon_coords=[[(lon1, lat1), (lon2, lat2), ...]],
    aliases=["Upper West Arm", "Kootenay Lake Upper West Arm"],
    linked_waterbodies=["18851"]  # Kootenay Lake
)
```

### 4. DistanceLandmark - Buffer Zones

**Use**: "within 100m of bridge"  
**How**: Wraps another landmark with distance buffer

```python
"LANDMARK_HWY20_BRIDGE_100M": DistanceLandmark(
    base_landmark=PointLandmark(...),
    distance_meters=100,
    aliases=["within 100 meters of Highway 20"]
)
```

---

## Constraint Types

**Spatial constraints parsed from regulation names**:

| Type | Pattern | Example |
|------|---------|---------|
| **directional** | `(upstream\|downstream) of <landmark>` | "WIGWAM RIVER (upstream of km 42)" |
| **zone** | `(in <zone>)` | "KOOTENAY LAKE (in Upper West Arm)" |
| **distance** | `within X m of <landmark>` | "CREEK (within 100m of bridge)" |
| **exclude_tributary** | `except <landmark>` | "BELLA COOLA (except upstream of Hunlen Falls)" |
| **include_only** | `includes only <list>` | "BELLA COOLA (includes only Atnarko and Talchako)" |

---

## Complete Example

**Regulation**: `"WIGWAM RIVER (upstream of the Forest Service recreation site adjacent to km 42...)"`

**Step 1 - Match Base Waterbody**:
- DirectMatch finds `gnis_id="2311"` → Returns ALL Wigwam River segments (500+ segments)

**Step 2 - Parse Scope**:
- ScopeParser detects: `(upstream of ...)`
- Fuzzy matches text to landmark alias: `"LANDMARK_WIGWAM_KM42_R4"`
- Creates constraint: `{type: "directional", direction: "upstream", landmark_id: "..."}`

**Step 3 - Apply Scope Filtering**:
- PointLandmark finds nearest segment to GPS coords  
- Graph traverses upstream from that segment
- Returns only ~200 segments upstream of km 42

**Result**: Regulation applies to only upstream portion, not entire river

---

## Benefits

✅ **No duplication** - One DirectMatch per waterbody, scope auto-parsed  
✅ **Separation of concerns** - Matching (what) separate from scope (where)  
✅ **Works with any match type** - DirectMatch, MetadataMatch, FuzzyMatch  
✅ **Composable** - Multiple constraints can chain (direction + exclusion)  
✅ **Pre-calculated** - Build-time processing, O(1) query-time lookups  
✅ **Maintainable** - Landmarks reused across regulations via aliases

---

## Implementation Phases

### Phase 1: Landmark Classes
- [ ] Create abstract `Landmark` base class  
- [ ] Implement PointLandmark, SegmentLandmark, PolygonLandmark, DistanceLandmark  
- [ ] Add to `name_variations.py`

### Phase 2: Graph Methods
- [ ] `find_nearest_segment(point)` - spatial index  
- [ ] `traverse_upstream_from(segment)` - BFS against flow  
- [ ] `traverse_downstream_from(segment)` - BFS with flow

### Phase 3: Scope Parsing
- [ ] Create `scope_parser.py` with ScopeParser class  
- [ ] Parse patterns: directional, zone, distance, tributary filters  
- [ ] Fuzzy alias matching

### Phase 4: Linker Integration  
- [ ] Inject ScopeParser into WaterbodyLinker  
- [ ] Add `_apply_scope_filtering()` post-processing step  
- [ ] Ensure works with all match types

### Phase 5: Data Entry
- [ ] Wigwam km 42 landmark (PointLandmark)  
- [ ] Kootenay/Williston zones (PolygonLandmarks)  
- [ ] Confluences as needed (SegmentLandmarks)

---

## Summary

**Two-Phase Design**:  
1. **Matching**: Find base waterbody ("WIGWAM RIVER" → all segments)  
2. **Scope Filtering**: Refine using landmarks ("upstream of km 42" → subset)

**Abstract Landmark Pattern**: Type-specific spatial filtering (PointLandmark, SegmentLandmark, PolygonLandmark, DistanceLandmark)

**Key Decisions**: 
1. **PointLandmark** for discrete references (km markers, bridges, falls)
2. **SegmentLandmark** when segment IS the reference (confluences)
3. **PolygonLandmark** for zones and areas
4. **Matching finds base waterbody, scope filtering refines it**
5. **Tributary inclusion/exclusion is scope filtering, not matching**

---


    distance_meters: float              # Buffer distance
    
    def get_geometry(self) -> Polygon:
        base_geom = self.base_landmark.get_geometry()
        return base_geom.buffer(self.distance_meters / 111000)  # Approx degrees
    
    def filter_features(self, features, relationship, graph):
        """Filter by distance from base landmark."""
        buffer_poly = self.get_geometry()
        
        filtered = []
        for feature in features:
            if relationship == "within":
                if feature.geometry.within(buffer_poly):
                    filtered.append(feature)
            elif relationship == "outside":
                if not feature.geometry.intersects(buffer_poly):
                    filtered.append(feature)
        
        return filtered

# Example: Bridge buffer
"LANDMARK_HWY20_BRIDGE_100M": DistanceLandmark(
    landmark_id="LANDMARK_HWY20_BRIDGE_100M",
    name="100m from Highway 20 Bridge",
    base_landmark=PointLandmark(...),  # Bridge location
    distance_meters=100,
    region="Region 5",
    aliases=["within 100 meters of Highway 20"],
    note="100m buffer zone around bridge"
)
```

---

## 2. Integration with Linking Pipeline

**Key Insight**: Scope filtering is a **post-processing step** that applies to ANY successful match, not just DirectMatch.

### Linking Flow
```
1. Initial Match (any strategy)
   ├─ DirectMatch (manual GNIS/watershed code)
   ├─ MetadataMatch (BC Lakes Database)
   └─ FuzzyMatch (name similarity)
   
2. Scope Filtering (if spatial constraint detected)
   ├─ Parse constraint from regulation name (directional, zone, distance, tributary inclusion/exclusion)
   ├─ Apply landmark-based filtering
   ├─ Filter by spatial relationships (upstream/downstream/within/outside)
   ├─ Include/exclude tributaries based on constraints
   └─ Return refined feature set

3. Return LinkResult with filtered features
```

**Why This Design?**
- ✅ **Universal**: Works with any match type (DirectMatch, MetadataMatch, FuzzyMatch)
- ✅ **Modular**: Scope filtering is separate concern from matching
- ✅ **Composable**: Can apply multiple constraints sequentially (direction + tributary exclusion)
- ✅ **Scope-Aware**: Tributary inclusion/exclusion happens in scope filtering, not matching
- ✅ **Testable**: Can test matching and filtering independently

---

## 3. Scope Constraint Types

```python
@dataclass
class SpatialConstraint:
    """Parsed spatial constraint from regulation name."""
    constraint_type: str                # "directional", "zone", "distance", "exclude_tributary", "include_only"
    landmark_id: Optional[str]          # Reference landmark (None for include_only)
    parameters: Dict[str, Any]          # Type-specific params
    
# Examples:
{
    "constraint_type": "directional",
    "landmark_id": "LANDMARK_WIGWAM_KM42_R4",
    "parameters": {"direction": "upstream"}
}

{
    "constraint_type": "zone",
    "landmark_id": "LANDMARK_KOOTENAY_UPPER_WEST_ARM",
    "parameters": {"relationship": "within"}
}

{
    "constraint_type": "distance",
    "landmark_id": "LANDMARK_HWY20_BRIDGE_100M",
    "parameters": {"relationship": "within", "distance_m": 100}
}

{
    "constraint_type": "exclude_tributary",
    "landmark_id": "LANDMARK_HUNLEN_FALLS",
    "parameters": {"direction": "upstream"}  # Exclude everything upstream of falls
}

{
    "constraint_type": "include_only",
    "landmark_id": None,
    "parameters": {"gnis_ids": ["11611", "17209"]}  # Only include specific tributaries
}
```

---

## 4. Scope Parser Enhancement

```python
class ScopeParser:
    """Extracts spatial constraints from regulation names."""
    
    def __init__(self, landmarks: Dict[str, Landmark]):
        self.landmarks = landmarks
        # Build alias → landmark_id lookup for fast matching
        self.alias_index = {}
        for lm_id, lm in landmarks.items():
            for alias in lm.aliases:
                self.alias_index[alias.lower()] = lm_id
    
    def parse_spatial_constraint(self, waterbody_name: str) -> Optional[SpatialConstraint]:
        """
        Extract constraint from regulation name.
        
        Patterns:
        - (upstream|downstream) of <landmark> → directional
        - (in Zone A|Zone B) → zone
        - within X meters of <landmark> → distance
        - except <landmark> → exclude
        """
        
        # Directional pattern
        match = re.search(r'\((upstream|downstream) of (.+?)\)', waterbody_name)
        if match:
            direction = match.group(1)
            landmark_text = match.group(2)
            landmark_id = self._match_landmark(landmark_text)
            
            return SpatialConstraint(
                constraint_type="directional",
                landmark_id=landmark_id,
                parameters={"direction": direction}
            )
        
        # Zone pattern
        match = re.search(r'\(in (.+?)\)', waterbody_name)
        if match:
            zone_text = match.group(1)
            landmark_id = self._match_landmark(zone_text)
            
            return SpatialConstraint(
                constraint_type="zone",
                landmark_id=landmark_id,
                parameters={"relationship": "within"}
            )
        
        # Distance pattern
        match = re.search(r'within (\d+)\s*(?:m|meters?) of (.+?)\)', waterbody_name)
        if match:
            distance = float(match.group(1))
            landmark_text = match.group(2)
            landmark_id = self._match_landmark(landmark_text)
            
            return SpatialConstraint(
                constraint_type="distance",
                landmark_id=landmark_id,
                parameters={"relationship": "within", "distance_m": distance}
            )
        
        # Exclusion pattern ("except", "excluding", "does not include")
        match = re.search(r'\((?:except|excluding|does not include) (.+?)\)', waterbody_name)
        if match:
            exclusion_text = match.group(1)
            
            # Check if it's a landmark reference (e.g., "upstream of falls")
            if 'upstream of' in exclusion_text.lower() or 'above' in exclusion_text.lower():
                landmark_text = re.sub(r'(upstream of|above)\s*', '', exclusion_text, flags=re.IGNORECASE)
                landmark_id = self._match_landmark(landmark_text)
                if landmark_id:
                    return SpatialConstraint(
                        constraint_type="exclude_tributary",
                        landmark_id=landmark_id,
                        parameters={"direction": "upstream"}
                    )
        
        # Inclusion pattern ("only", "includes only")
        match = re.search(r'\((?:only|includes only) (.+?)\)', waterbody_name)
        if match:
            tributary_text = match.group(1)
            # Parse list of tributary names/IDs
            tributary_gnis_ids = self._parse_tributary_list(tributary_text)
            
            return SpatialConstraint(
                constraint_type="include_only",
                landmark_id=None,
                parameters={"gnis_ids": tributary_gnis_ids}
            )
        
        return None
```

---

## 5. Linker Integration - Clean Separation

**Main Linking Method** (modified):
```python
def match_waterbody(self, regulation_name: str, context: Dict) -> LinkResult:
    """
    Primary linking method with scope filtering.
    
    Flow:
    1. Try all matching strategies (direct, metadata, fuzzy)
    2. If match found, apply scope filtering
    3. Return refined features
    """
    
    # STEP 1: Initial match using existing strategies
    link_result = None
    
    # Try DirectMatch
    if regulation_name in self.manual_corrections.direct_matches:
        link_result = self._apply_direct_match(regulation_name)
    
    # Try other strategies if no direct match
    if not link_result or link_result.status != LinkStatus.SUCCESS:
        link_result = self._try_metadata_match(regulation_name, context)
    
    if not link_result or link_result.status != LinkStatus.SUCCESS:
        link_result = self._try_fuzzy_match(regulation_name, context)
    
    # STEP 2: Apply scope filtering if match succeeded
    if link_result and link_result.status == LinkStatus.SUCCESS:
        link_result = self._apply_scope_filtering(regulation_name, link_result)
    
    return link_result


def _apply_scope_filtering(self, regulation_name: str, link_result: LinkResult) -> LinkResult:
    """
    Apply spatial constraints to filter matched features.
    
    This runs AFTER initial matching, works with ANY match type.
    """
    # Parse constraint from regulation name
    constraint = self.scope_parser.parse_spatial_constraint(regulation_name)
    
    if not constraint:
        return link_result  # No scope constraint, return original
    
    # Apply appropriate filtering
    filtered_features = self._filter_by_constraint(
        link_result.matched_features,
        constraint
    )
    
    # Return new LinkResult with filtered features
    return LinkResult(
        status=LinkStatus.SUCCESS,
        matched_features=filtered_features,
        match_method=f"{link_result.match_method}+scope_filter",
        confidence=link_result.confidence,
        notes=f"{link_result.notes} | Scope: {constraint.constraint_type}"
    )


def _filter_by_constraint(self, features: List, constraint: SpatialConstraint) -> List:
    """Apply spatial constraint using appropriate landmark."""
    
    if constraint.constraint_type == "directional":
        landmark = self.landmarks[constraint.landmark_id]
        return landmark.filter_features(
            features,
            constraint.parameters["direction"],
            self.graph
        )
    
    elif constraint.constraint_type == "zone":
        landmark = self.landmarks[constraint.landmark_id]
        return landmark.filter_features(
            features,
            constraint.parameters["relationship"],
            self.graph
        )
    
    elif constraint.constraint_type == "distance":
        landmark = self.landmarks[constraint.landmark_id]
        return landmark.filter_features(
            features,
            constraint.parameters["relationship"],
            self.graph
        )
    
    elif constraint.constraint_type == "exclude_tributary":
        # Get the excluded area using landmark
        landmark = self.landmarks[constraint.landmark_id]
        excluded_segments = landmark.filter_features(
            features,
            constraint.parameters["direction"],
            self.graph
        )
        excluded_ids = {f.linear_feature_id for f in excluded_segments}
        # Return features NOT in excluded set
        return [f for f in features if f.linear_feature_id not in excluded_ids]
    
    elif constraint.constraint_type == "include_only":
        # Filter to only specified tributaries
        allowed_gnis = set(constraint.parameters["gnis_ids"])
        return [f for f in features if f.gnis_id in allowed_gnis]
    
    return features
      Create ScopeParser class with constraint extraction
- [ ] Implement pattern matching for directional/zone/distance/exclude
- [ ] Implement fuzzy alias matching for landmarks
- [ ] Add `parse_spatial_constraint()` method

### Phase 4: Linker Integration
- [ ] Modify `match_waterbody()` to call `_apply_scope_filtering()`
- [ ] Implement `_apply_scope_filtering()` as post-processing step
- [ ] Implement `_filter_by_constraint()` dispatcher
- [ ] Ensure scope filtering works with ALL match types (DirectMatch, MetadataMatch, FuzzyMatch)

### Phase 5 features,
            constraint.parameters["relationship"],
            self.graph
        )
    
    elif constraint.constraint_type == "distance":
        return landmark.filter_features(
            features,
            constraint.parameters["relationship"],
            self.graph
  ============ BUILD TIME ============

# 1. DATA: Landmark definitions
landmarks = {
    "LANDMARK_WIGWAM_KM42_R4": PointLandmark(
        landmark_id="LANDMARK_WIGWAM_KM42_R4",
        name="km 42 on Bighorn FSR",
        coordinates=[-115.xxxx, 49.xxxx],
        aliases=["km 42 on Bighorn (Ram) Forest Service Road", ...],
        linked_waterbodies=["2311"],
        ...
    ),
    "LANDMARK_KOOTENAY_UPPER_WEST_ARM": PolygonLandmark(
        landmark_id="LANDMARK_KOOTENAY_UPPER_WEST_ARM",
        name="Kootenay Lake - Upper West Arm",
        polygon_coords=[...],
        aliases=["Upper West Arm", ...],
        linked_waterbodies=["18851"],
        ...
    )
}

# 2. PROCESS REGULATIONS: Link each regulation
for regulation in parsed_regulations:
    reg_name = regulation.waterbody_name
    
    # Try matching (DirectMatch, Metadata, Fuzzy, etc.)
    link_result = linker.match_waterbody(reg_name, context)
    # → Returns all WIGWAM RIVER segments OR all KOOTENAY LAKE features

**Clean Architecture**: Scope filtering is a **post-processing step** that runs AFTER initial waterbody matching. This means:
- Works with ANY match type (DirectMatch, MetadataMatch, FuzzyMatch)
- Modular and testable separation of concerns
- Composable constraints can be applied sequentially (direction + tributary exclusion)
- No coupling between matching strategy and scope filtering
- **Tributary inclusion/exclusion is a scope concern**, not a matching concern

**Key Decisions**: 
1. Use PointLandmark (with nearest-segment lookup) for discrete reference points (road markers, bridges, falls)
2. Use SegmentLandmark only when the segment itself is the reference (confluences, specific reaches)
3. Use PolygonLandmark for area-based constraints (lake zones, management areas)
4. Apply scope filtering universally after matching, not during matching
5. **Handle tributary inclusion/exclusion in scope filtering** - it's about spatial constraints, not name matching
        # Scope filtering happens inside match_waterbody():
        # - ScopeParser extracts constraint
        # - Landmark filters features
        # - Returns refined feature set
        
        # Map regulation → segment IDs
        for feature in link_result.matched_features:
            segment_to_regs[feature.linear_feature_id].append(regulation.id)

# 3. MERGE SEGMENTS: Combine segments with identical (regulation_set + watershed)
merged_segments = {}
for seg_id, reg_ids in segment_to_regs.items():
    key = (frozenset(reg_ids), watershed_codes[seg_id])
    merged_segments[key].append(seg_id)

# 4. SAVE INDEX
segment_regulation_index = {
    f"group_{i}": {
        "segments": segment_ids,
        "regulations": list(reg_ids),
        "watershed_code": watershed,
        "geometry": combine_geometries(segment_ids)
    }
    for i, ((reg_ids, watershed), segment_ids) in enumerate(merged_segments.items())
}

save_json("segment_regulation_index.json", segment_regulation_index)


# ============ QUERY TIME ============

# User clicks segment on map
clicked_segment_id = "700123456"

# O(1) lookup
regulations = segment_regulation_index[find_group(clicked_segment_id)]

# Display regulations to user
# → No scope constraint detected
# → Returns all lake features unchangedaph
        )
    
    elif constraint.constraint_type == "distance":
        return landmark.filter_features(
            features,
            constraint.parameters["relationship"],
            self.graph
        )
    
    return features
```

---

## 5. Advantages of Abstract Landmark Design

✅ **Type-Safe**: Each landmark type knows how to handle its spatial operations
✅ **Extensible**: Easy to add new landmark types (LineStringLandmark, MultiPointLandmark, etc.)
✅ **Precise**: Points give exact references, no ambiguity about segment start/end
✅ **Flexible**: Can compose landmarks (DistanceLandmark wraps other landmarks)
✅ **Clear Semantics**: Each type's behavior is explicit in its filter_features method
✅ **Reusable**: Same landmark can be used with different constraint types

---

## 6. Use Cases by Type

| Landmark Type | Use Cases | Example |
|---------------|-----------|---------|
| **PointLandmark** | Road markers, bridges, dams, falls | km 42 on FSR, Hunlen Falls |
| **SegmentLandmark** | Confluences, specific reaches | Atnarko-Talchako confluence |
| **PolygonLandmark** | Lake zones, management areas | Kootenay Upper West Arm, Williston Zone A |
| **DistanceLandmark** | Buffer zones, proximity rules | 100m from bridge, 500m from dam |

---

## 7. Implementation Checklist

### Phase 1: Core Classes
- [ ] Create abstract `Landmark` base class
- [ ] Implement `PointLandmark` with nearest-segment logic
- [ ] Implement `SegmentLandmark` with position parameter
- [ ] Implement `PolygonLandmark` with spatial relationships
- [ ] Implement `DistanceLandmark` wrapper

### Phase 2: Graph Methods
- [ ] `find_nearest_segment(point)` - spatial index lookup
- [ ] `get_segment_start_node(segment_id)` - pour point
- [ ] `get_segment_end_node(segment_id)` - mouth
- [ ] `traverse_upstream_from(node)` - BFS against flow
- [ ] `traverse_downstream_from(node)` - BFS with flow

### Phase 3: Scope Parsing
- [ ] Enhance ScopeParser with constraint types
- [ ] Pattern matching for directional/zone/distance
- [ ] Alias matching for landmarks

### Phase 4: Data Entry
- [ ] Create PointLandmark for Wigwam km 42
- [ ] Create PolygonLandmarks for Kootenay/Williston zones
- [ ] Create SegmentLandmarks for confluences

---

## 8. Example Complete Flow

```python
# 1. DATA: Landmark definition
landmarks = {
    "LANDMARK_WIGWAM_KM42_R4": PointLandmark(
        landmark_id="LANDMARK_WIGWAM_KM42_R4",
        name="km 42 on Bighorn FSR",
        coordinates=[-115.xxxx, 49.xxxx],
        ...
    )
}

# 2. BUILD TIME: Parse regulation
reg_name = "WIGWAM RIVER (upstream of the Forest Service recreation site adjacent to km 42...)"

constraint = scope_parser.parse(reg_name)
# → SpatialConstraint(type="directional", landmark_id="...", params={"direction": "upstream"})

# 3. BUILD TIME: Get base features
features = linker.get_features_from_direct_match(gnis_id="2311")  # All Wigwam segments

# 4. BUILD TIME: Apply constraint
landmark = landmarks[constraint.landmark_id]
filtered = landmark.filter_features(features, "upstream", graph)
# → PointLandmark finds nearest segment, traverses upstream, returns subset

# 5. BUILD TIME: Index
segment_to_regs[seg_id] = [reg_id]

# 6. QUERY TIME: Instant lookup
regulations = segment_regulation_index[clicked_segment_id]
```

---

## Summary

**Abstract Landmark Pattern** enables precise spatial references with type-specific behavior. **PointLandmarks** solve the segment ambiguity problem by finding nearest segment dynamically. Each landmark type encapsulates its filtering logic. System is extensible for future spatial constraint types.

**Key Decisions**: 
1. **PointLandmark** for discrete references (km markers, bridges, falls)
2. **SegmentLandmark** when segment IS the reference (confluences)
3. **PolygonLandmark** for zones and areas
4. **Matching finds base waterbody, scope filtering refines it**
5. **Tributary inclusion/exclusion is scope filtering, not matching**

---

## Implementation Phases
