# Post-MVP Implementation Roadmap

## Overview

This document outlines the incremental improvements to the waterbody linking system after the MVP is deployed. Each phase builds on the previous one, progressively improving accuracy and reducing warning noise.

**Key Principle**: Each phase is independently deployable and provides measurable value. Phases are ordered by impact (warning reduction, accuracy improvement) vs implementation complexity.

---

## Phase 1: Easy Exclusions

### Goal

Activate exclusions that have single FWA matches with minimal code changes. This is the **highest impact, lowest effort** improvement.

### Scope

- **Only**: Global-level exclusions with single FWA match
- **Only**: WHOLE_SYSTEM exclusion type (no complex scopes yet)
- **Only**: Exclusions already linked and categorized in MVP

### Why Phase 1

- **Biggest impact**: ~30-50% of exclusions typically have single matches
- **No new linking work**: Data structure already built during MVP
- **No disambiguation**: Single match = no ambiguity
- **Low risk**: Conservative approach (MVP) already working, this just refines it
- **Warning reduction**: 40-60% fewer warnings to users

### Prerequisites

- MVP deployed and stable
- Exclusion linking data available in regulation metadata
- FWA graph traversal working for tributary cascade

### Implementation

#### Data Structure (Already Built in MVP)

```python
regulation = {
    "regulation_id": "reg_001",
    "exclusions": {
        "ready_to_activate": [  # These are what Phase 1 uses
            {
                "waterbody": "Michel Creek",
                "fwa_id": "fwa_michel_001",
                "includes_tributaries": true,
                "status": "linked_not_applied"
            }
        ],
        "needs_review": [...]  # Ignored in Phase 1
    }
}
```

#### Activation Algorithm

```python
def apply_phase_1_exclusions(regulation, linked_segments):
    """
    Apply ready-to-activate exclusions to segment list
    """
    segments_to_exclude = set()
    
    for exclusion in regulation.exclusions["ready_to_activate"]:
        # Get FWA feature (already validated in MVP)
        fwa_feature = fwa_database.get_by_id(exclusion["fwa_id"])
        
        # Add main feature segments
        segments_to_exclude.add(fwa_feature.segment_id)
        
        # Handle tributary cascade
        if exclusion["includes_tributaries"]:
            # Find all upstream tributaries
            upstream_segments = find_all_upstream_tributaries(fwa_feature)
            segments_to_exclude.update(upstream_segments)
    
    # Remove excluded segments
    final_segments = [s for s in linked_segments if s not in segments_to_exclude]
    
    # Remove warnings for activated exclusions
    for segment in final_segments:
        segment.warnings = [
            w for w in segment.warnings
            if not (w["type"] == "unlinked_exclusion" and 
                   w["waterbody"] in [e["waterbody"] for e in regulation.exclusions["ready_to_activate"]])
        ]
    
    return final_segments, segments_to_exclude

# Usage
if PHASE_1_ENABLED:
    linked_segments, excluded = apply_phase_1_exclusions(regulation, linked_segments)
else:
    # MVP behavior - no exclusions applied
    pass
```

#### Code Changes Required

**Minimal diff from MVP**:

```python
# MVP code:
def process_regulation(regulation):
    segments = find_segments_mvp(regulation)
    # Exclusions already linked and stored, but not applied
    return segments

# Phase 1 code:
def process_regulation(regulation):
    segments = find_segments_mvp(regulation)
    
    # NEW: Apply ready exclusions
    if config.PHASE_1_EXCLUSIONS_ENABLED:
        segments, excluded = apply_phase_1_exclusions(regulation, segments)
        log_exclusion_application(regulation.id, excluded)
    
    return segments
```

### Expected Impact

**Example Regulation: ELK RIVER'S TRIBUTARIES**

```
Before Phase 1 (MVP):
- Segments: 5,000 (all Elk River tributaries)
- Warnings per segment: 8 (one per exclusion)
- Total warnings: 40,000

Exclusions ready to activate:
- Michel Creek [including tributaries] → 50 segments
- Alexander Creek upstream [including tributaries] → 30 segments
- Abruzzi Creek (no tributaries) → 5 segments

After Phase 1:
- Segments: 4,915 (5,000 - 85 excluded)
- Warnings per segment: 5 (only ambiguous exclusions remain)
- Total warnings: 24,575
- Improvement: 39% fewer warnings, 1.7% more accurate coverage
```

**System-Wide Estimates**:
- 1,500 regulations in system
- Average 500 segments per regulation
- Average 3 ready exclusions per regulation
- Average 50 segments per exclusion

**Results**:
- Segments excluded: 225,000 (30% of coverage - more accurate)
- Warnings removed: 1,125,000 (40% reduction)
- Processing time: <1% increase (exclusion filtering is fast)

### Validation & Testing

**Test Cases**:

1. **Single exclusion, no tributaries**:
   - Input: "except Abruzzi Creek" (5 segments)
   - Expected: Remove 5 segments, remove warning from remaining
   
2. **Single exclusion, with tributaries**:
   - Input: "except Michel Creek [including tributaries]" (50 segments)
   - Expected: Remove 50 segments, remove warning from remaining

3. **Multiple ready exclusions**:
   - Input: 3 ready exclusions (85 segments total)
   - Expected: Remove all 85, remove all 3 warnings

4. **Mixed ready and needs_review**:
   - Input: 2 ready, 2 ambiguous
   - Expected: Apply 2 ready, keep warnings for 2 ambiguous

5. **Exclusion outside global scope** (edge case):
   - Input: Ready exclusion for "Cedar Lake" (not in Elk River tributaries)
   - Expected: No segments removed (no overlap), no warning changes

6. **Overlapping exclusions** (edge case):
   - Input: "except Michel Creek" and "except Michel Creek Tributary"
   - Expected: Remove both, handle overlap correctly (use set operations)

### Success Criteria

- ✅ All single-match exclusions applied correctly
- ✅ Tributary cascade working (tributaries excluded when flagged)
- ✅ Warnings removed only from affected segments
- ✅ Ambiguous exclusions unchanged (still warning)
- ✅ No regulations fail that succeeded in MVP
- ✅ Metrics show expected warning reduction

### Rollout Plan

1. **Deploy with feature flag** (default OFF):
   - Allows gradual rollout
   - Can be toggled per regulation or globally
   
2. **Test on subset of regulations**:
   - Enable for 10% of regulations
   - Monitor for errors, unexpected behavior
   
3. **Validate metrics**:
   - Compare segment counts MVP vs Phase 1
   - Verify warning reduction
   
4. **Full rollout**:
   - Enable globally
   - Monitor for 1 week
   
5. **Remove feature flag**:
   - Make Phase 1 the default behavior

---

## Phase 2: Complex Scopes (DIRECTIONAL, SEGMENT)

### Goal

Implement accurate scope filtering for directional and bounded segment regulations. This reduces over-application of regulations to entire waterbodies.

### Scope

- **DIRECTIONAL scope**: "upstream of X", "downstream of X"
- **SEGMENT scope**: "from X to Y"
- **Landmark linking**: Link text references to geographic points

### Why Phase 2

- **High impact**: ~20-30% of regulations use directional/segment scopes
- **Accuracy improvement**: Apply regulations to correct sections instead of entire system
- **Warning reduction**: Remove scope simplification warnings
- **User clarity**: More precise regulation presentation

### Prerequisites

- Phase 1 deployed (exclusions working)
- Landmark database built or identified
- Directional traversal algorithms implemented
- Landmark snapping logic working

### Key Challenges

#### Challenge 1: Landmark Linking

**Problem**: Text references to landmarks must be converted to geographic points.

**Examples**:
- "Kamloops Lake" → lat/lng or FWA point feature
- "Highway 1 bridge" → bridge location
- "dam" → dam structure location
- "confluence of X and Y" → intersection point

**Solution Approach**:

1. **Build landmark database**:
   ```python
   landmarks = {
       "Region 3": {
           "Kamloops Lake": {
               "type": "lake_outlet",
               "fwa_point": "point_kamloops_001",
               "lat": 50.7,
               "lng": -120.3,
               "waterbodies": ["Thompson River"]  # Where it's valid
           },
           "Highway 1 bridge": {
               "type": "bridge",
               "fwa_point": "point_hwy1_thompson_001",
               "lat": 50.5,
               "lng": -120.1,
               "waterbodies": ["Thompson River"],
               "note": "Multiple Hwy 1 bridges exist - this is Thompson River crossing"
           }
       }
   }
   ```

2. **Landmark matching strategy**:
   - Exact match in database
   - Fuzzy match with confidence threshold
   - Regional context filtering
   - Waterbody context validation (landmark must be on specified waterbody)

3. **Fallback behavior**:
   - If landmark not found: default to WHOLE_SYSTEM (same as MVP)
   - Attach warning: "Landmark 'X' not found, applying to entire system"

#### Challenge 2: Snapping Landmark to Stream Network

**Problem**: Landmark point must be snapped to nearest point on FWA stream network.

**Solution**:
```python
def snap_landmark_to_stream(landmark_point, waterbody, max_distance=500):
    """
    Find nearest point on waterbody to landmark
    
    Args:
        landmark_point: (lat, lng) or FWA point feature
        waterbody: FWA waterbody feature
        max_distance: Maximum snap distance in meters
    
    Returns:
        snapped_point on waterbody linestring, or None if too far
    """
    # Get waterbody geometry
    linestring = waterbody.geometry  # MultiLineString
    
    # Find nearest point on linestring to landmark
    snapped = linestring.interpolate(linestring.project(landmark_point))
    
    # Check distance
    distance = landmark_point.distance(snapped)
    
    if distance > max_distance:
        log_warning(f"Landmark {landmark_point} is {distance}m from waterbody - may be incorrect")
        return None
    
    return snapped
```

**Edge Cases**:
- Landmark on tributary, not main stem → snap to correct tributary
- Multiple valid snap points (braided channels) → choose main channel
- Landmark far from waterbody → return None, fall back to WHOLE_SYSTEM

#### Challenge 3: Directional Traversal

**Problem**: Split waterbody network into upstream/downstream sections.

**Algorithm for DOWNSTREAM**:
```python
def find_downstream_segments(waterbody, landmark_point):
    """
    Find all segments downstream of landmark
    """
    # 1. Snap landmark to waterbody
    snapped_point = snap_landmark_to_stream(landmark_point, waterbody)
    if not snapped_point:
        return None  # Fallback to WHOLE_SYSTEM
    
    # 2. Find segment containing snapped point
    landmark_segment = find_segment_containing_point(waterbody, snapped_point)
    
    # 3. Traverse downstream from landmark
    downstream_segments = [landmark_segment]
    
    # Use FWA downstream graph relationships
    current_segments = [landmark_segment]
    visited = set()
    
    while current_segments:
        segment = current_segments.pop()
        if segment in visited:
            continue
        visited.add(segment)
        
        # Get downstream neighbors
        downstream_neighbors = fwa_graph.get_downstream(segment)
        for neighbor in downstream_neighbors:
            if neighbor.waterbody_id == waterbody.id:  # Stay on same waterbody
                downstream_segments.append(neighbor)
                current_segments.append(neighbor)
    
    return downstream_segments
```

**Algorithm for UPSTREAM**:
```python
def find_upstream_segments(waterbody, landmark_point, includes_tributaries):
    """
    Find all segments upstream of landmark
    """
    snapped_point = snap_landmark_to_stream(landmark_point, waterbody)
    if not snapped_point:
        return None
    
    landmark_segment = find_segment_containing_point(waterbody, snapped_point)
    
    # Traverse upstream
    upstream_segments = [landmark_segment]
    current_segments = [landmark_segment]
    visited = set()
    
    while current_segments:
        segment = current_segments.pop()
        if segment in visited:
            continue
        visited.add(segment)
        
        # Get upstream neighbors
        upstream_neighbors = fwa_graph.get_upstream(segment)
        
        for neighbor in upstream_neighbors:
            if includes_tributaries:
                # Include all upstream (main stem + tributaries)
                upstream_segments.append(neighbor)
                current_segments.append(neighbor)
            elif neighbor.waterbody_id == waterbody.id:
                # Only main stem
                upstream_segments.append(neighbor)
                current_segments.append(neighbor)
    
    return upstream_segments
```

**Tributary Inclusion**:

For directional scopes with `includes_tributaries=true`:
- Find tributaries that join the **directional section**
- Include ALL upstream tributaries of those joining tributaries

Example:
```
Thompson River downstream of Kamloops Lake [including tributaries]

Kamloops Lake (landmark)
│
└─ Downstream section (INCLUDED):
   ├─ Main stem segments
   ├─ Tributary A (joins downstream section)
   │  └─ ALL upstream of Tributary A (included)
   └─ Tributary B (joins downstream section)
      └─ ALL upstream of Tributary B (included)

Upstream section (EXCLUDED):
├─ Main stem segments
└─ Tributary C (joins upstream section - excluded)
```

Implementation:
```python
def find_directional_segments_with_tributaries(waterbody, landmark, direction):
    """
    Find directional section + tributaries that join it
    """
    # 1. Find directional main stem segments
    if direction == "DOWNSTREAM":
        main_stem_segments = find_downstream_segments(waterbody, landmark)
    else:
        main_stem_segments = find_upstream_segments(waterbody, landmark, 
                                                    includes_tributaries=False)
    
    # 2. For each main stem segment, find joining tributaries
    all_segments = set(main_stem_segments)
    
    for segment in main_stem_segments:
        # Find tributaries joining at this segment
        joining_tributaries = fwa_graph.get_tributaries_joining_at(segment)
        
        for tributary in joining_tributaries:
            # Include tributary + all its upstream network
            all_segments.add(tributary.segment_id)
            upstream_of_tributary = find_all_upstream_tributaries(tributary)
            all_segments.update(upstream_of_tributary)
    
    return list(all_segments)
```

#### Challenge 4: Segment (Bounded) Scope

**Problem**: Extract section between two landmarks.

**Algorithm**:
```python
def find_segment_between_landmarks(waterbody, start_landmark, end_landmark, 
                                   includes_tributaries):
    """
    Find all segments between two landmarks on waterbody
    """
    # 1. Snap both landmarks
    start_point = snap_landmark_to_stream(start_landmark, waterbody)
    end_point = snap_landmark_to_stream(end_landmark, waterbody)
    
    if not start_point or not end_point:
        return None  # Fallback to WHOLE_SYSTEM
    
    # 2. Find segments containing landmarks
    start_segment = find_segment_containing_point(waterbody, start_point)
    end_segment = find_segment_containing_point(waterbody, end_point)
    
    # 3. Find path between segments on main stem
    path_segments = find_path_between_segments(start_segment, end_segment, waterbody)
    
    if not path_segments:
        log_error(f"No path found between {start_landmark} and {end_landmark}")
        return None  # Fallback
    
    # 4. If includes_tributaries, find tributaries joining the path
    if includes_tributaries:
        all_segments = set(path_segments)
        for segment in path_segments:
            joining_tributaries = fwa_graph.get_tributaries_joining_at(segment)
            for tributary in joining_tributaries:
                all_segments.add(tributary.segment_id)
                upstream = find_all_upstream_tributaries(tributary)
                all_segments.update(upstream)
        return list(all_segments)
    else:
        return path_segments

def find_path_between_segments(start_seg, end_seg, waterbody):
    """
    Find shortest path on waterbody between two segments
    Use Dijkstra or BFS on FWA graph
    """
    # BFS from start to end
    queue = [(start_seg, [start_seg])]
    visited = set()
    
    while queue:
        segment, path = queue.pop(0)
        
        if segment == end_seg:
            return path
        
        if segment in visited:
            continue
        visited.add(segment)
        
        # Explore neighbors (both upstream and downstream)
        neighbors = fwa_graph.get_neighbors(segment)
        
        for neighbor in neighbors:
            if neighbor.waterbody_id == waterbody.id:  # Stay on same waterbody
                queue.append((neighbor, path + [neighbor]))
    
    return None  # No path found
```

**Edge Cases**:
- Landmarks in wrong order (end before start) → swap them, log warning
- Multiple paths between landmarks (braided channels) → choose shortest or main channel
- Landmarks on different waterbodies → error, fallback to WHOLE_SYSTEM

### Implementation Phases

**Phase 2a: DIRECTIONAL scopes (simpler)**
- Implement landmark database
- Implement snapping algorithm
- Implement directional traversal
- Test on DIRECTIONAL regulations

**Phase 2b: SEGMENT scopes (more complex)**
- Implement path-finding algorithm
- Test on SEGMENT regulations

**Phase 2c: Tributary inclusion for complex scopes**
- Implement tributary joining logic
- Test on regulations with includes_tributaries=true

### Expected Impact

**Example: THOMPSON RIVER downstream of Kamloops Lake [including tributaries]**

```
MVP behavior:
- Applied to: Entire Thompson River + all tributaries (5,000 segments)
- Warning: "Scope simplified to entire system"

Phase 2 behavior:
- Applied to: Thompson River downstream section + tributaries joining downstream (500 segments)
- No scope warning
- Improvement: 90% more accurate, 4,500 fewer segment-regulation associations
```

**System-Wide**:
- ~300 regulations use DIRECTIONAL/SEGMENT scopes
- Average reduction: 60% fewer segments per regulation
- Warning reduction: All scope simplification warnings removed for successful scopes

### Success Criteria

- ✅ Landmark database built with >80% coverage
- ✅ Directional scopes applied correctly
- ✅ Segment scopes applied correctly
- ✅ Tributary joining logic working
- ✅ Fallback to WHOLE_SYSTEM when landmarks not found
- ✅ No regulations fail that succeeded in MVP/Phase 1

---

## Phase 3: Exclusion & Inclusion Disambiguation

### Goal

Build manual review workflow for ambiguous exclusions and implement inclusion support.

### Scope

- Manual review UI for exclusions with multiple FWA matches
- Inclusion implementation (adding waterbodies outside global scope)
- Name variation database management tools

### Why Phase 3

- **Moderate impact**: Remaining ~30% of exclusions need disambiguation
- **Requires UI**: Can't be fully automated
- **Inclusion support**: Nice-to-have feature, lower priority than accuracy

### Manual Review Workflow

**UI Screens**:

1. **Review Queue Dashboard**:
   - List of regulations needing review
   - Priority (CRITICAL > HIGH > MEDIUM > LOW)
   - Count of issues per regulation
   - Estimated impact (segments affected)

2. **Disambiguation Screen**:
   ```
   Regulation: ELK RIVER'S TRIBUTARIES
   Exclusion: "Mill Creek"
   
   Candidates found:
   [ ] Mill Creek (Region 3, near Fernie) - MULTILINESTRING - 15 segments
   [ ] Mill Creek (Region 3, near Cranbrook) - MULTILINESTRING - 8 segments
   [ ] Mill Creek (Region 3, tributary of Flathead River) - MULTILINESTRING - 12 segments
   
   Context:
   - Elk River is in Region 3, Kootenay area
   - Exclusion text: "Mill Creek"
   - No additional scope specified
   
   Suggested action: Select correct Mill Creek based on geographic proximity to Elk River
   
   [Select] [Skip] [Mark as unresolvable]
   ```

3. **Name Variation Editor**:
   ```
   Add name variation:
   Raw name: BABIENE LAKE
   Region: Region 7
   Gazetteer name: Babine Lake
   
   [Add to database]
   
   Recent additions:
   - "Michel Cr." → "Michel Creek" (Region 3)
   - "COQUIHALLA R." → "Coquihalla River" (Region 3)
   ```

### Inclusion Implementation

**Algorithm**:
```python
def process_inclusions(regulation, global_segments):
    """
    Add inclusion waterbodies to regulation coverage
    """
    inclusion_segments = []
    
    for inclusion in regulation.identity.inclusions:
        # Link inclusion waterbody
        candidates = link_waterbody(inclusion.waterbody_key, regulation.region)
        
        if len(candidates) == 1:
            # Single match - add to coverage
            feature = candidates[0]
            segments = get_waterbody_segments(feature)
            
            # Handle tributary inclusion
            if inclusion.includes_tributaries:
                tributary_segments = find_all_upstream_tributaries(feature)
                segments.extend(tributary_segments)
            
            inclusion_segments.extend(segments)
            
        elif len(candidates) > 1:
            # Multiple matches - needs disambiguation
            queue_for_manual_review(inclusion, candidates)
        else:
            # No match - needs linking
            queue_for_manual_review(inclusion, [])
    
    # Merge with global segments
    final_segments = global_segments + inclusion_segments
    
    return final_segments
```

**Edge Cases**:
- Inclusion already in global scope → deduplicate, log as redundant
- Inclusion with complex scope → link waterbody, apply scope filtering
- Inclusion far from main waterbody → flag for review (may be parser error)

### Success Criteria

- ✅ Manual review UI functional
- ✅ Disambiguation workflow working
- ✅ Name variation database editable
- ✅ Inclusions added correctly
- ✅ <5% of exclusions remain unresolved

---

## Phase 4: Rule-Level Scope Modifications

### Goal

Implement rule-specific scope modifications within regulations. This is the most complex feature, providing full fidelity with regulation text.

### Scope

- Rule-level directional scopes
- Rule-level segment scopes
- Rule-level exclusions
- Rule-level inclusions
- Rule precedence resolution

### Why Phase 4

- **Lower impact**: ~10-20% of rules have specific scopes
- **High complexity**: Requires per-rule segment computation
- **Lower priority**: Most regulations work well with global scope + Phase 1-3 improvements

### The Complexity

**Example**:
```
Regulation: THOMPSON RIVER [including tributaries]
Global scope → 5,000 segments

Rule 1: "Trout limit = 2" (no location)
  → Inherits global scope → 5,000 segments

Rule 2: "Salmon fishing prohibited downstream of Kamloops Lake"
  → Directional scope → 500 segments (subset of global)

Rule 3: "Kokanee limit = 5 in Adams Lake only"
  → Inclusion scope → 100 segments (NOT in global scope)

Result: Regulation applies to 5,100 segments total, but different rules to different segments
```

### Implementation Approach

**Storage Model**:
```python
regulation = {
    "regulation_id": "reg_001",
    "global_scope_segments": [...],  # 5,000 segments
    "rules": [
        {
            "rule_id": "rule_001",
            "scope_type": "INHERITED",
            "applies_to_segments": ["*"],  # All global scope
        },
        {
            "rule_id": "rule_002",
            "scope_type": "RESTRICTIVE",
            "applies_to_segments": [...]  # 500 specific segments
        },
        {
            "rule_id": "rule_003",
            "scope_type": "ADDITIVE",
            "applies_to_segments": [...]  # 100 segments outside global
        }
    ]
}
```

**Query-Time Logic**:
```python
def get_rules_for_segment(segment_id, regulation):
    """
    Return rules that apply to a specific segment
    """
    applicable_rules = []
    
    for rule in regulation.rules:
        if rule.scope_type == "INHERITED":
            # Applies to all global scope segments
            if segment_id in regulation.global_scope_segments:
                applicable_rules.append(rule)
        else:
            # Check specific segment list
            if segment_id in rule.applies_to_segments:
                applicable_rules.append(rule)
    
    return applicable_rules
```

### Challenges

See [Rule-Level Scope Layering](#rule-level-scope-layering-challenges) section below for detailed challenges and solutions.

### Success Criteria

- ✅ Rule-level scopes applied correctly
- ✅ Rule precedence working
- ✅ Segment-to-rule mapping efficient
- ✅ Query performance acceptable (<100ms per segment)

---

## Rule-Level Scope Layering Challenges

### Challenge 1: Scope Validation

**Problem**: Rule scope may reference waterbodies not in global scope - is this intentional or error?

**Examples**:
- ✅ Valid: "Kokanee limit = 5 in Adams Lake only" (intentional addition to Thompson River regulation)
- ❌ Error: Salmon rule references "Similkameen River" but regulation is for "Thompson River" (likely parser error)

**Solution**:
- Flag when rule scope is completely outside global scope
- Require manual review with context shown
- Allow intentional additions with explicit INCLUSION type marker

**Edge Cases**:
- Rule scope partially overlaps global scope → split into overlapping and non-overlapping parts
- Rule scope identical to global scope → redundant, but harmless

### Challenge 2: Overlapping Rules with Different Scopes

**Problem**: Multiple rules for same species, different locations.

**Example**:
```
Rule 1: "Trout limit = 2" (global scope - 5,000 segments)
Rule 2: "Trout limit = 1 downstream of Kamloops Lake" (directional - 500 segments)
```

**Question**: What is the trout limit downstream of Kamloops Lake?

**Answer**: Rule 2 overrides Rule 1 (more specific wins)

**Implementation**:
```python
def resolve_rule_conflicts(segment_id, rules):
    """
    Apply precedence: most specific scope wins
    """
    # Group by (rule_type, species)
    rule_groups = defaultdict(list)
    for rule in rules:
        key = (rule.rule_type, rule.species)
        rule_groups[key].append(rule)
    
    final_rules = []
    for key, conflicting_rules in rule_groups.items():
        if len(conflicting_rules) == 1:
            final_rules.append(conflicting_rules[0])
        else:
            # Multiple rules - apply precedence
            # Precedence order: ADDITIVE > RESTRICTIVE > INHERITED
            precedence = {
                "ADDITIVE": 3,
                "RESTRICTIVE": 2,
                "INHERITED": 1
            }
            sorted_rules = sorted(conflicting_rules, 
                                key=lambda r: precedence[r.scope_type], 
                                reverse=True)
            final_rules.append(sorted_rules[0])  # Highest precedence
    
    return final_rules
```

### Challenge 3: Exclusions at Rule Level

**Problem**: Rule excludes part of global scope.

**Example**:
```
Global: "THOMPSON RIVER [including tributaries]" → 5,000 segments
Rule: "Bait ban, except Adams River"
```

**Behavior**:
- Bait ban applies to 4,950 segments (5,000 - Adams River system = 50 segments)
- Other rules still apply to Adams River (only bait ban is excluded)

**Implementation**: Same as global exclusions, but scoped to rule.

```python
def apply_rule_exclusions(rule, global_segments):
    """
    Remove exclusions from rule's applicable segments
    """
    rule_segments = set(global_segments)
    
    for exclusion in rule.exclusions:
        # Link exclusion waterbody
        feature = link_waterbody(exclusion.waterbody_key)
        
        if feature:
            # Remove excluded segments
            excluded_segments = get_waterbody_segments(feature)
            if exclusion.includes_tributaries:
                excluded_segments.extend(find_all_upstream_tributaries(feature))
            
            rule_segments -= set(excluded_segments)
    
    return list(rule_segments)
```

### Challenge 4: Computational Complexity

**Problem**: With N rules and M segments, potentially N×M computations.

**Example**:
- 1 regulation with 50 rules
- Global scope covers 5,000 segments
- Worst case: 250,000 scope evaluations

**Solutions**:

1. **Caching**: Precompute scope-to-segment mappings
   ```python
   # Compute once during regulation processing
   for rule in regulation.rules:
       rule.segment_cache = compute_rule_segments(rule, global_segments)
   
   # Query time: O(1) lookup
   applicable_rules = [r for r in rules if segment_id in r.segment_cache]
   ```

2. **Indexing**: Spatial index for segment lookups
   ```python
   # Build R-tree index of segments
   segment_index = rtree.Index()
   for segment in segments:
       segment_index.insert(segment.id, segment.bounds)
   
   # Fast spatial queries for scope filtering
   segments_in_scope = segment_index.query(scope_bounds)
   ```

3. **Lazy evaluation**: Only compute when queried
   ```python
   class Rule:
       def __init__(self, ...):
           self._segment_cache = None
       
       @property
       def segments(self):
           if self._segment_cache is None:
               self._segment_cache = compute_rule_segments(self)
           return self._segment_cache
   ```

### Challenge 5: Tributary Cascade at Rule Level

**Problem**: Does a rule-level exclusion cascade to tributaries?

**Example**:
```
Rule: "Salmon closure downstream of dam, except Tributary X"
```

**Question**: Does "except Tributary X" exclude:
1. Only Tributary X main stem?
2. Tributary X + all its tributaries?

**Solution**: Same as global scope - use **[Includes Tributaries]** marker or regulation lookup strategy (see MVP documentation).

---

## Unsolved Problems (Future Research)

### 1. Buffer Regions

**Problem**: Distance-based buffers along streams.

**Examples**:
- "within 500m of bridge"
- "1 km upstream from dam"

**Challenges**:
- Buffer geometry computation (complex for curved streams)
- Segment intersection with buffer
- Performance for large buffers

**Proposed Solution**:
- Use PostGIS ST_Buffer function
- Precompute buffers for common landmarks
- Fall back to approximate (e.g., "nearest N segments")

### 2. Named Parts

**Problem**: Named portions of waterbodies.

**Examples**:
- "Thompson River - North Thompson section"
- "Columbia Lake (east arm)"

**Challenges**:
- Named parts not in FWA database
- Ambiguous boundaries
- Regional naming variations

**Proposed Solution**:
- Build named parts database
- Link to FWA segment ranges
- Manual curation required

### 3. Confluence Scopes

**Problem**: Regulations at confluence points.

**Examples**:
- "at the confluence of Thompson and Fraser Rivers"
- "where Creek X meets Lake Y"

**Challenges**:
- Finding exact confluence point in FWA network
- Defining scope extent (just confluence, or surrounding area?)

**Proposed Solution**:
- Identify confluence as FWA point feature
- Default to small radius around confluence (e.g., 100m)
- Allow manual override of extent

### 4. Coordinate-Based References

**Problem**: GPS coordinates in regulation text.

**Examples**:
- "north of 51° 30' N"
- "within boundary defined by coordinates..."

**Challenges**:
- Parsing coordinate formats
- Converting to FWA segment filters
- Precision/accuracy of boundaries

**Proposed Solution**:
- Parse common coordinate formats
- Use spatial intersection with FWA segments
- Validate coordinates are reasonable for waterbody

### 5. Waterbody Type Filtering

**Problem**: Provincial/zonal regulations may only apply to certain waterbody types.

**Examples**:
- "All high elevation lakes" (elevation > 1500m AND type == LAKE)
- "Class I streams" (classification-based)

**Challenges**:
- FWA may not have all required metadata (elevation, classification)
- Parsing type conditions from regulation text
- Efficient filtering at query time

**Proposed Solution**:
- Enrich FWA data with elevation from DEM
- Build waterbody classification database
- Precompute type filters, cache results

---

## Implementation Timeline

### Realistic Estimates

**Phase 1: Easy Exclusions**
- Development: 1-2 weeks
- Testing: 1 week
- Deployment: 1 week
- **Total: 3-4 weeks**

**Phase 2: Complex Scopes**
- Phase 2a (DIRECTIONAL): 3-4 weeks development, 2 weeks testing
- Phase 2b (SEGMENT): 2-3 weeks development, 2 weeks testing
- Phase 2c (Tributaries): 1-2 weeks development, 1 week testing
- **Total: 10-12 weeks**

**Phase 3: Disambiguation & Inclusions**
- UI development: 4-6 weeks
- Backend integration: 2-3 weeks
- Testing: 2 weeks
- **Total: 8-11 weeks**

**Phase 4: Rule-Level Scopes**
- Development: 6-8 weeks
- Testing: 3-4 weeks
- Performance optimization: 2-3 weeks
- **Total: 11-15 weeks**

**Overall Timeline**: 32-42 weeks (~8-10 months) for full implementation

---

## Prioritization Matrix

| Phase | Impact | Complexity | Time | Priority |
|-------|--------|------------|------|----------|
| Phase 1: Easy Exclusions | HIGH (40% warning reduction) | LOW | 1 month | **DO FIRST** |
| Phase 2: Complex Scopes | HIGH (accuracy for 30% of regulations) | MEDIUM | 3 months | **DO SECOND** |
| Phase 3: Disambiguation | MEDIUM (remaining 30% exclusions) | MEDIUM | 2-3 months | **DO THIRD** |
| Phase 4: Rule Scopes | LOW (10-20% of rules) | HIGH | 3-4 months | **DO LAST** |

---

## Success Metrics by Phase

### Phase 1
- Warning density: 8.5 → 5.0 per segment (41% reduction)
- Exclusion coverage: 0% → 40% applied
- User confusion: 40% → 25% (estimated from warnings)

### Phase 2
- Scope simplification warnings: 30% regulations → 5% regulations
- Regulation accuracy: 70% → 90% (segments correctly scoped)
- Over-application: 300% avg → 110% avg (segments per regulation vs actual)

### Phase 3
- Exclusion coverage: 40% → 70% applied
- Manual review queue: 150 items → 45 items
- Inclusions working: 0% → 90% of inclusions added

### Phase 4
- Rule-level scope accuracy: 60% → 95%
- Segment-rule associations: Accurate per rule instead of per regulation
- Query precision: 70% → 95% (correct rules returned for segment query)

---

## Next Steps Summary

1. **Deploy MVP** and collect metrics for baseline
2. **Implement Phase 1** (easy exclusions) for quick win
3. **Build landmark database** in parallel for Phase 2 prep
4. **Implement Phase 2** (complex scopes) for accuracy improvement
5. **Build UI** for Phase 3 (disambiguation)
6. **Evaluate Phase 4** need based on user feedback from Phases 1-3

For MVP implementation details, see [MVP_LINKING_IMPLEMENTATION.md](MVP_LINKING_IMPLEMENTATION.md).
For regulation precedence rules, see [REGULATION_CASCADING.md](REGULATION_CASCADING.md).
