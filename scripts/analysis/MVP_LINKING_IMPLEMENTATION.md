# MVP Waterbody Linking Implementation

## Overview

This document defines the **Minimum Viable Product (MVP)** implementation for linking parsed fishing regulations to FWA (Freshwater Atlas) stream network segments. The MVP takes a conservative, warning-heavy approach to maximize coverage while flagging uncertainties.

**Core Principle**: Better to show a rule to too many segments with warnings than to miss segments where the rule applies.

---

## MVP Philosophy

### The Four Pillars

1. **Maximum extent**: Always link to the largest possible set of segments that could be affected
2. **Conservative fallbacks**: When scope cannot be resolved, default to widest interpretation
3. **Abundant warnings**: Every simplification/failure generates user-visible warnings on affected segments
4. **Manual review priority**: Main waterbody linking failures are TOP priority to resolve before anything else

### What MVP Does NOT Do

- Does NOT apply exclusions (stores them for Phase 1, generates warnings)
- Does NOT add inclusions (logs for manual review)
- Does NOT implement complex scopes (DIRECTIONAL, SEGMENT, BUFFER - defaults to WHOLE_SYSTEM)
- Does NOT implement landmark linking
- Does NOT use automatic fuzzy matching (only manual curation)

---

## MVP Capabilities

### Core Linking

| Feature                    | Status     | MVP Behavior                                                              |
|----------------------------|------------|---------------------------------------------------------------------------|
| Main waterbody linking     | ✅ / ❌    | REQUIRED - includes disambiguation. Single match → auto-link              |
|   - Single FWA match       | ✅         | Auto-link with 100% confidence                                            |
|   - Multiple FWA matches   | ❌         | Manual review queue with candidates (CRITICAL)                            |
|   - No FWA matches         | ❌         | Manual review queue (CRITICAL)                                            |
| WHOLE_SYSTEM scope         | ✅         | Links to entire waterbody feature                                         |
| TRIBUTARIES_ONLY scope     | ✅         | Links to all upstream tributaries, excludes main stem                     |
| Includes tributaries flag  | ✅         | Traverses full upstream tributary network                                 |

**Note**: Disambiguation is integral to main waterbody linking. For exclusions/inclusions, disambiguation failures just create warnings (no manual review needed).

### Regulation Types

| Feature                    | Status     | MVP Behavior                                                              |
|----------------------------|------------|---------------------------------------------------------------------------|
| Provincial regulations     | ✅         | Applied to ALL segments in BC (no type filtering)                         |
| Zonal regulations          | ✅         | Applied to ALL segments in zone (no type filtering)                       |
| Waterbody-specific regs    | ✅         | Applied per global scope linking                                          |
| Rule inheritance           | ✅         | Rules without location inherit global scope                               |

### Complex Scopes (All Default to WHOLE_SYSTEM)

| Feature                    | Status     | Fallback Behavior                  | Warning Attached                              |
|----------------------------|------------|------------------------------------|-----------------------------------------------|
| DIRECTIONAL scope          | ⚠️         | → WHOLE_SYSTEM + tributaries flag  | "Directional scope not implemented"           |
| SEGMENT scope              | ⚠️         | → WHOLE_SYSTEM + tributaries flag  | "Segment scope not implemented"               |
| BUFFER scope               | ⚠️         | → WHOLE_SYSTEM + tributaries flag  | "Buffer scope not implemented"                |
| NAMED_PART scope           | ⚠️         | → WHOLE_SYSTEM + tributaries flag  | "Named part scope not implemented"            |
| CONFLUENCE scope           | ⚠️         | → WHOLE_SYSTEM + tributaries flag  | "Confluence scope not implemented"            |

### Exclusions & Inclusions

| Feature                    | Status     | MVP Behavior                                                              |
|----------------------------|------------|---------------------------------------------------------------------------|
| Global exclusions          | ⚠️         | Linked and categorized, but NOT applied - warnings on ALL segments        |
| Global inclusions          | ⚠️         | Ignored - logged to manual review queue (low priority)                    |
| Rule-level exclusions      | ⚠️         | Ignored - warnings on all segments receiving rule                         |
| Rule-level inclusions      | ⚠️         | Ignored - logged to manual review queue (low priority)                    |

---

## Critical Path: Main Waterbody Linking

### Why This Is Priority #1

The MVP **requires** successful linking of `identity.waterbody_key` to an FWA feature. If this fails, the entire regulation cannot be processed. All other features (exclusions, complex scopes, etc.) can be deferred, but this one cannot.

### Linking Process

**Main waterbody linking includes disambiguation as a core step:**

1. **Name variation lookup** (check manually curated variation database by region)
2. **Name normalization** (apply standardized transformations)
3. **Exact match attempt** (search FWA gazetteer database)
4. **Disambiguation**:
   - **Single match** → Auto-link (✅ success)
   - **Multiple matches** → Manual review queue with candidates (❌ requires human decision)
   - **No matches** → Manual review queue (❌ critical failure)

**FWA Feature Types**: Matching is done across ALL FWA feature geometry types - polygons (lakes), multilinestrings (rivers/streams), and points. The same waterbody name may exist as different geometry types in the database.

**Critical**: NO automatic fuzzy matching for auto-linking. All name variations must be manually curated. Fuzzy matching may be used as a tool to HELP USERS identify potential typos during manual review, but never for automatic linking.

### Name Variation Database

**Structure**: Maps raw regulation names to official gazetteer names, organized by region.

```python
name_variations = {
    "Region 3": {
        "ELK RIVER'S TRIBUTARIES": "Elk River",
        "Michel Cr.": "Michel Creek",
        "COQUIHALLA R.": "Coquihalla River",
        "BABIENE LAKE": "Babine Lake",  # Typo variation
        "Kootenay R.": "Kootenay River"
    },
    "Region 4": {
        "THOMPSON R.": "Thompson River",
        "Adams L.": "Adams Lake"
    }
}

def lookup_name_variation(raw_name, region_id):
    """
    Convert raw regulation name to official gazetteer name
    using manually curated variation database
    """
    if region_id in name_variations:
        return name_variations[region_id].get(raw_name, raw_name)
    return raw_name
```

**Building the Database**:
- Start with empty database
- As regulations fail to link, manually review and add variations
- Use fuzzy matching as a TOOL during manual review to spot typos
- Add the verified variation to the database
- Re-run linking to confirm success

### Name Normalization

**Basic transformations** applied before matching:

```python
def normalize_waterbody_name(name):
    """
    Normalize regulation names to match FWA naming conventions
    """
    # Expand abbreviations
    name = name.replace(" Cr.", " Creek")
    name = name.replace(" R.", " River")
    name = name.replace(" L.", " Lake")
    
    # Remove possessive forms
    name = name.replace("'S TRIBUTARIES", "")
    
    # Case normalization
    name = name.title()
    
    return name

# Example
"ELK RIVER'S TRIBUTARIES" → "Elk River"
"Michel Cr." → "Michel Creek"
```

### Linking Failure Handling

**When main waterbody cannot be linked:**

1. **Log to HIGH PRIORITY manual review queue**:
```python
{
    "priority": "CRITICAL",
    "regulation_id": "reg_123",
    "waterbody_key": "Mystery Creek System",
    "identity_verbatim": "MYSTERY CREEK SYSTEM [including tributaries]",
    "issue": "No FWA feature found matching waterbody name",
    "candidates": [],  # Empty if no matches, populated if multiple
    "requires": "Manual waterbody linking",
    "suggested_actions": [
        "Check for spelling variations",
        "Search historical names",
        "Use fuzzy matching tool to find similar names",
        "Verify waterbody exists in region"
    ]
}
```

2. **Do NOT add regulation to any segments** - avoid false positives

3. **Track linking failure statistics** to prioritize systematic fixes

### Fuzzy Matching (Manual Review Tool Only)

```python
def suggest_typo_candidates(name, fwa_database):
    """
    ONLY used during manual review to help users identify simple typos
    NOT used for automatic linking
    """
    from fuzzywuzzy import fuzz
    
    suggestions = []
    for fwa_feature in fwa_database:
        score = fuzz.ratio(name, fwa_feature.name)
        if score > 85:  # High similarity suggests possible typo
            suggestions.append({
                "fwa_name": fwa_feature.name,
                "similarity": score,
                "geometry_type": fwa_feature.geometry_type,
                "note": "Possible typo - requires manual verification"
            })
    
    return sorted(suggestions, key=lambda x: x["similarity"], reverse=True)

# This helps manual reviewers spot "BABIENE" vs "Babine" typos
# But the actual variation must be manually added to database
```

---

## Tributary Enrichment Algorithm (MVP Version)

### Overview

This algorithm finds all FWA segments that should receive a regulation based on the main waterbody and tributary inclusion flag.

**MVP Limitation**: Only handles WHOLE_SYSTEM and TRIBUTARIES_ONLY scopes. Directional and segment scopes default to WHOLE_SYSTEM.

### Input

- Main waterbody name (from `identity.waterbody_key`)
- Global scope type (`WHOLE_SYSTEM` or `TRIBUTARIES_ONLY`)
- Global scope `includes_tributaries` flag (true/false)
- Exclusions list (from `identity.exclusions[]`)
- Inclusions list (from `identity.inclusions[]`)

### Output

- Set of FWA segment IDs that receive the regulation
- Categorized exclusions (ready/needs_review)
- Unlinked exclusion warnings attached to each segment
- Unlinked inclusion records for manual review

### Step-by-Step Algorithm

#### Step 1: Link Main Waterbody

**Input**: `identity.waterbody_key` (e.g., "ELK RIVER")

**Process**:
1. Look up regional name variation
2. Apply name normalization
3. Search FWA gazetteer database for exact match
4. Handle disambiguation:
   - If single match: use that feature (✅ success)
   - If multiple matches: flag for manual review (❌ critical failure)
   - If no match: flag for manual review (❌ critical failure)

**Output**: Main waterbody FWA feature (or null if failed)

**Edge Cases**:
- Waterbody name not in gazetteer → manual review
- Multiple waterbodies with same name in region → manual review
- Typo in regulation text → fuzzy matching tool suggests candidates during manual review
- Waterbody exists but has different geometry types (lake vs river) → may need type hint or manual selection

#### Step 2: Graph Traversal - Find All Tributaries

**Input**: Main waterbody FWA feature

**Process**:
1. Determine what to include based on scope type:
   - If `scope.type == "WHOLE_SYSTEM"` AND `includes_tributaries == true`:
     * Include main stem + all upstream tributaries
   - If `scope.type == "WHOLE_SYSTEM"` AND `includes_tributaries == false`:
     * Include ONLY main stem (no tributaries)
   - If `scope.type == "TRIBUTARIES_ONLY"`:
     * Include ONLY tributaries (exclude main stem)
     * Note: `includes_tributaries` must be true for TRIBUTARIES_ONLY

2. Use FWA stream network graph to find upstream connections:
```python
def find_segments(main_waterbody, scope_type, includes_tributaries):
    if scope_type == "TRIBUTARIES_ONLY":
        # Find ALL upstream tributaries, exclude main stem
        all_upstream = traverse_upstream_network(main_waterbody)
        return [s for s in all_upstream if s.waterbody_id != main_waterbody.id]
    
    elif scope_type == "WHOLE_SYSTEM":
        if includes_tributaries:
            # Include main stem + all tributaries
            main_segments = get_waterbody_segments(main_waterbody)
            tributary_segments = traverse_upstream_network(main_waterbody)
            return main_segments + tributary_segments
        else:
            # Only main stem
            return get_waterbody_segments(main_waterbody)
    
    else:
        # Complex scope types (DIRECTIONAL, SEGMENT, etc.)
        # MVP: Default to WHOLE_SYSTEM behavior
        return find_segments(main_waterbody, "WHOLE_SYSTEM", includes_tributaries)
```

**Output**: 
```python
candidate_segments = [
    "fwa_segment_001",  # Main stem
    "fwa_segment_002",  # Tributary A
    "fwa_segment_003",  # Tributary B  
    "fwa_segment_004",  # Sub-tributary of A
    ...
]
```

**Edge Cases**:
- Disconnected segments (rare in FWA, but possible) → only include connected segments
- Circular references in graph → use visited set to prevent infinite loops
- Very large tributary networks (1000+ segments) → may need progress tracking
- Segments at boundary of FWA coverage → include what's available, log boundary issue

#### Step 3: Process Exclusions (MVP: Link and Categorize, Don't Apply)

**Input**: 
- `candidate_segments` from step 2
- `identity.exclusions[]` from parsed regulation

**Process**:

For each exclusion in `identity.exclusions[]`:

**3a. Attempt to Link Exclusion**
```python
exclusion_result = link_waterbody(exclusion.waterbody_key, region)

if len(exclusion_result) == 1:
    # Single match - ready for Phase 1 activation
    linked_exclusions_ready.append({
        "exclusion": exclusion,
        "fwa_id": exclusion_result[0].fwa_id,
        "fwa_name": exclusion_result[0].name,
        "geometry_type": exclusion_result[0].geometry_type,
        "includes_tributaries": exclusion.includes_tributaries,
        "status": "ready_to_activate"
    })
    
elif len(exclusion_result) > 1:
    # Multiple matches - needs disambiguation
    ambiguous_exclusions.append({
        "exclusion": exclusion,
        "candidates": exclusion_result,
        "status": "needs_disambiguation"
    })
    
else:
    # No matches - needs linking
    unlinked_exclusions.append({
        "exclusion": exclusion,
        "status": "needs_linking"
    })
```

**3b. Generate Warnings for ALL Exclusions (MVP Behavior)**

Even for exclusions that linked successfully, generate warnings because MVP doesn't apply them yet:

```python
for exclusion in exclusions:
    warning = {
        "type": "unlinked_exclusion",
        "waterbody": exclusion.waterbody_key,
        "original_text": exclusion.location_verbatim,
        "message": f"{exclusion.waterbody_key} is listed as an exception but has not been geographically excluded in this MVP version. This area may be excepted - verify regulations before fishing.",
        "phase_1_ready": exclusion in linked_exclusions_ready  # Hint for users
    }
    # Attach to ALL segments in candidate_segments
    for segment_id in candidate_segments:
        attach_warning(segment_id, warning)
```

**Output**:
```python
# Candidate segments unchanged (exclusions NOT applied in MVP)
final_segments = candidate_segments

# Categorized exclusions
exclusions_data = {
    "ready_to_activate": linked_exclusions_ready,
    "needs_review": ambiguous_exclusions + unlinked_exclusions
}

# All segments get warnings about ALL exclusions
```

**Edge Cases**:
- Exclusion references a waterbody outside the global scope → still attempt to link, store result
- Exclusion has complex scope (e.g., "upstream of bridge") → link waterbody, ignore scope for MVP
- Exclusion is misspelled → ends up in unlinked, manual review suggests corrections
- Same waterbody excluded multiple times → deduplicate

#### Step 4: Process Inclusions (MVP: Log for Manual Review)

**Input**:
- `final_segments` from step 3
- `identity.inclusions[]` from parsed regulation

**Process**:

For each inclusion in `identity.inclusions[]`:

```python
# MVP: Log all inclusions to manual review queue, don't add segments
manual_review_queue.append({
    "type": "unlinked_inclusion",
    "regulation_id": regulation.id,
    "waterbody": inclusion.waterbody_key,
    "location_verbatim": inclusion.location_verbatim,
    "impact": "low - does not reduce coverage, only missing additive feature",
    "priority": "LOW"
})

# Do NOT add segments
# Do NOT generate warnings (no impact on existing coverage)
```

**Output**:
```python
# Segments unchanged (inclusions not added)
final_segments = final_segments  # Same as input

# Logged to review queue for future implementation
```

**Edge Cases**:
- Inclusion adds a completely different waterbody type (e.g., lake to river regulation) → logged
- Inclusion has complex scope → logged with full context for future implementation
- Inclusion references waterbody already in global scope → logged as redundant

#### Step 5: Attach Metadata to Segments

**Input**: `final_segments`, `exclusions_data`, `unlinked_inclusions`

**Process**:

For each FWA segment in `final_segments`:

```python
segment_metadata = {
    "segment_id": segment_id,
    "regulations": [regulation.id],
    "included_reason": "main_stem" | "tributary",  # How it was included
    "warnings": [],
    "exclusions_pending": []  # Phase 1 ready exclusions
}

# Attach warnings for ALL exclusions (ready + needs_review)
for exclusion in all_exclusions:
    segment_metadata["warnings"].append({
        "type": "unlinked_exclusion",
        "waterbody": exclusion.waterbody_key,
        "message": "...",
        "phase_1_ready": exclusion.status == "ready_to_activate"
    })

# Track which exclusions could affect this segment in Phase 1
for ready_exclusion in exclusions_data["ready_to_activate"]:
    # Check if this segment would be excluded when Phase 1 activates
    if segment_in_exclusion_scope(segment_id, ready_exclusion):
        segment_metadata["exclusions_pending"].append(ready_exclusion)

store_segment_metadata(segment_id, segment_metadata)
```

**Output**:
```python
# Complete segment index
segment_index = {
    "fwa_segment_002": {
        "regulations": [regulation_id],
        "included_reason": "tributary",
        "warnings": [
            {
                "type": "unlinked_exclusion",
                "waterbody": "Michel Creek",
                "message": "Michel Creek is listed as an exception...",
                "phase_1_ready": true
            },
            {
                "type": "unlinked_exclusion",
                "waterbody": "Mystery Creek",
                "message": "Mystery Creek is listed as an exception...",
                "phase_1_ready": false
            }
        ],
        "exclusions_pending": [
            {"waterbody": "Michel Creek", "fwa_id": "fwa_michel_001"}
        ]
    }
}
```

---

## Scope Defaulting Rules

### When Complex Scopes are Encountered

**MVP Behavior**: Any scope type other than WHOLE_SYSTEM or TRIBUTARIES_ONLY defaults to WHOLE_SYSTEM.

```python
def apply_global_scope(main_waterbody, global_scope):
    """
    MVP implementation of scope application
    """
    if global_scope.type in ["WHOLE_SYSTEM", "TRIBUTARIES_ONLY"]:
        # Supported scopes
        return find_segments(main_waterbody, global_scope.type, 
                           global_scope.includes_tributaries)
    
    else:
        # Complex scope - default to WHOLE_SYSTEM
        segments = find_segments(main_waterbody, "WHOLE_SYSTEM", 
                                global_scope.includes_tributaries)
        
        # Attach warning to ALL segments
        warning = {
            "type": "simplified_scope",
            "original_scope": global_scope.location_verbatim,
            "applied_scope": f"entire {main_waterbody.name} " + 
                           ("system [including tributaries]" if global_scope.includes_tributaries 
                            else "[main stem only]"),
            "reason": f"{global_scope.type} scope not implemented in MVP - defaulting to WHOLE_SYSTEM",
            "impact": "This regulation may not apply to entire system. Verify before fishing."
        }
        
        for segment_id in segments:
            attach_warning(segment_id, warning)
        
        return segments
```

### Example: DIRECTIONAL Scope

**Input**:
```python
regulation = {
    "identity": {
        "waterbody_key": "THOMPSON RIVER",
        "global_scope": {
            "type": "DIRECTIONAL",
            "direction": "DOWNSTREAM",
            "landmark": "Kamloops Lake",
            "includes_tributaries": true
        }
    }
}
```

**MVP Behavior**:
1. Link "THOMPSON RIVER" → `fwa_thompson_001`
2. Ignore DIRECTIONAL scope, default to WHOLE_SYSTEM
3. Apply `includes_tributaries=true` → include main stem + all tributaries
4. Attach warning to ALL segments:
   - "Regulation specifies 'downstream of Kamloops Lake' but MVP applies to entire Thompson River system. Verify before fishing."

**Result**: 5,000 segments with regulation + warnings (instead of 500 segments accurately scoped)

---

## Data Structures

### Regulation Processing Result

```python
{
    "regulation_id": "reg_001",
    "status": "linked" | "pending_review" | "failed",
    "main_waterbody": {
        "linked": true,
        "fwa_id": "fwa_thompson_001",
        "name": "Thompson River",
        "geometry_type": "MULTILINESTRING",
        "confidence": 100
    },
    "scope_applied": {
        "type": "WHOLE_SYSTEM",
        "includes_tributaries": true,
        "original_type": "DIRECTIONAL",  # What was requested
        "simplified": true  # Was it simplified?
    },
    "scope_simplifications": [
        {
            "type": "global_scope",
            "original": "downstream of Kamloops Lake [including tributaries]",
            "simplified_to": "entire system [including tributaries]",
            "reason": "directional scope not implemented"
        }
    ],
    "exclusions": {
        "ready_to_activate": [
            {
                "waterbody": "Michel Creek",
                "fwa_id": "fwa_michel_001",
                "geometry_type": "MULTILINESTRING",
                "includes_tributaries": true,
                "text": "Michel Creek**[Includes Tributaries]**",
                "status": "linked_not_applied",
                "estimated_segments": 50  # How many segments would be excluded
            }
        ],
        "needs_review": [
            {
                "waterbody": "Mystery Creek",
                "candidates": [],
                "text": "Mystery Creek",
                "status": "unlinked",
                "reason": "No FWA feature found"
            },
            {
                "waterbody": "Mill Creek",
                "candidates": [
                    {"fwa_id": "fwa_mill_001", "name": "Mill Creek", "region": "Region 3"},
                    {"fwa_id": "fwa_mill_002", "name": "Mill Creek", "region": "Region 3"},
                    {"fwa_id": "fwa_mill_003", "name": "Mill Creek", "region": "Region 3"}
                ],
                "text": "Mill Creek",
                "status": "ambiguous",
                "reason": "Multiple FWA features match"
            }
        ]
    },
    "inclusions": [
        {
            "waterbody": "Cedar Lake",
            "text": "Cedar Lake",
            "status": "not_implemented_mvp",
            "logged_to_review": true
        }
    ],
    "linked_segments": [
        "fwa_thompson_001",
        "fwa_thompson_002",
        ...
    ],
    "segment_count": 5000,
    "warnings_attached": 15000,  # Count of warning instances (3 per segment)
    "phase_1_impact": {
        "segments_to_exclude": 50,  # From ready exclusions
        "warnings_to_remove": 5000  # 1 warning per segment for Michel Creek
    }
}
```

### Manual Review Queue Entry

```python
{
    "priority": "CRITICAL" | "HIGH" | "MEDIUM" | "LOW",
    "type": "main_waterbody_linking" | "disambiguation" | "inclusion" | "complex_scope",
    "regulation_id": "reg_001",
    "issue": "No FWA feature found",
    "context": {
        "waterbody_key": "Mystery Creek System",
        "identity_verbatim": "MYSTERY CREEK SYSTEM [including tributaries]",
        "region": "Region 3",
        "candidates": []  # Empty if no matches, populated if multiple
    },
    "suggested_fixes": [
        "Check for spelling variations",
        "Search historical names",
        "Use fuzzy matching tool: suggest_typo_candidates('Mystery Creek System', fwa_database)"
    ],
    "blocked_regulations": 1,  # How many regulations can't proceed
    "created_at": "2026-01-28T10:30:00Z"
}
```

### Segment Metadata Storage

```python
{
    "segment_id": "fwa_thompson_downstream_001",
    "waterbody_name": "Thompson River",
    "geometry_type": "MULTILINESTRING",
    "region": "Region 3",
    "regulations": {
        "provincial": ["prov_001", "prov_002"],
        "zonal": ["zone_r3_001"],
        "waterbody_specific": ["reg_001", "reg_002"]
    },
    "warnings": [
        {
            "type": "simplified_scope",
            "regulation_id": "reg_001",
            "message": "Regulation specifies 'downstream of Kamloops Lake' but applies to entire system",
            "severity": "WARNING"
        },
        {
            "type": "unlinked_exclusion",
            "regulation_id": "reg_001",
            "waterbody": "Michel Creek",
            "message": "Michel Creek is listed as an exception but not excluded",
            "phase_1_ready": true,
            "severity": "WARNING"
        }
    ],
    "exclusions_pending": [
        {
            "regulation_id": "reg_001",
            "waterbody": "Michel Creek",
            "fwa_id": "fwa_michel_001",
            "will_remove_in_phase_1": false  # This segment not in Michel Creek
        }
    ]
}
```

---

## Success Metrics

### Coverage Metrics

- **Main waterbody linking success rate**: % of regulations with main waterbody successfully linked
  - Target: >95%
  - Critical: <80% requires investigation
  
- **Segment coverage**: % of FWA segments receiving at least one regulation
  - Target: 100% (via provincial/zonal regulations)
  
- **Regulation processing rate**: % of regulations fully processed (vs manual review)
  - Target: >90%
  - Track separately: critical failures vs low-priority (inclusions)

### Quality Metrics

- **Warning density**: Average warnings per segment
  - MVP baseline: ~5-10 warnings per segment
  - Target for Phase 1: <3 warnings per segment
  
- **Manual review queue size**: Count of regulations needing human intervention
  - Track by priority: CRITICAL, HIGH, MEDIUM, LOW
  - Goal: CRITICAL queue size → 0
  
- **Linking confidence**: Distribution of match quality
  - 100% confidence: single exact match
  - 0% confidence: no match or multiple matches

### Exclusion Readiness (Phase 1 Planning)

- **Ready exclusions**: % of exclusions with single match
  - Typical: 30-50% of exclusions
  - These activate automatically in Phase 1
  
- **Ambiguous exclusions**: % needing disambiguation
  - Typical: 20-30% of exclusions
  - Require manual review workflow
  
- **Unlinked exclusions**: % with no matches
  - Typical: 20-40% of exclusions
  - Require name variation curation
  
- **Estimated Phase 1 impact**:
  - Segments to exclude: sum of ready exclusion segment counts
  - Warnings to remove: segments × ready exclusions count
  - Expected warning reduction: ~40-60%

### Tracking Dashboard Example

```python
mvp_metrics = {
    "timestamp": "2026-01-28T10:30:00Z",
    "total_regulations": 1500,
    "linked": 1350,  # 90%
    "pending_review": 150,  # 10%
    "failed": 0,
    
    "manual_review_queue": {
        "CRITICAL": 120,  # Main waterbody linking failures
        "HIGH": 0,
        "MEDIUM": 0,
        "LOW": 30  # Inclusions
    },
    
    "segment_stats": {
        "total_segments": 50000,
        "segments_with_regulations": 50000,  # 100%
        "avg_warnings_per_segment": 8.5,
        "segments_with_phase_1_pending": 15000  # 30%
    },
    
    "exclusion_stats": {
        "total_exclusions": 500,
        "ready_to_activate": 200,  # 40%
        "needs_disambiguation": 150,  # 30%
        "needs_linking": 150,  # 30%
        "estimated_phase_1_segment_reduction": 5000,  # 10% of coverage
        "estimated_phase_1_warning_reduction": 200000  # 40% of warnings
    }
}
```

---

## Edge Cases & Error Handling

### Main Waterbody Linking

**Edge Case**: Waterbody name matches multiple geometry types (e.g., "Adams Lake" → polygon + multilinestring)
- **Handling**: Flag for manual review with all candidates shown
- **Future**: Could auto-select based on identity_type hint ("LAKE" → prefer polygon)

**Edge Case**: Regulation uses historical name not in current gazetteer
- **Handling**: No match → manual review → add to name variation database
- **Example**: "Babine Lake" spelled as "Babiene Lake" (typo)

**Edge Case**: Waterbody crosses regional boundaries
- **Handling**: FWA features typically split at boundaries, match to primary region
- **Note**: Segment-level region tracking handles cross-boundary cases

### Tributary Traversal

**Edge Case**: Circular references in FWA graph (very rare, usually data errors)
- **Handling**: Use visited set in traversal algorithm to prevent infinite loops
- **Logging**: Log circular reference for FWA data quality review

**Edge Case**: Very large tributary networks (e.g., Fraser River → 10,000+ segments)
- **Handling**: Algorithm works but may be slow
- **Optimization**: Consider caching upstream relationships

**Edge Case**: Disconnected segments within same waterbody
- **Handling**: Only include segments connected to main waterbody via graph traversal
- **Logging**: Log disconnected segments for investigation

### Exclusion Processing

**Edge Case**: Exclusion references waterbody outside global scope
- **Handling**: Still attempt to link and categorize
- **Phase 1**: Would exclude segments not in global scope (no-op, but logged)
- **Reason**: May catch parser errors or unusual regulation structures

**Edge Case**: Same waterbody excluded multiple times in one regulation
- **Handling**: Deduplicate exclusions, keep first occurrence
- **Logging**: Log duplicate for regulation quality review

**Edge Case**: Exclusion has complex scope (e.g., "Michel Creek upstream of bridge")
- **MVP Handling**: Link "Michel Creek", ignore scope
- **Phase 2**: Will need to implement directional filtering

### Scope Defaulting

**Edge Case**: Regulation specifies TRIBUTARIES_ONLY but includes_tributaries=false
- **Handling**: Invalid combination - default to TRIBUTARIES_ONLY (must include tributaries)
- **Warning**: "Conflicting scope settings - using TRIBUTARIES_ONLY"
- **Logging**: Log for regulation quality review

**Edge Case**: Regulation has no global scope specified
- **Handling**: Default to WHOLE_SYSTEM with includes_tributaries=false (most restrictive)
- **Warning**: "No scope specified - defaulting to main waterbody only"

### Warning Attachment

**Edge Case**: Segment receives 20+ warnings (many exclusions + scope simplification)
- **Handling**: Attach all warnings, but may need UI to summarize
- **Future**: Group similar warnings, show count

**Edge Case**: Warning text too long for storage/display
- **Handling**: Truncate message, store full text separately
- **UI**: Show summary with "more details" link

---

## Next Steps

The MVP provides a solid foundation with maximum coverage and abundant warnings. The next phases will progressively improve accuracy and reduce warning noise:

**Phase 1: Easy Exclusions** (Immediate Post-MVP)
- Activate single-match exclusions already linked in MVP
- Biggest impact: ~40-60% reduction in warnings
- No new linking work required - just apply stored exclusions

**Phase 2: Complex Scopes** (Medium Priority)
- Implement DIRECTIONAL, SEGMENT, BUFFER scopes
- Requires landmark linking system
- Reduces over-application of regulations

**Phase 3: Disambiguation & Manual Review Workflow** (Lower Priority)
- Build UI for manual review queue
- Disambiguation workflow for ambiguous exclusions
- Name variation database management

**Phase 4: Inclusions & Advanced Features** (Lowest Priority)
- Implement inclusion addition
- Rule-level scope modifications
- Type-based filtering

See [POST_MVP_ROADMAP.md](POST_MVP_ROADMAP.md) for detailed implementation plans for each phase.

For information on how regulations cascade and override each other, see [REGULATION_CASCADING.md](REGULATION_CASCADING.md).
