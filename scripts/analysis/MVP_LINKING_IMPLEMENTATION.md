# MVP Waterbody Linking Implementation

## Overview

This document defines the **Minimum Viable Product (MVP)** implementation for linking parsed fishing regulations to FWA (Freshwater Atlas) stream network segments. The MVP takes a conservative, warning-heavy approach to maximize coverage while flagging uncertainties.

**Core Principle**: Better to show a rule to too many segments with warnings than to miss segments where the rule applies.

---

## Data Model Reference

The linking system works with the parsed regulation data model defined in [models.py](c:\\Users\\DawsonHorvath\\Documents\\Workspace\\BC-freshwater-fishing-regulations\\scripts\\synopsis_pipeline\\models.py):

### Key Classes

- **ParsedWaterbody**: Top-level regulation object
  - `identity` (IdentityObject): Waterbody identification and spatial scope
  - `regs_verbatim` (str): Exact regulation text
  - `audit_log` (List[str]): Parsing issues/notes
  - `rules` (List[RuleGroup]): Individual regulation rules

- **IdentityObject**: Waterbody identity and global scope
  - `waterbody_key` (str): Main waterbody name for FWA linking (e.g., "ELK RIVER")
  - `identity_type` (str): STREAM, STILL_WATER, ADMINISTRATIVE_AREA, etc.
  - `global_scope` (ScopeObject): Master spatial constraint
  - `exclusions` (List[ScopeObject]): Geographic exclusions
  - `inclusions` (List[ScopeObject]): Geographic inclusions
  - `component_waterbodies` (List[str]): For MULTIPLE_WATERBODIES type
  - `location_descriptor` (Optional[str]): Disambiguation info
  - `alternate_names` (List[str]): Former/alternate names

- **ScopeObject**: Spatial scope definition
  - `type` (str): WHOLE_SYSTEM, DIRECTIONAL, SEGMENT, TRIBUTARIES_ONLY, BUFFER, NAMED_PART, VAGUE
  - `waterbody_key` (str): "ALL" or specific waterbody component
  - `includes_tributaries` (Optional[bool]): Tri-state flag (True/False/None)
  - `location_verbatim` (Optional[str]): Exact location text from regulation
  - `landmark_verbatim` (Optional[str]): Spatial reference point
  - `landmark_end_verbatim` (Optional[str]): End point for SEGMENT type
  - `direction` (Optional[str]): UPSTREAM, DOWNSTREAM, NORTH_OF, etc.

- **RuleGroup**: Atomic Regulation Unit (ARU)
  - `rule_text_verbatim` (str): Exact rule text
  - `scope` (ScopeObject): Where this specific rule applies
  - `restriction` (RestrictionObject): Legal restriction details

### Linking Process Flow

```
ParsedWaterbody
    └─> identity.waterbody_key ──┐
                                  ├─> [MVP LINKING] ─> FWA Feature
                                  │
    └─> identity.global_scope ────┤
        ├─> type                  │
        └─> includes_tributaries  ├─> [TRIBUTARY ENRICHMENT] ─> FWA Segment IDs
                                  │
    └─> identity.exclusions ──────┘
        (linked but not applied in MVP)
```

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
| WHOLE_SYSTEM scope         | ✅         | Links to entire waterbody, respects includes_tributaries flag             |
| TRIBUTARIES_ONLY scope     | ✅         | Links to all upstream tributaries, excludes main stem                     |
| Includes tributaries flag  | ✅         | Controls whether tributaries are included in scope                        |

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

- **ParsedWaterbody** object containing:
  - `identity.waterbody_key` (str): Main waterbody name for linking
  - `identity.global_scope` (ScopeObject): Contains type and includes_tributaries
    - `global_scope.type` (str): One of WHOLE_SYSTEM, TRIBUTARIES_ONLY, DIRECTIONAL, SEGMENT, etc.
    - `global_scope.includes_tributaries` (Optional[bool]): Tri-state flag
    - `global_scope.waterbody_key` (str): "ALL" or specific component
  - `identity.exclusions` (List[ScopeObject]): Geographic exclusions
  - `identity.inclusions` (List[ScopeObject]): Geographic inclusions
  - `rules` (List[RuleGroup]): Individual regulation rules

### Output

- Set of FWA segment IDs that receive the regulation
- Categorized exclusions (ready/needs_review)
- Unlinked exclusion warnings attached to each segment
- Unlinked inclusion records for manual review

### Step-by-Step Algorithm

#### Step 1: Link Main Waterbody

**Input**: `parsed_waterbody.identity.waterbody_key` (str) - e.g., "ELK RIVER"

**Process**:
```python
def link_main_waterbody(parsed_waterbody: ParsedWaterbody) -> Optional[FWAFeature]:
    waterbody_key = parsed_waterbody.identity.waterbody_key
    region = parsed_waterbody.identity.location_descriptor  # May help with disambiguation
    
    # 1. Look up regional name variation
    normalized_name = lookup_name_variation(waterbody_key, region)
    
    # 2. Apply name normalization
    normalized_name = normalize_waterbody_name(normalized_name)
    
    # 3. Search FWA gazetteer database for exact match
    matches = fwa_gazetteer.search(normalized_name)
    
    # 4. Handle disambiguation
    if len(matches) == 1:
        return matches[0]  # ✅ success
    elif len(matches) > 1:
        queue_for_manual_review(parsed_waterbody, matches)  # ❌ ambiguous
        return None
    else:
        queue_for_manual_review(parsed_waterbody, [])  # ❌ not found
        return None
```

**Output**: FWA feature object (or None if failed)

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
- `candidate_segments` (List[str]) from step 2
- `parsed_waterbody.identity.exclusions` (List[ScopeObject])

**Process**:

For each exclusion in `identity.exclusions`:

**3a. Attempt to Link Exclusion**
```python
def process_exclusions_mvp(parsed_waterbody: ParsedWaterbody,
                          candidate_segments: List[str]) -> Dict[str, Any]:
    """
    Link and categorize exclusions but DO NOT apply them (MVP behavior).
    
    Returns metadata structure for Phase 1 activation.
    """
    ready_to_activate = []
    needs_review = []
    
    for exclusion_scope in parsed_waterbody.identity.exclusions:
        # Extract waterbody name from exclusion scope
        exclusion_waterbody = exclusion_scope.waterbody_key
        
        # Attempt linking (same process as main waterbody)
        matches = link_waterbody(exclusion_waterbody, 
                                parsed_waterbody.identity.location_descriptor)
        
        if len(matches) == 1:
            # Single match - ready for Phase 1
            ready_to_activate.append({
                "scope_object": exclusion_scope,
                "fwa_feature": matches[0],
                "waterbody_key": exclusion_waterbody,
                "includes_tributaries": exclusion_scope.includes_tributaries,
                "location_verbatim": exclusion_scope.location_verbatim
            })
        else:
            # Multiple/no matches - needs manual review
            needs_review.append({
                "scope_object": exclusion_scope,
                "waterbody_key": exclusion_waterbody,
                "candidates": matches,
                "reason": "ambiguous" if len(matches) > 1 else "not_found"
            })
    
    # Attach warnings to ALL candidate segments (MVP behavior)
    for segment_id in candidate_segments:
        attach_warning(segment_id, {
            "type": "exclusions_not_applied",
            "count": len(parsed_waterbody.identity.exclusions),
            "ready_count": len(ready_to_activate),
            "message": f"{len(parsed_waterbody.identity.exclusions)} exclusions exist but not yet applied"
        })
    
    return {
        "ready_to_activate": ready_to_activate,
        "needs_review": needs_review
    }
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
- `parsed_waterbody.identity.inclusions` (List[ScopeObject])

**Process**:

For each inclusion_scope in `identity.inclusions`:

```python
# MVP: Log all inclusions to manual review queue, don't add segments
manual_review_queue.append({
    "type": "unlinked_inclusion",
    "regulation_id": parsed_waterbody.identity.name_verbatim,
    "scope_object": inclusion_scope,
    "waterbody_key": inclusion_scope.waterbody_key,
    "location_verbatim": inclusion_scope.location_verbatim,
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

## Complete End-to-End Example

### Input: Parsed Regulation (ELK RIVER'S TRIBUTARIES)

```python
from synopsis_pipeline.models import ParsedWaterbody, IdentityObject, ScopeObject, RuleGroup

parsed_waterbody = ParsedWaterbody(
    identity=IdentityObject(
        name_verbatim="ELK RIVER'S TRIBUTARIES (see exceptions)",
        waterbody_key="ELK RIVER",
        identity_type="TRIBUTARIES",
        component_waterbodies=[],
        alternate_names=[],
        location_descriptor="Region 4",
        notes="see exceptions",
        global_scope=ScopeObject(
            type="TRIBUTARIES_ONLY",
            waterbody_key="ALL",
            includes_tributaries=None,  # Not applicable for TRIBUTARIES_ONLY
            location_verbatim=None,
            landmark_verbatim=None,
            landmark_end_verbatim=None,
            direction=None
        ),
        exclusions=[
            ScopeObject(  # Exclusion 1
                type="WHOLE_SYSTEM",
                waterbody_key="MICHEL CREEK",
                includes_tributaries=True,
                location_verbatim="Michel Creek",
                landmark_verbatim=None,
                landmark_end_verbatim=None,
                direction=None
            ),
            ScopeObject(  # Exclusion 2
                type="DIRECTIONAL",
                waterbody_key="ALEXANDER CREEK",
                includes_tributaries=True,
                location_verbatim="Alexander Creek upstream of Hwy 3 bridge",
                landmark_verbatim="Hwy 3 bridge",
                landmark_end_verbatim=None,
                direction="UPSTREAM"
            ),
            ScopeObject(  # Exclusion 3
                type="WHOLE_SYSTEM",
                waterbody_key="ABRUZZI CREEK",
                includes_tributaries=False,
                location_verbatim="Abruzzi Creek",
                landmark_verbatim=None,
                landmark_end_verbatim=None,
                direction=None
            )
        ],
        inclusions=[]
    ),
    regs_verbatim="Trout/char daily quota = 2...",
    audit_log=[],
    rules=[
        RuleGroup(
            rule_text_verbatim="Trout/char daily quota = 2",
            scope=ScopeObject(
                type="WHOLE_SYSTEM",
                waterbody_key="ALL",
                includes_tributaries=None,
                location_verbatim=None,
                landmark_verbatim=None,
                landmark_end_verbatim=None,
                direction=None
            ),
            restriction=RestrictionObject(
                type="harvest",
                details="Trout/char daily quota = 2",
                dates=None
            )
        )
    ]
)
```

### MVP Processing Steps

**Step 1: Link Main Waterbody**
```python
waterbody_key = parsed_waterbody.identity.waterbody_key  # "ELK RIVER"
matches = fwa_gazetteer.search("Elk River", region="Region 4")
# Result: [fwa_elk_river_001] (single match ✅)

linked_fwa_feature = matches[0]
status = "SUCCESS"
```

**Step 2: Tributary Enrichment**
```python
global_scope = parsed_waterbody.identity.global_scope
# scope.type = "TRIBUTARIES_ONLY"
# scope.includes_tributaries = None (N/A for this type)

# Find all upstream tributaries, exclude main stem
segments = find_all_upstream_tributaries(linked_fwa_feature, exclude_mainstem=True)
# Result: 5,000 segment IDs
```

**Step 3: Process Exclusions (Link Only)**
```python
exclusion_results = process_exclusions_mvp(parsed_waterbody, segments)

# Exclusion 1: Michel Creek
fwa_matches_1 = link_waterbody("MICHEL CREEK", "Region 4")
# → 1 match → ready_to_activate ✅
# Would exclude: 50 segments (creek + tributaries)

# Exclusion 2: Alexander Creek upstream of Hwy 3 bridge  
fwa_matches_2 = link_waterbody("ALEXANDER CREEK", "Region 4")
# → 1 match → ready_to_activate ✅
# Scope: DIRECTIONAL (not implemented → simplified to WHOLE_SYSTEM)
# Would exclude: 30 segments (entire creek, not just upstream - Phase 2 needed)

# Exclusion 3: Abruzzi Creek
fwa_matches_3 = link_waterbody("ABRUZZI CREEK", "Region 4")
# → 1 match → ready_to_activate ✅
# Would exclude: 5 segments (main stem only, no tributaries)

exclusion_metadata = {
    "ready_to_activate": [
        {
            "scope_object": parsed_waterbody.identity.exclusions[0],
            "fwa_feature": fwa_michel_001,
            "waterbody_key": "MICHEL CREEK",
            "includes_tributaries": True
        },
        {
            "scope_object": parsed_waterbody.identity.exclusions[1],
            "fwa_feature": fwa_alexander_001,
            "waterbody_key": "ALEXANDER CREEK",
            "includes_tributaries": True,
            "warning": "DIRECTIONAL scope simplified to WHOLE_SYSTEM"
        },
        {
            "scope_object": parsed_waterbody.identity.exclusions[2],
            "fwa_feature": fwa_abruzzi_001,
            "waterbody_key": "ABRUZZI CREEK",
            "includes_tributaries": False
        }
    ],
    "needs_review": []
}

# MVP: Exclusions NOT applied
# Segments remain: 5,000 (no change)
```

**Step 4: Attach Warnings**
```python
# ALL 5,000 segments get this warning:
for segment_id in segments:
    attach_warning(segment_id, {
        "type": "exclusions_not_applied",
        "count": 3,
        "ready_count": 3,
        "message": "3 exclusions exist but not yet applied: Michel Creek, Alexander Creek, Abruzzi Creek. These areas may be excepted from this regulation."
    })
```

### MVP Output

```python
mvp_output = {
    "parsed_waterbody": parsed_waterbody,
    "link_status": "SUCCESS",
    "linked_fwa_feature": fwa_elk_river_001,
    "segments": list(segments),  # 5,000 segment IDs
    "exclusions": exclusion_metadata,
    "inclusions": [],
    "warnings_per_segment": 1,
    "total_warnings": 5000,
    "metadata": {
        "scope_type": "TRIBUTARIES_ONLY",
        "scope_simplified": False,
        "exclusions_total": 3,
        "exclusions_ready": 3,
        "exclusions_needs_review": 0,
        "phase_1_preview": {
            "segments_would_be_excluded": 85,
            "warnings_would_be_removed": 5000
        }
    }
}
```

### What Phase 1 Changes

```python
# When Phase 1 is enabled:
final_segments, excluded = apply_phase_1_exclusions(mvp_output)

# Excluded:
#   - Michel Creek: 50 segments (main stem + tributaries)
#   - Alexander Creek: 30 segments (ENTIRE creek - DIRECTIONAL scope simplified)
#   - Abruzzi Creek: 5 segments (main stem only)
#   - Total: 85 segments

# Final result:
#   - Segments: 4,915 (instead of 5,000)
#   - Warnings: 0 (instead of 5,000)
#   - Accuracy: Improved by 1.7%
#   - Warning reduction: 100%
```

### What Phase 2 Changes (DIRECTIONAL Scope)

```python
# When Phase 2 is enabled:
# Alexander Creek exclusion becomes more accurate

# Current (MVP & Phase 1): Excludes entire Alexander Creek (30 segments)
# Phase 2: Excludes only upstream of Hwy 3 bridge (15 segments)

# Improvement: 15 segments restored, more accurate regulation coverage
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
