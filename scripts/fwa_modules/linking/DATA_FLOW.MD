Excellent! This simplifies things a LOT. Let me help you decide on the index structure, then design a clean data flow.

## Decision: Index Structure (B vs C)

**Recommendation: Option C - Composite Key in Single Index**

**Why?**
- ✅ **Single file to maintain** - simpler deployment, single source of truth
- ✅ **Type is explicit in the key** - `STREAM_700123456` vs `LAKE_18851` - immediately clear
- ✅ **Easier querying** - user clicks a feature, you already know type+id, just build the key
- ✅ **Future-proof** - can add more types (WETLAND, GLACIER, etc.) without new files
- ✅ **Atomic operations** - single JSON file for atomic reads/writes

**Structure**:
```json
{
  "STREAM_700123456": {
    "feature_type": "STREAM",
    "waterbody_poly_id": "700123456",
    "gnis_id": "2311",
    "gnis_name": "WIGWAM RIVER",
    "watershed_code": "100.234567",
    "regulation_ids": ["reg_001", "reg_002"],
    "geometry": {...}
  },
  "LAKE_18851": {
    "feature_type": "LAKE",
    "waterbody_poly_id": "18851",
    "gnis_id": "18851",
    "gnis_name": "KOOTENAY LAKE",
    "watershed_code": null,
    "regulation_ids": ["reg_045"],
    "geometry": {...}
  }
}
```

**Query time**: `clicked_feature → key = f"{feature.type}_{feature.waterbody_poly_id}" → index[key]`

---

## Updated Data Flow - Clean Class Separation

**Pipeline Stage**: BUILD TIME (offline processing)

```
┌─────────────────────────────────────────────────────────────────┐
│                         INPUT: Parsed Regulations               │
│  [{identity: {...}, rules: [...], ...}, ...]                    │
│                                                                  │
│  Data: ~2000 regulation objects from synopsis parsing           │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  [BUILD TIME - STEP 1]     WaterbodyLinker                      │
│                                                                  │
│  Input:  regulation.identity (name_verbatim, waterbody_key, etc)│
│  Output: MatchResult with List[Feature]                         │
│                                                                  │
│  Data Transform: Regulation identity → FWA feature objects      │
│  Example: "WIGWAM RIVER" → 500 stream segments                  │
│                                                                  │
│  Strategies:                                                     │
│  - DirectMatch: Manual GNIS/watershed/poly_id lookup            │
│  - MetadataMatch: BC Lakes Database lookup (fuzzy name search)  │
│  - PolygonMatch: Admin boundary feature lists (manual configs)  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼ MatchResult(matched_features: 500 features)
                             │
┌─────────────────────────────────────────────────────────────────┐
│  [BUILD TIME - STEP 2]     RegulationMapper (Orchestrator)      │
│                                                                  │
│  Input:  All parsed regulations                                 │
│  Output: {feature_id: [regulation_ids]} inverted index          │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  PROCESSING FLOW (with safe fallbacks):                  │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│    500 features → [ScopeFilter: global_scope] → 200 scoped      │
│                      ↓                      (or 500 if no data) │
│    FOR EACH RULE:                                               │
│      200 global → [ScopeFilter: rule.scope] → 50 rule-scoped    │
│                      ↓                      (or 200 if no data) │
│      IF rule.includes_tributaries (or global if null):          │
│        50 scoped → [TributaryEnricher] → 75 enriched            │
│        Map THIS RULE to 75 enriched features                    │
│      ELSE:                                                      │
│        Map THIS RULE to 50 scoped features (no enrichment)      │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  SAFE FALLBACK BEHAVIOR (ScopeFilter):                   │   │
│  │  • Landmark not found → return all input features        │   │
│  │  • Scope type not implemented → WHOLE_SYSTEM             │   │
│  │  • includes_tributaries ALWAYS respected (not complex)   │   │
│  │                                                           │   │
│  │  MATCH FAILURES (WaterbodyLinker):                       │   │
│  │  • Polygon match config missing → skip regulation + warn │   │
│  │  • No matching strategy succeeds → skip regulation + warn│   │
│  │                                                           │   │
│  │  "MVP" = incomplete landmark data, so many scopes fall   │   │
│  │  back to WHOLE_SYSTEM (still maps correctly though)      │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  Data Transform: regulation → features  TO  feature → regulations│
│  Example:                                                        │
│    Before: reg_001 → [feat_A, feat_B], reg_002 → [feat_B, feat_C]│
│    After:  feat_A → [reg_001], feat_B → [reg_001, reg_002]      │
└────────────────────────────┬────────────────────────────────────┘
                             │
        ┌────────────────────┴────────────────────┐
        │                                         │
        ▼ Uses:                                   ▼ Uses:
┌─────────────────────┐               ┌─────────────────────────┐
│   ScopeFilter       │               │  TributaryEnricher      │
│                     │               │                         │
│ Spatial constraints │               │ Graph traversal         │
│ • WHOLE_SYSTEM      │               │ • Memoization cache     │
│ • DIRECTIONAL       │               │ • Fallback logic        │
│ • SEGMENT           │               │ • Conditional enrichment│
│ • BUFFER            │               │                         │
│ • NAMED_PART        │               │ Logic:                  │
│ • VAGUE (fallback)  │               │ IF includes_tributaries:│
│                     │               │   Add upstream features │
└─────────────────────┘               └─────────────────────────┘
                             │
                             ▼ FeatureRegMap: {feature_id: [reg_ids]}
                             │   Individual features → merged groups
┌─────────────────────────────────────────────────────────────────┐
│  [BUILD TIME - STEP 3]     FeatureMerger                        │
│                                                                  │
│  Input:  {feature_id: [reg_ids]} for ~500k features             │
│  Output: Merged groups with combined geometries                 │
│                                                                  │
│  Data Transform: Individual segments → merged geometries        │
│  Example: 200 adjacent segments with same regs → 1 LineString   │
│                                                                  │
│  Grouping Key: (gnis_id OR watershed_code, regulation_set)      │
│  - Reduces ~500k features to ~50k groups for UI rendering       │
│  - Preserves original mapping in feature_to_regs.json           │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼ MergedGroups: {group_id: {features, regs, geom}}
                             │   In-memory → persisted JSON files
┌─────────────────────────────────────────────────────────────────┐
│  [BUILD TIME - STEP 4]     RegulationIndexBuilder               │
│                                                                  │
│  Input:  feature_to_regs dict + merged_groups dict              │
│  Output: TWO JSON files written to disk                         │
│                                                                  │
│  Data Transform: Python objects → JSON files                    │
│                                                                  │
│  Files Created:                                                  │
│  1. feature_to_regs.json - Individual feature lookup (500k keys)│
│  2. merged_features.json - Grouped geometries (50k groups)      │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    OUTPUT: Query-Ready Indices                  │
│  - feature_to_regs.json (O(1) feature → regulations lookup)     │
│  - merged_features.json (optimized for UI rendering)            │
│  - regulation_metadata.json (regulation details)                │
│                                                                  │
│  **QUERY TIME**: User clicks feature → instant regulation lookup│
└─────────────────────────────────────────────────────────────────┘
```

---

## Class Definitions

### 1. WaterbodyLinker

```pseudocode
CLASS LinkingResult:
    status              // SUCCESS, AMBIGUOUS, NOT_FOUND, NOT_IN_DATA, IGNORED, ERROR
    matched_feature     // Single FWA feature (or null)
    matched_features    // List of FWA features (for multi-match success)
    candidate_features  // Ambiguous candidates (for AMBIGUOUS status)
    link_method         // "direct_match", "name_variation", "natural_search", "skip_entry"
    matched_name        // Actual name that matched (for name variations)
    error_message       // Human-readable explanation

CLASS WaterbodyLinker:
    // Matches regulation identities to FWA features
    
    ATTRIBUTES:
        gazetteer               // MetadataGazetteer (BC Lakes + streams)
        manual_corrections      // ManualCorrections (DirectMatch, NameVariation, SkipEntry, UnmarkedWaterbodies)
        stats                   // Counter tracking LinkStatus frequencies
        feature_to_regulations  // Dict tracking which regulations map to which features (for duplicate detection)
    
    METHOD link_waterbody(waterbody_key, region, mgmt_units, name_verbatim):
        // Main entry point - multi-stage linking workflow
        
        lookup_name = name_verbatim OR waterbody_key
        
        // STEP 0: Check SkipEntry (IGNORED or NOT_IN_DATA)
        IF region AND skip_entry exists:
            RETURN LinkingResult(IGNORED or NOT_IN_DATA with note)
        
        // STEP 1: Check DirectMatch (manual ID mapping)
        IF region AND direct_match exists:
            result = apply_direct_match(waterbody_key, direct_match)
            RETURN result with link_method="direct_match"
        
        // STEP 2: Check NameVariation (manual name corrections)
        IF region AND name_variation exists:
            result = link_with_name_variation(waterbody_key, name_var, region, mgmt_units)
            RETURN result with link_method="name_variation"
        
        // STEP 3: Natural gazetteer search (try multiple variations)
        search_variations = [
            name_verbatim,                          // Original
            remove_brackets(name_verbatim),         // "LAKE (Region 5)" → "LAKE"
            remove_brackets_and_quotes(name_verbatim) // Full cleanup
        ]
        
        FOR variation IN search_variations:
            result = natural_search(variation, region, mgmt_units)
            IF result.status != NOT_FOUND:
                BREAK
        
        // Track successful matches for duplicate detection
        IF result.status == SUCCESS:
            record_match(result, region, waterbody_key, name_verbatim)
        
        RETURN result with link_method="natural_search"
    
    METHOD get_duplicate_mappings():
        // Return FWA features that have multiple regulation names
        // Used to detect cross-listed regulations, alternate names, potential duplicates
        RETURN {identity_key: data WHERE len(data["regulations"]) > 1}
    
    METHOD natural_search(name, region, mgmt_units):
        // Search gazetteer by name with deduplication and MU filtering
        
        // 1. Search with region filter (fallback to all regions if no match)
        matches = gazetteer.search(name, region=region)
        IF no matches AND region specified:
            matches = gazetteer.search(name, region=null)
        
        // 2. Filter out KML points
        matches = [m WHERE m.geometry_type != "point"]
        
        // 3. Deduplicate by waterbody identity
        //    - Streams: group by fwa_watershed_code (multiple segments = one stream)
        //    - Lakes: group by gnis_id (multiple polygons = one lake)
        //    - Polygons: group by waterbody_key
        unique_matches = deduplicate_by_identity(matches)
        
        // 4. Filter by MU overlap if mgmt_units provided
        IF mgmt_units:
            unique_matches = filter_by_mu_overlap(unique_matches, mgmt_units)
        
        // 5. Validate region/MU consistency
        IF NOT validate_region_mu_match(region, unique_matches):
            RETURN LinkingResult(AMBIGUOUS, candidates=matches, error="Region/MU mismatch")
        
        // 6. Return based on match count
        IF len(unique_matches) == 0:
            RETURN LinkingResult(NOT_FOUND)
        ELSE IF len(unique_matches) == 1:
            // Check if GNIS lake with multiple polygons (needs direct match)
            IF is_gnis_lake AND multiple_polygons:
                RETURN LinkingResult(AMBIGUOUS, candidates=all_polygons)
            ELSE:
                RETURN LinkingResult(SUCCESS, matched_features=all_features_in_group)
        ELSE:
            RETURN LinkingResult(AMBIGUOUS, candidates=all_matches)


// NOTE: Polygon matching for administrative areas (parks, watersheds) 
// is NOT YET IMPLEMENTED in current code.
// 
// Future implementation would need:
// - PolygonMatchConfig class to define feature_ids for each park/watershed
// - apply_polygon_match() method to lookup features by spatial containment
// - Integration in link_waterbody() workflow (check identity_type == "ADMINISTRATIVE_AREA")
//
// For now, parks/watersheds can be handled via DirectMatch with explicit feature lists
```

### Helper Functions for Admin Boundaries (Future Implementation)

**NOTE**: These are planned features not yet implemented. Parks/watersheds currently handled via DirectMatch.

```pseudocode
FUNCTION get_all_features_in_polygon(polygon, feature_type):
    // Generate feature_ids for a polygon boundary
    // Used to create PolygonMatchConfig entries
    // feature_type: "LAKE", "STREAM", or null for all
    
    candidates = spatial_index.query(polygon.bounds)
    feature_ids = []
    
    FOR EACH feature IN candidates:
        IF polygon.contains(feature.centroid):
            IF feature_type is null OR feature.type == feature_type:
                feature_ids.ADD(feature.id)
    
    RETURN feature_ids


FUNCTION get_all_features_in_watershed(watershed_code, feature_type):
    // Get all features in a watershed by code
    // Used for watershed-based admin boundaries
    
    features = fwa_features.query(watershed_code)
    
    IF feature_type is not null:
        features = FILTER features WHERE type == feature_type
    
    RETURN feature.ids
```

---

### 2. ScopeFilter

```pseudocode
CLASS ScopeFilter:
    // Applies spatial scope constraints to filter features
    // Handles both global_scope and rule.scope
    
    ATTRIBUTES:
        graph               // FWA stream network graph
        landmarks           // Landmark lookup table (km markers, bridges, etc)
        scope_parser        // Parses scope constraints
    
    METHOD apply_scope(features, scope):
        // Main entry point - filters features by spatial constraint
        // Fallback: Returns all features if scope unclear (user can verify)
        
        SWITCH scope.type:
            CASE "WHOLE_SYSTEM":
                RETURN features  // No filtering needed
            
            CASE "DIRECTIONAL":
                RETURN filter_directional(features, scope)
            
            CASE "SEGMENT":
                RETURN filter_segment(features, scope)
            
            CASE "BUFFER":
                RETURN filter_buffer(features, scope)
            
            CASE "NAMED_PART":
                RETURN filter_named_part(features, scope)
            
            CASE "VAGUE":
                LOG WARNING "VAGUE scope: " + scope.location_verbatim
                RETURN features  // Safe fallback
            
            DEFAULT:
                LOG ERROR "Scope type not implemented: " + scope.type + ", defaulting to WHOLE_SYSTEM"
                RETURN features  // Safe fallback
    
    METHOD filter_directional(features, scope):
        // Filter by upstream/downstream of landmark
        
        landmark = find_landmark(scope.landmark_verbatim)
        IF landmark is null:
            LOG WARNING "Landmark not found: " + scope.landmark_verbatim
            RETURN features  // Safe fallback
        
        RETURN landmark.filter_features(features, scope.direction, graph)
    
    // Similar methods: filter_segment, filter_buffer, filter_named_part
```

---

### 3. TributaryEnricher

```pseudocode
CLASS TributaryEnricher:
    // Enriches feature set with tributaries/upstream features
    // Uses caching to avoid duplicate graph traversals
    
    ATTRIBUTES:
        graph                // FWA stream network graph
        enrichment_cache     // Memoization: feature_id → upstream_features
    
    METHOD enrich_with_tributaries(features, scope, global_scope):
        // Add upstream features if includes_tributaries flag is true
        // Fallback logic: scope → global_scope → false
        
        // 1. Determine includes_tributaries flag
        includes_tribs = scope.includes_tributaries
        IF includes_tribs is null AND global_scope exists:
            includes_tribs = global_scope.includes_tributaries
        IF includes_tribs is null:
            includes_tribs = false  // Default
        
        // 2. Early return if no enrichment needed
        IF NOT includes_tribs:
            RETURN features
        
        // 3. Find all upstream features (with caching)
        enriched = SET(features)
        
        FOR EACH feature IN features:
            feature_id = feature.composite_id
            
            // Check cache first (performance optimization)
            IF feature_id NOT IN enrichment_cache:
                upstream = graph.get_upstream_features(feature.id)
                enrichment_cache[feature_id] = upstream
            
            // Add cached upstream features to result
            enriched.ADD_ALL(enrichment_cache[feature_id])
        
        RETURN LIST(enriched)
    
    METHOD clear_cache():
        // Clear cache between regulation batches
        enrichment_cache.CLEAR()
```

---

### 4. RegulationMapper

```pseudocode
CLASS RegulationMapper:
    // Orchestrates the full pipeline: Link → Scope → Enrich → Map
    // Creates inverted index: feature_id → regulation_ids
    
    ATTRIBUTES:
        linker              // WaterbodyLinker instance
        scope_filter        // ScopeFilter instance
        tributary_enricher  // TributaryEnricher instance
    
    METHOD apply_scope_and_enrich(base_features, scope, global_scope):
        // Helper: Apply spatial filter + tributary enrichment
        // Encapsulates two-step process to avoid code duplication
        
        // Step 1: Spatial filtering (DIRECTIONAL/SEGMENT/BUFFER/etc)
        scoped_features = scope_filter.apply_scope(base_features, scope)
        
        // Step 2: Tributary enrichment (conditional on includes_tributaries)
        enriched_features = tributary_enricher.enrich_with_tributaries(
            scoped_features,
            scope,
            global_scope  // Fallback for includes_tributaries
        )
        
        RETURN enriched_features
    
    METHOD process_all_regulations(regulations):
        // Main processing loop - creates feature → regulation index
        // Always attempts rule-level scopes with safe fallbacks
        
        feature_to_regs = {}
        
        FOR EACH regulation IN regulations:
            // 1. Match waterbody identity to FWA features
            match_result = linker.match_regulation(regulation)
            
            IF match_result.status != SUCCESS:
                LOG WARNING "Failed to match: " + regulation.identity.name
                CONTINUE
            
            // 2. Apply global spatial scope filter (narrows search space)
            // Falls back to WHOLE_SYSTEM if landmarks/data missing
            globally_scoped = scope_filter.apply_scope(
                match_result.matched_features,
                regulation.identity.global_scope
            )
            
            // 3. Process each rule independently
            FOR EACH rule IN regulation.rules:
                // Apply rule spatial scope (refines global scope)
                // Falls back to globally_scoped if landmarks/data missing
                rule_scoped = scope_filter.apply_scope(
                    globally_scoped,
                    rule.scope
                )
                
                // Enrich with tributaries based on rule's includes_tributaries flag
                // (falls back to global scope's flag if rule's is null)
                rule_features = tributary_enricher.enrich_with_tributaries(
                    rule_scoped,
                    rule.scope,
                    global_scope = regulation.identity.global_scope
                )
                
                // Map this rule to its final feature set
                FOR EACH feature IN rule_features:
                    feature_key = feature.type + "_" + feature.id
                    feature_to_regs[feature_key].ADD(regulation.id + "_rule_" + rule.id)
        
        RETURN feature_to_regs


// PROCESSING FLOW:
//   Match → Global spatial scope (fallback: WHOLE_SYSTEM if no landmark data)
//         → For each rule:
//             → Rule spatial scope (fallback: use global scope if no landmark data)
//             → Enrich with tributaries (ALWAYS respected - not complex)
//             → Map rule to features
//
// FALLBACK BEHAVIOR:
//   • Missing landmarks → ScopeFilter returns all input features
//   • Scope type not implemented yet → WHOLE_SYSTEM
//   • includes_tributaries always applied (just determines enrichment, no external data needed)
//
// "MVP" REALITY:
//   MVP isn't a separate mode - it's just the system running with incomplete landmark/polygon data.
//   Many scopes will fall back to WHOLE_SYSTEM, but the architecture is identical.
```

---

### 5. FeatureMerger

```pseudocode
CLASS FeatureMerger:
    // Merges features with identical (grouping_key + regulation_set)
    // Reduces ~500k individual features to ~50k groups for UI
    
    METHOD merge_features(feature_to_regs):
        // Group by (gnis_id OR watershed_code, regulation_set)
        
        group_map = {}
        
        // 1. Build grouping key for each feature
        FOR EACH (feature_id, reg_ids) IN feature_to_regs:
            feature = get_feature(feature_id)
            reg_set = FROZEN_SET(reg_ids)
            
            // Determine grouping key (priority order)
            IF feature.gnis_id exists:
                group_key = ("gnis_" + feature.gnis_id, reg_set)
            ELSE IF feature.type == "STREAM":
                group_key = ("watershed_" + feature.watershed_code, reg_set)
            ELSE IF feature.type == "LAKE":
                group_key = ("waterbody_" + feature.waterbody_key, reg_set)
            ELSE:
                group_key = ("feature_" + feature_id, reg_set)  // Fallback
            
            group_map[group_key].ADD(feature_id)
        
        // 2. Create merged groups
        merged_groups = {}
        FOR EACH (group_key, feature_ids) IN group_map:
            features = [get_feature(id) for id in feature_ids]
            merged_geometry = merge_geometries(features)
            
            merged_groups["group_" + index] = {
                feature_ids: feature_ids,
                regulation_ids: LIST(reg_set),
                gnis_id: features[0].gnis_id,
                gnis_name: features[0].gnis_name,
                feature_type: features[0].type,
                geometry: merged_geometry
            }
        
        RETURN merged_groups


STRUCTURE MergedGroup:
    // Group of features with identical regulations
    feature_ids         // List of individual feature IDs in group
    regulation_ids      // List of regulation IDs applying to group
    gnis_id             // GNIS ID (if named waterbody)
    gnis_name           // GNIS name (if named waterbody)
    feature_type        // "STREAM" or "LAKE"
    geometry            // Merged LineString or MultiPolygon
```

---

### 6. RegulationIndexBuilder

```pseudocode
CLASS RegulationIndexBuilder:
    // Creates final query-ready indices for UI
    // Outputs two JSON files for different use cases
    
    METHOD build_index(feature_to_regs, merged_groups):
        // Persist indices to disk
        
        // 1. Individual feature lookup (O(1) queries)
        SAVE_JSON("feature_to_regs.json", feature_to_regs)
        
        // 2. Merged groups (optimized for UI rendering)
        SAVE_JSON("merged_features.json", merged_groups)
        
        LOG INFO "Index built: " + COUNT(feature_to_regs) + " features, " 
                                  + COUNT(merged_groups) + " groups"
```

---

## Final Pipeline

```pseudocode
FUNCTION build_regulation_index(parsed_regulations):
    // Main pipeline orchestration
    
    // 1. Initialize all components
    linker = NEW WaterbodyLinker(
        direct_matches = LOAD("direct_matches.json"),
        polygon_matches = LOAD("polygon_matches.json"),
        metadata_gazetteer = LOAD("bc_lakes_database.json"),
        fwa_features = LOAD("fwa_features.gpkg")
    )
    
    scope_filter = NEW ScopeFilter(
        graph = LOAD("fwa_graph.gpickle"),
        landmarks = LOAD("landmarks.json")
    )
    
    tributary_enricher = NEW TributaryEnricher(
        graph = LOAD("fwa_graph.gpickle")
    )
    
    mapper = NEW RegulationMapper(linker, scope_filter, tributary_enricher)
    merger = NEW FeatureMerger()
    builder = NEW RegulationIndexBuilder()
    
    // 2. Run pipeline (Steps 1-6 from diagram)
    feature_to_regs = mapper.process_all_regulations(parsed_regulations)
    merged_groups = merger.merge_features(feature_to_regs)
    builder.build_index(feature_to_regs, merged_groups)
    
    LOG "Pipeline complete - indices ready for UI"
```

---

## Does this flow make sense?

Key design decisions:
1. ✅ **Composite key** `{type}_{id}` in single index
2. ✅ **Manual polygon matches** with explicit feature IDs
3. ✅ **Merging** by `(gnis_id OR watershed_code/waterbody_key, regulation_set)`
4. ✅ **Clean class separation** - each class has single responsibility
5. ✅ **Tributaries handled separately** via graph traversal, not spatial exclusion
6. ✅ **Renamed** `match_waterbody` → `match_regulation` (more accurate)

---

## ScopeFilter Implementation Details

### Overview

ScopeFilter needs to handle different combinations of:
- **Feature Types**: STREAM (linear) vs LAKE (polygon)
- **Scope Types**: DIRECTIONAL, SEGMENT, BUFFER, NAMED_PART, VAGUE, WHOLE_SYSTEM
- **Landmark Types**: km markers, bridges, confluences, geographic features, access points

### Landmark Data Model

```pseudocode
CLASS Landmark:
    id                  // Unique identifier
    name                // Human-readable name (e.g., "Bridge at km 42")
    aliases             // List of alternative names for fuzzy matching
    landmark_type       // KM_MARKER, BRIDGE, CONFLUENCE, LAKE_OUTLET, ACCESS_POINT, etc.
    waterbody_gnis_id   // Which waterbody this landmark is on
    
    // Spatial location data
    geometry            // Point geometry (lat/lon)
    
    // For STREAM features:
    stream_feature_id   // Which specific stream segment contains this landmark
    downstream_measure  // Linear distance from mouth (meters) - for ordering
    
    // For LAKE features:
    lake_feature_id     // Which lake polygon contains this landmark
    
    // Metadata
    source              // "SYNOPSIS_PDF", "GPS", "MANUAL", etc.
    confidence          // 0.0-1.0 confidence in location accuracy
    notes               // Additional context


CLASS LandmarkIndex:
    // Fast lookups by different keys
    by_id               // {landmark_id: Landmark}
    by_waterbody        // {gnis_id: [Landmarks]}
    by_name             // Fuzzy search index (aliases included)
    by_type             // {landmark_type: [Landmarks]}
```

### Scope Type × Feature Type × Landmark Type Matrix

#### 1. DIRECTIONAL Scope (upstream/downstream of landmark)

**For STREAM features:**

```pseudocode
METHOD filter_directional(stream_features, scope):
    // scope.landmark_verbatim: "km 42", "the bridge", "Highway 3 crossing"
    // scope.direction: "UPSTREAM" or "DOWNSTREAM"
    
    // 1. Resolve landmark
    landmark = find_landmark(scope.landmark_verbatim, feature_type="STREAM")
    IF landmark is null:
        RETURN stream_features  // Fallback
    
    // 2. Filter based on downstream_measure
    filtered = []
    FOR EACH feature IN stream_features:
        IF scope.direction == "UPSTREAM":
            // Keep features with higher downstream_measure (further upstream)
            IF feature.downstream_measure > landmark.downstream_measure:
                filtered.ADD(feature)
        ELSE:  // DOWNSTREAM
            // Keep features with lower downstream_measure (further downstream)
            IF feature.downstream_measure < landmark.downstream_measure:
                filtered.ADD(feature)
    
    RETURN filtered


// Landmark types useful for STREAM DIRECTIONAL:
// - KM_MARKER: "km 42" → landmark.downstream_measure = 42000 meters
// - BRIDGE: "Highway 3 bridge" → landmark.downstream_measure = 38500 meters
// - CONFLUENCE: "confluence with Wigwam Creek" → landmark.downstream_measure = 12300 meters
// - LAKE_OUTLET: "outlet of Kootenay Lake" → landmark.downstream_measure = 0 meters (mouth)
```

**For LAKE features:**

```pseudocode
METHOD filter_directional(lake_features, scope):
    // DIRECTIONAL doesn't make sense for lakes (no flow direction)
    // But might mean "upstream lakes" vs "downstream lakes" in a chain
    
    LOG WARNING "DIRECTIONAL scope on LAKE feature - treating as WHOLE_SYSTEM"
    RETURN lake_features  // Fallback
    
// Alternative interpretation: Use graph connectivity
// "Upstream of Kootenay Lake" = all lakes that drain INTO Kootenay Lake
// This would require lake connectivity data
```

---

#### 2. SEGMENT Scope (between two landmarks)

**For STREAM features:**

```pseudocode
METHOD filter_segment(stream_features, scope):
    // scope.landmarks: ["km 10", "km 42"]
    // scope.segment_type: "BETWEEN", "FROM_TO"
    
    // 1. Resolve both landmarks
    landmark_start = find_landmark(scope.landmarks[0], feature_type="STREAM")
    landmark_end = find_landmark(scope.landmarks[1], feature_type="STREAM")
    
    IF landmark_start is null OR landmark_end is null:
        RETURN stream_features  // Fallback
    
    // 2. Ensure start < end (order by downstream_measure)
    IF landmark_start.downstream_measure > landmark_end.downstream_measure:
        SWAP(landmark_start, landmark_end)
    
    // 3. Filter to segment
    filtered = []
    FOR EACH feature IN stream_features:
        measure = feature.downstream_measure
        IF measure >= landmark_start.downstream_measure AND 
           measure <= landmark_end.downstream_measure:
            filtered.ADD(feature)
    
    RETURN filtered


// Example from synopsis:
// "from km 10 to km 42" → landmarks: ["km 10", "km 42"]
// "from the bridge to the confluence" → landmarks: ["Highway 3 bridge", "Wigwam Creek confluence"]
```

**For LAKE features:**

```pseudocode
METHOD filter_segment(lake_features, scope):
    // SEGMENT doesn't apply to lakes (no linear ordering)
    LOG WARNING "SEGMENT scope on LAKE feature - treating as WHOLE_SYSTEM"
    RETURN lake_features  // Fallback
```

---

#### 3. BUFFER Scope (within distance of landmark)

**For STREAM features:**

```pseudocode
METHOD filter_buffer(stream_features, scope):
    // scope.landmark_verbatim: "the campground"
    // scope.distance: 200  (meters)
    // scope.distance_unit: "METERS"
    
    // 1. Resolve landmark
    landmark = find_landmark(scope.landmark_verbatim, feature_type="STREAM")
    IF landmark is null:
        RETURN stream_features  // Fallback
    
    // 2. Filter by linear distance along stream
    filtered = []
    distance_m = convert_to_meters(scope.distance, scope.distance_unit)
    
    FOR EACH feature IN stream_features:
        linear_dist = ABS(feature.downstream_measure - landmark.downstream_measure)
        IF linear_dist <= distance_m:
            filtered.ADD(feature)
    
    RETURN filtered


// Example from synopsis:
// "within 200m of the campground" → distance=200, unit=METERS
// Landmark types useful: ACCESS_POINT, CAMPGROUND, BRIDGE, etc.
```

**For LAKE features:**

```pseudocode
METHOD filter_buffer(lake_features, scope):
    // Use spatial buffer around landmark point geometry
    
    landmark = find_landmark(scope.landmark_verbatim, feature_type="LAKE")
    IF landmark is null:
        RETURN lake_features
    
    // Create buffer polygon around landmark point
    buffer_polygon = landmark.geometry.buffer(scope.distance)
    
    // Filter lakes that intersect buffer
    filtered = []
    FOR EACH feature IN lake_features:
        IF feature.geometry.intersects(buffer_polygon):
            filtered.ADD(feature)
    
    RETURN filtered
```

---

#### 4. NAMED_PART Scope (specific named regions/zones)

**For STREAM features:**

```pseudocode
METHOD filter_named_part(stream_features, scope):
    // scope.part_name: "the canyon section", "Class IV rapids zone"
    // scope.named_region_id: "WIGWAM_RIVER_CANYON"  (from parsing)
    
    // Option A: Pre-defined regions with feature ID lists
    region = named_regions.get(scope.named_region_id)
    IF region is null:
        RETURN stream_features  // Fallback
    
    filtered = []
    FOR EACH feature IN stream_features:
        IF feature.id IN region.feature_ids:
            filtered.ADD(feature)
    
    RETURN filtered


// Option B: Use landmarks to define region
// "the canyon section" = between "canyon entrance" and "canyon exit" landmarks
// This is really a SEGMENT scope with special landmark types
```

**For LAKE features:**

```pseudocode
METHOD filter_named_part(lake_features, scope):
    // scope.part_name: "the north arm", "the shallow bay"
    
    // Use spatial polygon to define named part of lake
    region = named_lake_regions.get(scope.named_region_id)
    IF region is null:
        RETURN lake_features
    
    filtered = []
    FOR EACH feature IN lake_features:
        IF feature.geometry.intersects(region.polygon):
            filtered.ADD(feature)
    
    RETURN filtered
```

---

### Landmark Resolution Algorithm

```pseudocode
FUNCTION find_landmark(landmark_verbatim, feature_type, waterbody_gnis_id):
    // Multi-stage fuzzy matching
    
    // 1. Normalize input
    normalized = normalize_landmark_text(landmark_verbatim)
    // "km. 42" → "km 42"
    // "the bridge at Highway 3" → "highway 3 bridge"
    
    // 2. Check for km marker pattern
    km_match = REGEX_MATCH(normalized, r"km\s+(\d+\.?\d*)")
    IF km_match:
        km_value = FLOAT(km_match.group(1))
        downstream_measure = km_value * 1000  // Convert to meters
        
        // Find or create km marker landmark
        landmark = get_or_create_km_landmark(
            waterbody_gnis_id,
            km_value,
            downstream_measure
        )
        RETURN landmark
    
    // 3. Fuzzy search in landmark database
    candidates = landmark_index.fuzzy_search(
        normalized,
        waterbody_gnis_id=waterbody_gnis_id,
        feature_type=feature_type
    )
    
    IF candidates is empty:
        RETURN null  // Trigger fallback
    
    // 4. Rank by confidence
    best_match = MAX(candidates, key=lambda c: c.confidence * similarity_score(normalized, c.name))
    
    IF best_match.confidence < 0.5:
        LOG WARNING "Low confidence landmark match: " + landmark_verbatim
    
    RETURN best_match
```

---

### Parsing Regulation Scopes - Data Requirements

To enable effective filtering, parsed regulations should provide:

**For DIRECTIONAL scopes:**
```json
{
  "type": "DIRECTIONAL",
  "location_verbatim": "upstream of km 42",
  "direction": "UPSTREAM",
  "landmark_verbatim": "km 42",
  "landmark_type_hint": "KM_MARKER"  // Optional: helps resolution
}
```

**For SEGMENT scopes:**
```json
{
  "type": "SEGMENT",
  "location_verbatim": "from km 10 to km 42",
  "landmarks": [
    {"verbatim": "km 10", "type_hint": "KM_MARKER"},
    {"verbatim": "km 42", "type_hint": "KM_MARKER"}
  ]
}
```

**For BUFFER scopes:**
```json
{
  "type": "BUFFER",
  "location_verbatim": "within 200m of the campground",
  "landmark_verbatim": "the campground",
  "distance": 200,
  "distance_unit": "METERS",
  "landmark_type_hint": "CAMPGROUND"
}
```

**For NAMED_PART scopes:**
```json
{
  "type": "NAMED_PART",
  "location_verbatim": "in the canyon section",
  "part_name": "the canyon section",
  "named_region_id": null  // To be resolved during linking
}
```

---

### Required Landmark Data Sources

**Priority 1 - Easy to extract:**
- **Km markers**: Parse from synopsis text ("km 10", "km 42")
  - Calculate `downstream_measure` from km value
  - Auto-generate during parsing

**Priority 2 - Manual curation:**
- **Major landmarks**: Bridges, confluences, lake outlets mentioned in synopsis
  - GPS coordinates from maps
  - Measure `downstream_measure` from FWA data

**Priority 3 - External databases:**
- **Recreation sites**: Campgrounds, boat launches (BC Rec Sites database)
- **Infrastructure**: Road crossings (BC road network + FWA intersections)
- **Geographic features**: Falls, rapids, canyons (BCGNIS gazetteer)

---

### Implementation Phases

**Phase 1 (MVP):**
- Support KM_MARKER landmarks only (auto-generated from text)
- Implement DIRECTIONAL and SEGMENT for streams
- WHOLE_SYSTEM fallback for everything else

**Phase 2:**
- Add manual landmark database (20-30 major landmarks)
- Implement BUFFER scope for streams
- Better fuzzy matching

**Phase 3:**
- Named regions support
- Lake-specific filtering
- External data integration