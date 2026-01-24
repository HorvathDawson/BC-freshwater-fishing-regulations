# Regulation Data Architecture - Design Problem

**Date**: January 23, 2026  
**Project**: BC Freshwater Fishing Regulations Map Application

## Current State

### What We Have

#### 1. **LLM Parsed Regulations** (`llm_parsed_results.json`)
- **Source**: BC Freshwater Fishing Regulations Synopsis (PDF)
- **Format**: JSON array with ~2000+ entries
- **Structure**:
```json
{
  "waterbody_name": "COWICHAN RIVER (see map below)",
  "raw_text": "**No Fishing** July 15-Aug 31[Includes Tributaries]...",
  "cleaned_text": "No Fishing July 15-Aug 31 [Includes Tributaries]...",
  "geographic_groups": [
    {
      "location": "from weir at Cowichan Lake outlet to Greendale Trestle",
      "rules": [
        {
          "rule": "No Fishing",
          "type": "closure",
          "dates": ["Nov 15-Apr 15"],
          "species": "null"
        }
      ]
    }
  ],
  "mu": ["1-4"],  // Management Unit (zone)
  "symbols": ["Incl. Tribs", "Stocked"],
  "page": 17,
  "region": "REGION 1 - Vancouver Island"
}
```

**Key Features**:
- **Waterbody-level entries**: One entry per named waterbody
- **Geographic groups**: Rules can apply to specific segments/locations
- **Tributary inclusion**: `"Incl. Tribs"` symbol indicates rules apply to tributaries
- **Rule types**: closure, harvest, gear_restriction, licensing, restriction, access, note
- **Species-specific**: Rules can target specific fish species
- **Date ranges**: Seasonal restrictions

#### 2. **FWA Waterbodies** (PMTiles - `waterbodies_bc.pmtiles`)
- **~3 million features** (streams, lakes, wetlands, manmade)
- **Current properties** in tiles:
```json
{
  "linear_feature_id": "356123456789",  // Unique FWA ID
  "gnis_name": "Cowichan River",
  "waterbody_key": "COWICHAN_RIVER",
  "stream_order": 7,
  "fwa_watershed_code": "920-...",
  "blue_line_key": "...",
  "lake_name": "",
  "layer": "zone_1_streams"
}
```

#### 3. **FWA Stream Graph** (`fwa_bc_primal_full.gpickle`)
- **Directed network graph** of BC stream system
- **Nodes**: Stream junction points (x,y coordinates)
- **Edges**: Stream segments with FWA linear_feature_id
- **Enriched with tributary relationships**:
  - `stream_tributary_of`: Parent stream name
  - `lake_tributary_of`: Lake that stream feeds/drains
- **Supports graph traversal** for finding upstream/downstream segments

#### 4. **Linked Waterbodies** (`matched_waterbodies.json` - 50MB+)
- Links regulation entries to FWA features
- Maps waterbody names → FWA linear_feature_ids
- Contains spatial geometries

#### 5. **Map Application** (React + MapLibre + PMTiles)

**Current UI/UX Implementation**:

**Map Interaction**:
- **Base map**: MapLibre GL with Protomaps basemap tiles
- **Waterbody layers**: PMTiles with 8 management zones
  - Streams (colored by zone, width by stream order)
  - Lakes (filled polygons)
  - Wetlands (diagonal line pattern)
  - Manmade waterbodies (dashed outline)
- **Click behavior**:
  1. User clicks on map
  2. Query features within 15px buffer
  3. If multiple features → show DisambiguationMenu
  4. If single feature → show InfoPanel
  5. Selected feature gets blue highlight (4px line)
  6. Map zooms to fit selected feature bounds
- **Hover behavior**: 
  - Features within 10px get cyan highlight preview
  - Cursor changes to pointer
- **Layer controls**:
  - Toggle visibility: zones, streams, lakes, wetlands, manmade
  - Desktop: Panel open by default
  - Mobile: Panel closed by default

**InfoPanel Display** (Current structure):
```
┌─────────────────────────────┐
│ [TYPE TAG]           [✕]    │  ← Stream/Lake/Wetland/Manmade
│ WATERBODY NAME              │
│ [ID] [STOCKED] [CLASSIFIED] │  ← Tags
├─────────────────────────────┤
│ REGULATIONS                 │
│ Limit: Regional Standard    │  ← species_limit (placeholder)
│ Season: Open All Year       │  ← season_dates (placeholder)
│ Gear: No Restrictions       │  ← gear_restriction (placeholder)
│                             │
│ OFFICIAL TEXT               │  ← regulation_text_snippet
│ "(raw text)"                │     (not yet populated)
├─────────────────────────────┤
│ DETAILS                     │
│ Zone: 1-4                   │  ← zones
│ Mgmt Unit: 1-4              │  ← mgmt_units
│ Watershed Code: 920-...     │  ← fwa_watershed_code
└─────────────────────────────┘
```

**Responsive Design**:
- **Desktop** (>768px):
  - InfoPanel as right sidebar (350px wide, full height)
  - Map takes remaining space
  - Panel slides in from right
- **Mobile** (≤768px):
  - InfoPanel as bottom sheet (70vh height)
  - Swipeable to collapse (shows 160px header only)
  - Panel slides up from bottom
  - Auto-collapses on map pan/zoom

**Current Limitations** (what needs regulation data):
- ❌ `species_limit`, `season_dates`, `gear_restriction` show placeholders
- ❌ `regulation_text_snippet` not populated
- ❌ No indication of segment-specific rules
- ❌ No inheritance visualization (tributary rules)
- ❌ No spatial regulation overlays (parks, swimming areas)
- ❌ No indication of regulation complexity (base vs inherited vs self)

## The Problem

### Regulation Complexity & Hierarchy

Fishing regulations have **multiple levels of specificity** with an **inheritance hierarchy**:

#### **1. Base Regulations** (Lowest Priority)
- **Source**: Management Unit / Zone defaults
- **Example**: "Region 1, MU 1-4: Trout daily limit = 4, Open all year"
- **Applies**: To all waterbodies in zone UNLESS overridden

#### **2. Inherited Regulations** (Medium Priority)
- **Source**: Parent waterbody when `"Incl. Tribs"` symbol present
- **Example**: Cowichan River has "No Fishing July 15-Aug 31 [Includes Tributaries]"
  - → All tributary streams inherit this closure
- **Complexity**: 
  - Inherited from inlet/outlet of lakes
  - Inherited from main stem to tributaries
  - Can apply selectively: "tributaries upstream of Holt Creek only"

#### **3. Self Regulations** (Highest Priority)
- **Source**: Specific rules for this exact waterbody/segment
- **Example**: "Cowichan River upstream of CNR Trestle: Fly fishing only Sept 1-Nov 15"
- **Overrides**: Both base and inherited rules

### Specific Challenges

#### **A. Segment-Level Regulations**
Many streams have **different regulations for different segments**:

```
COWICHAN RIVER:
├─ Segment 1: Weir to Greendale Trestle → No Fishing Nov 15-Apr 15
├─ Segment 2: Upstream of CNR Trestle → Fly fishing only Sept 1-Nov 15
├─ Segment 3: Downstream of CNR Trestle → No Fishing Sept 1-Nov 15
└─ All other segments → Base zone regulations
```

**Problem**: How to map text descriptions like "upstream of CNR Trestle" to specific FWA linear_feature_ids?

#### **B. Tributary Inheritance**
When `"Incl. Tribs"` is present, rules apply to **tributaries**, but:

- **Scope unclear**: Direct tributaries only? All upstream network?
- **Selective application**: "tributaries upstream of and including Holt Creek"
- **Overrides**: Tributary might have its own entry that overrides inherited rules

**Example**:
```
BLACK CREEK: 
  - "No Fishing Dec 1-May 31" 
  - Symbol: "Incl. Tribs"
  
→ Does this apply to:
  - All direct tributaries?
  - All upstream network to headwaters?
  - Named tributaries only?
```

#### **C. Spatial/Park Regulations**
Some regulations are **area-based** rather than waterbody-based:

```
"No vessels in swimming areas, as buoyed and signed"
"No Fishing in Englishman River Falls Provincial Park boundaries"
```

**Problem**: 
- Currently mixed in with waterbody regulations
- Need way to identify and separate spatial rules
- Some overlap with specific waterbodies

#### **D. Inlet/Outlet Regulations**
Lake regulations often have **separate entries for inlet/outlet streams** with complex rules:

**Example**: `WHITESWAN LAKE'S INLET & OUTLET STREAMS`
```json
{
  "waterbody_name": "WHITESWAN LAKE'S INLET & OUTLET STREAMS",
  "geographic_groups": [
    {
      "location": "inlet streams and outlet stream upstream of the falls",
      "rules": [{"rule": "No Fishing", "type": "closure"}]
    },
    {
      "location": "outlet stream downstream of the falls 2.4 km downstream of Whiteswan Lake",
      "rules": [
        {"rule": "No Fishing", "type": "closure", "dates": ["Aug 1-Mar 31"]},
        {"rule": "Rainbow trout daily quota = 5", "type": "harvest"},
        {"rule": "Catch and release all other species", "type": "harvest"}
      ]
    }
  ]
}
```

**Problem**: 
- Regulation entry is named for lake's inlets/outlets
- But applies to **separate stream features** in FWA
- Need to identify which streams are inlet vs outlet from graph
- Complex location logic: "outlet stream downstream of the falls" requires:
  1. Find lake in FWA
  2. Find outlet stream from graph (`lake_tributary_of` relationship)
  3. Find "the falls" landmark on that stream
  4. Split into upstream vs downstream segments
- Some regulations have **opposite logic**: "No Fishing EXCEPT outlet downstream..."

#### **E. Duplicate Regulations**
Many stream segments share identical regulations:

```
"Bait ban, single barbless hook, trout daily quota = 2"
```

**Problem**: 
- Storing full text for each of 3M features = huge file size
- Need deduplication strategy

### Quantified Scope (Analysis Results)

**Segment-Level Complexity** (from `analyze_segments.py`):

| Metric | Count | Percentage |
|--------|-------|------------|
| Total waterbody entries | 1,395 | 100% |
| **Entries with multiple segments** | **231** | **16.6%** |
| Entries with single location | 1,164 | 83.4% |
| Total location records | 1,805 | - |
| Unique location descriptions | 513 | - |

**Key Findings**:
- **~17% of waterbodies** have segment-level regulations (multiple geographic groups)
- **410 additional segment records** exist beyond base entries (1,805 - 1,395)
- **513 unique location descriptions** need to be parsed and linked to FWA IDs
- **Most regulations (83%)** apply to entire waterbody (simpler case)

**Common Location Patterns** (Top 10):
1. `[WHOLE WATERBODY]` - 1,216 records
2. `Includes Tributaries` - 36 records  
3. `parts` - 9 records (vague spatial reference) ⚠️
4. `including tributaries` - 8 records
5. `tributaries` - 6 records
6. `parts of lake` - 4 records ⚠️
7. `downstream of the falls` - 3 records
8. `within 100 m of fishing boundary sign at outlet` - 3 records
9. `swimming areas, as buoyed and signed` - 2 records (spatial)
10. `upstream of 152nd Street (Johnson Road)` - 2 records

**Vague Location Handling** ⚠️:

Some locations are too vague to map to specific segments:
- `"parts"`, `"parts of lake"`, `"on parts"` - No specific boundaries defined
- Example: `WOOD LAKE` has both whole-waterbody rules AND rules for "parts"

**Design Decision**: 
- Vague locations like "parts" will be **treated as whole-waterbody rules**
- Rationale: Without specific boundaries, cannot determine which segments they apply to
- Affects ~15-20 entries (parts, parts of lake, on parts combined)
- Alternative would be to flag these for manual review/mapping

**Implications**:
- Segment linking is **important but not overwhelming** (only 231/1395 waterbodies)
- Need to handle ~500 unique location text patterns
- Many patterns are generic ("tributaries", "parts") vs specific ("upstream of X bridge")
- Export files generated:
  - `segment_analysis.csv` - all 1,805 records
  - `segment_analysis_multi_segment_only.csv` - 641 multi-segment records

## Requirements

### Functional Requirements

1. **User clicks stream/lake** → Show complete regulation set (base + inherited + self)
2. **Clear precedence**: Base < Inherited < Self
3. **Support segment-specific rules** within same waterbody
4. **Handle tributary inheritance** from parent waterbodies
5. **Distinguish spatial regulations** (parks, signed areas)
6. **Enable regulation updates** without rebuilding PMTiles
7. **Fast lookups** (O(1) or O(log n)) for 3M features

### Technical Requirements

1. **PMTiles size constraint**: Keep tiles minimal (~50MB total preferred)
2. **Cloudflare R2 storage**: Separate regulation data from spatial data
3. **Client-side assembly**: Merge regulation layers in browser
4. **Efficient storage**: Deduplicate common regulations
5. **Versioning**: Annual regulation updates (regulations change yearly)

### Data Requirements

1. **Link regulations to FWA IDs**: Map parsed text to linear_feature_ids
2. **Identify segment boundaries**: Convert "upstream of X" to specific feature IDs
3. **Build tributary mappings**: Use FWA graph to determine inheritance
4. **Detect spatial regulations**: Flag park/area-based rules
5. **Handle inlet/outlet links**: Connect lakes to their inlet/outlet streams

## Constraints

- **Max ~3 million features** that could each have regulations
- **Most features share regulations** (likely 95%+ duplicates)
- **Regulations change annually** (need easy update path)
- **Client is web browser** (JavaScript/TypeScript, no heavy computation)
- **Base regulations are hard to parse** whether they're overridden or not
- **Text-based location references** (landmarks, distances, directions) need conversion to FWA IDs

## User Experience Considerations

### What Does a User Need?

When a user wants to fish a specific location, they need:
1. **Click a stream segment on map** → See all regulations for that exact spot
2. **Understand segment boundaries** → Know where regulations change (e.g., "upstream of X bridge")
3. **Clear visual indication** → Different colored segments for different regulation zones
4. **Minimal clutter** → Don't show hundreds of tiny FWA segments if they all have same rules
5. **Tributary awareness** → If clicking a tributary, understand if it inherits rules from parent stream

### Stream Segment Aggregation

**Problem**: FWA streams are split into many small segments (thousands per river). Displaying all individually creates visual clutter.

**Proposed Solution**: 
- **Aggregate consecutive segments** with identical regulations into single visual features
- **Split only where regulations change** (based on location-specific rules)
- Example:
  ```
  BEFORE (FWA segments):
  ├─ Segment 12345: Bait ban, trout limit 2
  ├─ Segment 12346: Bait ban, trout limit 2
  ├─ Segment 12347: Bait ban, trout limit 2
  ├─ Segment 12348: Fly fishing only  ← REGULATION CHANGE
  └─ Segment 12349: Fly fishing only
  
  AFTER (aggregated):
  ├─ Visual Group A (12345-12347): Bait ban, trout limit 2
  └─ Visual Group B (12348-12349): Fly fishing only
  ```

**Open Questions**:
- Should aggregation happen at tile generation time or client-side?
- How to handle user clicking aggregated segment (show all FWA IDs or just representative one)?
- Should we keep original FWA segments in data but render aggregated?

## Open Questions

### 1. Segment Linking Strategy

**Challenge**: Map text descriptions like "upstream of CNR Trestle" to FWA linear_feature_ids

**Options**:
- **A. Automatic**: Use landmark detection + graph traversal
  - Parse text for landmarks (bridges, falls, dams, etc.)
  - Geocode or match to known features
  - Use graph to find all segments upstream/downstream
  - **Risk**: May incorrectly identify landmarks
  
- **B. Manual**: Review interface for human mapping
  - Display map with waterbody highlighted
  - User manually draws/selects segment boundaries
  - Store as FWA ID list or start/end points
  - **Effort**: ~356 multi-segment waterbodies to review
  
- **C. Hybrid** (RECOMMENDED):
  - Automatic for simple patterns ("downstream of X")
  - Flag complex ones for manual review
  - Build library of common landmark patterns
  - **Workflow**:
    1. Auto-link with confidence scores
    2. Human reviews low-confidence matches
    3. Confirmed links become training data for future patterns

**Required Tool**: Manual segment identification interface
- Map viewer with waterbody displayed
- Location description shown alongside
- Drawing tools to mark segment boundaries
- Save as: `{location_key: [fwa_id_1, fwa_id_2, ...]}`
- Export format: JSON or CSV for version control

### 2. LLM Parsed Output Refinement

**Current Issues**:
- Location descriptions vary widely in format
- Some rules are ambiguous (e.g., "parts")
- Geographic groups sometimes combine unrelated rules
- Dates/species parsing inconsistent

**Potential Improvements**:
- **Standardize location format**: Parse into structured fields
  ```json
  {
    "location_type": "segment",  // segment, whole, tributary, spatial
    "direction": "upstream",      // upstream, downstream, between, null
    "landmark_start": "CNR Trestle",
    "landmark_end": null,
    "distance": "2.4 km",
    "applies_to_tributaries": true
  }
  ```
- **Normalize rule types**: Create taxonomy of regulation types
- **Extract landmarks**: Build database of common reference points
- **Confidence scores**: Flag ambiguous entries for review

**Question**: Should we re-process with improved prompts or post-process current output?

### 3. Tributary Inheritance Scope

**Simple Case**: Whole waterbody with "Incl. Tribs"
```
BLACK CREEK: "No Fishing Dec 1-May 31" [Incl. Tribs]
→ All tributaries inherit closure
```

**Complex Case**: Segment-specific with "Incl. Tribs"
```json
{
  "waterbody_name": "CHEHALIS RIVER",
  "symbols": ["Incl. Tribs"],
  "geographic_groups": [
    {
      "location": "from outlet of Chehalis Lake to logging road bridge 2.4 km downstream",
      "rules": [{"rule": "No Fishing", "type": "closure"}]
    },
    {
      "location": "downstream of logging road bridge 2.4 km downstream of lake",
      "rules": [{"rule": "No Fishing", "type": "closure", "dates": ["May 1-31"]}]
    }
  ]
}
```

**Questions**:
- Does "Incl. Tribs" apply to **tributaries of each segment** separately?
- Or only tributaries of the main stem as a whole?
- Example: Tributary joining between bridge and lake outlet:
  - Gets first segment rule only?
  - Gets whole-waterbody inherited rule?
  - Too complex to handle for v1?

**Proposed Approach** (for MVP):
- "Incl. Tribs" with **no location** → All tributaries inherit
- "Incl. Tribs" with **segment locations** → Flag for review, treat as whole-waterbody
- Future: Build segment→tributary graph mapping for precise inheritance

### 4. Spatial Regulation Handling

- **Option A**: Separate spatial layer with polygon overlay
- **Option B**: Pre-link to affected waterbody names only
- **Option C**: Client-side bbox intersection check

### 5. Data Format

- Single large JSON file (~5-15MB)?
- Split by zone/region?
- Binary format for compression?

### 6. Update Workflow

- Version per year (e.g., `regulations_2026.json`)?
- Effective date ranges within single file?
- How to handle mid-season emergency closures?

## Success Criteria

✅ **User clicks any stream/lake** → sees accurate, complete regulations  
✅ **File sizes reasonable** → PMTiles <100MB, Regulations JSON <20MB  
✅ **Fast loading** → Regulations load in <500ms on first click  
✅ **Easy updates** → Can update regulations without regenerating tiles  
✅ **Correct precedence** → Segment rules override inherited rules override base rules  
✅ **Maintainable** → Clear structure for future regulation additions  

## Next Steps

Need to determine optimal data structure that:
1. Stores regulations efficiently (minimal duplication)
2. Links parsed regulation text to FWA features
3. Supports regulation hierarchy (base/inherited/self)
4. Enables fast client-side lookups
5. Allows easy annual updates
