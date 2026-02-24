# Proposal: Admin Layer Polygon Intersections & Sub-Waterbody Regulations

## Problem

When an admin boundary (park, watershed, WMA) partially overlaps a lake or polygon waterbody, we currently assign the admin regulation to the **entire** waterbody. This is wrong — a lake straddling a park boundary should only have park regulations on the portion inside the park.

This is the same fundamental problem as scope-based sub-regions ("upstream of Highway 20", "south arm only") — different parts of one waterbody having different regulation sets.

## Two Approaches

### A: Geometry Clipping (Split the Polygon)

Create new sub-features by clipping:

```
Original Lake (WBK 12345)
├── Sub-feature A: lake ∩ admin_polygon  → gets admin regs + base regs
└── Sub-feature B: lake - admin_polygon  → gets only base regs
```

**How it works:**
1. At intersection time, compute `lake.intersection(admin_polygon)` and `lake.difference(admin_polygon)`
2. Create new FWAFeature entries with synthetic IDs (e.g., `12345_clip_parkA`, `12345_remainder`)
3. Both sub-features inherit all metadata from parent (zones, gnis, wbk, etc.)
4. Both sub-features inherit all regulations from parent
5. Clipped sub-feature additionally receives the admin regulation
6. Geo exporter emits both geometries separately

**Pros:** Geometrically exact. Each sub-feature is self-contained.  
**Cons:** Multiplies features. Synthetic IDs break the 1:1 mapping to FWA source data. Merge groups become complex — must track parent lineage. Every downstream consumer must handle the split. Sliver polygons from clip artifacts.

### B: Regulation Zones on Intact Features (Recommended)

Keep the waterbody as one feature. Attach a **spatial qualifier** to each regulation that indicates which part of the geometry it applies to.

```
Lake (WBK 12345) — single feature, single geometry
├── Reg: Provincial daily limit    → applies_to: null (whole feature)
├── Reg: Park catch-and-release`    → applies_to: { geometry: <clipped polygon>, source: "Tweedsmuir Park" }
└── Reg: Synopsis special closure  → applies_to: { scope_text: "south arm", geometry: null }
```

**How it works:**
1. `find_features_in_admin_area` stays as-is — returns the whole lake if it intersects
2. When attaching the admin regulation to the feature, compute and store the intersection geometry as a **regulation-level spatial qualifier** (not a feature-level split)
3. The regulation's `applies_to` field holds the clipped geometry (or null for whole-feature regs)
4. Geo exporter renders the base feature normally, plus overlay geometries for spatially-qualified regs
5. Frontend renders the overlay as a highlighted sub-region on the waterbody

**Data model addition:**
```python
# On the regulation/rule attachment, not on FWAFeature:
class SpatialQualifier:
    geometry: Optional[BaseGeometry]  # Clipped intersection, or None = whole feature
    source: Optional[str]             # "Tweedsmuir Park", "upstream of Hwy 20", etc.
    qualifier_type: str               # "admin_clip", "scope_text", "manual"
```

**Pros:**  
- FWAFeature stays 1:1 with FWA source data — no synthetic IDs, no metadata duplication  
- Merge groups unaffected — grouping operates on intact features  
- Naturally extends to scope-based sub-regions (same qualifier, different source)  
- Regulations are the thing that vary spatially, not features — this models reality correctly  
- Frontend can show/hide overlays independently per regulation  

**Cons:**  
- Geo export must handle overlay geometries (new layer or properties)  
- Frontend needs overlay rendering logic  
- PMTiles output must carry the qualifier geometries per-regulation  

## Scope Sub-Regions (Unified with Approach B)

Synopsis regulations with spatial scopes like "south arm" or "upstream of the falls" are the same concept — a regulation that applies to a sub-region of a waterbody. Currently these are attached to the whole feature with the scope as text only.

With `SpatialQualifier`, both admin clips and scope sub-regions use the same mechanism:

| Source | qualifier_type | geometry | source text |
|--------|---------------|----------|-------------|
| Admin boundary clip | `admin_clip` | Computed intersection | "Liard River Watershed" |
| Synopsis scope text | `scope_text` | null (or manual if drawn) | "upstream of Highway 20" |
| Manual correction | `manual` | From linking_corrections | "south arm" |

Scope-text qualifiers start with `geometry: null` (text-only), with the option to add drawn geometries later via manual corrections or a future editing tool.

## Implementation Sketch (Approach B)

### Phase 1: Compute & Store (Pipeline)
1. In `regulation_mapper._process_admin_regulations()`, after `find_features_in_admin_area` returns a lake, compute `lake_geom.intersection(admin_geom)` only if the lake isn't fully contained
2. Attach the intersection geometry to the regulation-feature mapping as a `SpatialQualifier`
3. Store in the pipeline result alongside existing rule data

### Phase 2: Export (Geo Exporter)
1. Base feature geometry exported as-is (no change)
2. Regulations with non-null `SpatialQualifier.geometry` emit an additional overlay geometry in a separate PMTiles layer or as a GeoJSON property
3. `regulations.json` includes the qualifier metadata per rule-feature mapping

### Phase 3: Frontend
1. Base waterbody rendered normally
2. When a regulation has a spatial qualifier with geometry, render a highlighted overlay polygon on the waterbody
3. Regulation panel shows which regs apply to which sub-region

### What Doesn't Change
- FWAFeature dataclass — untouched
- Merge grouping — operates on intact features
- Streams — no sub-region concept needed (segments are already small)
- Metadata/pickle — no rebuild needed
- `find_features_in_admin_area` — returns whole features, unchanged

## Open Questions

1. **Threshold for "partial"**: If 99% of the lake is inside the park, just assign to the whole lake? Configurable area-ratio threshold?
2. **PMTiles overlay encoding**: Separate source layer for overlays, or encode qualifier geometry as a property on the base feature?
3. **Scope text → geometry**: Is there appetite to manually draw scope sub-regions for high-priority waterbodies, or is text-only sufficient for now?
