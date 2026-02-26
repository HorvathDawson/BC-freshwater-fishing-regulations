# Proposal: Sub-Polygon Lake Subdivisions & Admin Boundary Intersections

> ## Corrections & Updates (Post-Review)
>
> The proposal below was written before GPKG verification. Several IDs, design
> decisions, and code references have since been corrected. Read this section
> first — it supersedes any conflicting details in the body.
>
> ### 1. Corrected IDs (GPKG-verified)
>
> | Lake | WATERBODY_POLY_ID | WATERBODY_KEY | GNIS_ID | Zones |
> |------|-------------------|---------------|---------|-------|
> | **Kootenay Lake** | `705016424` (single polygon) | `328974235` | `14091` | `['4']` |
> | **Williston Lake** | `166104863`, `163048831`, `48079019` (3 polygons) | `328961697` | `28522` | `['7A','7B']` / `['7A']` / `['7A']` |
>
> The body of this proposal uses wrong IDs everywhere (POLY `706389`, WBK
> `331076875` / `329393419`, GNIS `18851` / `21990`). **Replace all
> occurrences** with the verified values above.
>
> **Updated sub-polygon ID scheme:**
> ```
> Kootenay Lake (single polygon):
>   705016424_sub_main_body
>   705016424_sub_upper_west_arm
>   705016424_sub_lower_west_arm
>
> Williston Lake (multi-polygon, use WBK prefix):
>   wbk_328961697_sub_zone_a
>   wbk_328961697_sub_zone_b
>   wbk_328961697_sub_nation_arm
>   wbk_328961697_sub_davis_bay
> ```
>
> ### 2. Parent Polygon Handling — Keep Parents, Fan Out
>
> **The proposal's "parent suppression" approach (Issue 7, Phase 5) is
> replaced.** Do **NOT** remove parent polygons from metadata. Instead:
>
> - Keep parent polygons in `metadata[FeatureType.LAKE]` so that tributary
>   enrichment continues to work (tributaries look up parent via
>   `waterbody_key`).
> - When a DirectMatch uses `sub_polygon_ids`, the linker resolves to
>   sub-polygon features — this is the only regulation assignment path.
> - The "ALL PARTS" DirectMatch fans out to all sub-polygons via
>   `sub_polygon_ids` (multi-element list).
> - The exporter skips parent polygons that have sub-polygons (don't export
>   their geometry) by checking `"_sub_" in fwa_id` on siblings.
> - Parent polygons **remain searchable** in the gazetteer for tributary
>   lookups but **do not receive regulations directly** (no synopsis entry
>   links to the parent poly ID after conversion from `SKIP_ENTRIES`).
>
> This is less invasive than removing parents from metadata and avoids
> breaking the tributary enricher's `_get_stream_seeds_for_waterbody()`
> reverse index (which needs the real `waterbody_key` on the parent).
>
> ### 3. `mgmt_units_override` — Removed
>
> The `SubPolygon.mgmt_units_override` field (Issue 13) is **not needed**.
> MU is informational; inheriting the parent's MUs is acceptable. Remove
> from the dataclass.
>
> ### 4. Geometry Key Prefix — `LAKES_` not `LAKE_`
>
> The proposal's exporter section references `LAKE_{sub_id}` as the
> geometry cache key. The actual prefix is `{ftype_enum.value.upper()}_`
> which for `FeatureType.LAKE` (value `"lakes"`) produces **`LAKES_`**.
> All references to `LAKE_` in the exporter context should be `LAKES_`.
>
> ### 5. CRS Note — `UngazettedWaterbody` Now Also EPSG:3005
>
> Issue 14 ("CRS Difference Between UnmarkedWaterbody and SubPolygon") is
> **now moot**. The `UnmarkedWaterbody` class has been renamed to
> `UngazettedWaterbody` and its coordinates converted from WGS84 to
> EPSG:3005. Both `SubPolygon` and `UngazettedWaterbody` now use EPSG:3005.
>
> ### 6. Renames (Already Implemented)
>
> | Old Name | New Name |
> |----------|----------|
> | `FeatureType.UNMARKED` | `FeatureType.UNGAZETTED` (value `"ungazetted"`) |
> | `UnmarkedWaterbody` | `UngazettedWaterbody` |
> | `unmarked_waterbody_id` (field) | `ungazetted_waterbody_id` |
> | `UNMARKED_WATERBODIES` (dict) | `UNGAZETTED_WATERBODIES` |
> | `UNMARKED_` ID prefix | `UNGAZ_` |
>
> ### 7. `export_regulations_json` — Now Uses `regulation_details`
>
> The exporter's `export_regulations_json()` method no longer rebuilds
> regulation entries from raw parsed data and inline imports. Instead,
> the mapper populates `PipelineResult.regulation_details` (keyed by
> `rule_id`) during processing from all sources. The exporter writes
> this dict directly. Adding a new regulation source only requires
> populating `regulation_details` in the mapper — no exporter changes.

## Two Distinct Problems

There are two separate "polygon splitting" needs that share some infrastructure but are fundamentally different:

### Problem 1: Synopsis Sub-Regions (Kootenay Lake, Williston Lake)

The synopsis defines named sub-regions of large lakes with **different regulation sets** per region. These sub-regions don't exist in FWA — the lake is a single polygon. We need to split it.

**Currently skipped in `SKIP_ENTRIES` (not_found=True):**

| Lake | Sub-regions | GNIS | WBK | MU | Region |
|------|------------|------|-----|-----|--------|
| **Kootenay Lake** | Main Body, Upper West Arm, Lower West Arm | 18851 | 331076875 | 4-4 | 4 |
| **Williston Lake** | Zone A (= portion in Region 7A), Zone B (= portion in Region 7B), Nation Arm, Davis Bay (Finlay Reach) | 21990 | 329393419 | 7-58 | 7B |

Two geometry subdivision methods are used:

1. **Manual crop shapes** (Kootenay Lake) — a human draws simple dividing polygons based on the synopsis map (e.g. page 34), producing child polygons that each get their own regulation set.
2. **Zone boundary crops** (Williston Lake Zone A/B) — the synopsis "Zone A" and "Zone B" correspond to the portions of Williston Lake falling within **Region 7A** and **Region 7B** respectively. The existing WMU zone boundary polygon in the GPKG is used as the cropping shape — no hand-drawing needed. Further sub-regions (Nation Arm, Davis Bay) are then carved out of the zone-cropped pieces using manual crop shapes.

### Problem 2: Admin Boundary Partial Overlaps

An admin boundary (park, WMA, watershed) partially intersects a lake polygon. Currently the admin regulation is assigned to the **entire** lake, which is wrong — only the portion inside the boundary should get the admin regulation.

This is a **computed geometric intersection** — automated, not hand-drawn.

---

## Recommended Approach: Sub-Polygon Features

Both problems are solved the same way: create new `FWAFeature`-compatible sub-polygon entries that the existing pipeline (linker → mapper → exporter) handles natively. No new data model needed — sub-polygons are just features with synthetic IDs that carry their own geometry and inherit parent metadata.

The key insight: sub-polygons are **features**, not regulation-level annotations. Each sub-polygon can independently receive regulations, appear in merge groups, show up in search, and render on the map. This avoids building a parallel "overlay" system and keeps the entire existing pipeline intact.

### Why Not Regulation-Level Spatial Qualifiers?

An earlier version of this proposal (Approach B) suggested keeping the lake intact and attaching a `SpatialQualifier` geometry to each regulation. This doesn't work for synopsis sub-regions:

- Kootenay Lake has **3 sub-regions with entirely different regulation sets**. These aren't "the same lake with an overlay" — they're effectively 3 separate waterbodies for regulation purposes.
- Merge groups are keyed by `(feature_type, grouping_key, reg_set)`. A single feature can only appear in one merge group. Sub-regions with different reg sets **must** be separate features.
- The exporter, search index, and frontend all operate on features. Sub-polygon features slot in with zero changes to these consumers.

---

## Sub-Polygon ID Scheme

Sub-polygons need stable, deterministic IDs that:
1. Are globally unique across the gazetteer
2. Clearly identify the parent waterbody
3. Won't collide with real FWA `WATERBODY_POLY_ID` values (which are numeric strings like `"706389"`)
4. Sort together with siblings
5. Work as `fwa_id` in `FWAFeature` and as keys in `_polygon_geometries`

### Format: `{parent_waterbody_poly_id}_sub_{slug}`

```
Parent: Kootenay Lake, WATERBODY_POLY_ID = "706389", WBK = "331076875"

706389_sub_main_body          → Kootenay Lake - Main Body
706389_sub_upper_west_arm     → Kootenay Lake - Upper West Arm
706389_sub_lower_west_arm     → Kootenay Lake - Lower West Arm
```

For Williston Lake (it has 3 FWA polygons with WBK `329393419` — use `waterbody_key` as parent prefix):

```
Parent: Williston Lake, WBK = "329393419"

wbk_329393419_sub_zone_a         → Williston Lake - Zone A  (lake ∩ Region 7A boundary, minus Nation Arm & Davis Bay)
wbk_329393419_sub_nation_arm     → Nation Arm               (carved from Zone A via manual crop)
wbk_329393419_sub_davis_bay      → Davis Bay (Finlay Reach)  (carved from Zone A via manual crop)
wbk_329393419_sub_zone_b         → Williston Lake - Zone B  (lake ∩ Region 7B boundary)
```

**VERIFIED from parsed synopsis data:** Nation Arm (MU 7-30) and Davis Bay (MU 7-37) appear in the Region 7A synopsis tables, confirming they are geographically within Zone A. Zone A is therefore the **remainder** — its geometry is computed as `(lake ∩ zone_7A) - union(nation_arm, davis_bay)`. Zone B is simple: `lake ∩ zone_7B`. This ensures the sub-polygons fully tile the parent with no overlaps.

**Rule:** Use `{poly_id}_sub_{slug}` when the lake is a single polygon. Use `wbk_{waterbody_key}_sub_{slug}` when the lake has multiple FWA polygons sharing one `waterbody_key`.

The `_sub_` infix makes it trivially detectable as a sub-polygon (no enum needed, just `"_sub_" in fwa_id`).

### Why not use `waterbody_key` as parent for everything?

Because `waterbody_key` can be shared across multiple disconnected polygons. Using the specific `WATERBODY_POLY_ID` when unambiguous keeps the parent→child relationship precise. Only fall back to `wbk_` prefix when the lake genuinely spans multiple FWA polygons that all need subdividing together.

---

## Data Model: `SubPolygon` in `linking_corrections.py`

A new dataclass alongside `UnmarkedWaterbody`:

```python
@dataclass
class SubPolygon:
    """
    Defines a named sub-region of an existing FWA lake polygon.

    Used when the synopsis prescribes different regulations for different
    parts of a single lake (e.g. Kootenay Lake Main Body vs West Arms).

    Geometry is specified via exactly one of:
    - coordinates: Hand-drawn GeoJSON crop polygon (intersected with parent at load time)
    - crop_to_zone: Zone ID string (e.g. "7A") — uses the WMU zone boundary polygon
      from the GPKG as the cropping shape (automated, no hand-drawing needed)

    For remainder sub-polygons (e.g. "Zone A minus Nation Arm and Davis Bay"),
    set exclude_sub_polygon_ids to subtract other sub-polygons from this one's
    computed geometry. Exclusions are applied after the primary crop.

    Sub-polygons are injected into the gazetteer metadata dict as full entries,
    so get_feature_by_id(), get_feature_type_from_id(), and get_polygon_metadata()
    all resolve them natively. They participate normally in linking, merging, and export.

    Attributes:
        sub_polygon_id: Unique ID — "{parent_poly_id}_sub_{slug}" or "wbk_{wbk}_sub_{slug}"
        name: Display name (e.g. "Kootenay Lake - Main Body")
        parent_waterbody_poly_id: WATERBODY_POLY_ID of the parent lake polygon (if single polygon)
        parent_waterbody_key: WATERBODY_KEY of the parent lake (if multi-polygon)
        coordinates: GeoJSON polygon coordinates [[[lon, lat], ...]] in EPSG:3005 (BC Albers)
        crop_to_zone: Zone ID (e.g. "7A") — uses WMU boundary as cropping shape
        exclude_sub_polygon_ids: List of sibling sub_polygon_ids to subtract (remainder computation)
        note: How the geometry was created (e.g. "Drawn from synopsis map page 34")
    """
    sub_polygon_id: str
    name: str
    parent_waterbody_poly_id: Optional[str] = None
    parent_waterbody_key: Optional[str] = None
    coordinates: Any = None  # GeoJSON polygon rings — EPSG:3005
    crop_to_zone: Optional[str] = None  # Zone ID (e.g. "7A") — uses WMU boundary
    exclude_sub_polygon_ids: Optional[List[str]] = None  # Subtract these siblings
    note: str = ""
```

### Companion dict:

```python
# Sub-polygon definitions — manual subdivisions of large lakes
# Format: {"sub_polygon_id": SubPolygon(...)}
SUB_POLYGONS: Dict[str, SubPolygon] = {
    # ── Kootenay Lake (single polygon, manual crop shapes) ────────────
    "706389_sub_main_body": SubPolygon(
        sub_polygon_id="706389_sub_main_body",
        name="Kootenay Lake - Main Body",
        parent_waterbody_poly_id="706389",
        coordinates=[[[...]]],  # Drawn from synopsis map page 34 — EPSG:3005
        note="Main body excluding West Arms. Boundary drawn from regulation map, synopsis page 34.",
    ),
    "706389_sub_upper_west_arm": SubPolygon(
        sub_polygon_id="706389_sub_upper_west_arm",
        name="Kootenay Lake - Upper West Arm",
        parent_waterbody_poly_id="706389",
        coordinates=[[[...]]],
        note="Upper West Arm zone. Boundary drawn from regulation map, synopsis page 34.",
    ),
    "706389_sub_lower_west_arm": SubPolygon(
        sub_polygon_id="706389_sub_lower_west_arm",
        name="Kootenay Lake - Lower West Arm",
        parent_waterbody_poly_id="706389",
        coordinates=[[[...]]],
        note="Lower West Arm zone. Boundary drawn from regulation map, synopsis page 34.",
    ),

    # ── Williston Lake (multi-polygon WBK, zone boundary + manual crops) ─
    # Nation Arm = manual crop shape within Zone A portion (MU 7-30)
    "wbk_329393419_sub_nation_arm": SubPolygon(
        sub_polygon_id="wbk_329393419_sub_nation_arm",
        name="Williston Lake - Nation Arm",
        parent_waterbody_key="329393419",
        coordinates=[[[...]]],  # EPSG:3005 — rough crop around Nation Arm
        note="Nation Arm of Williston Lake (MU 7-30), within Zone A. Nation River (GNIS 16593) flows into this arm.",
    ),
    # Davis Bay = manual crop shape within Zone A portion (MU 7-37)
    "wbk_329393419_sub_davis_bay": SubPolygon(
        sub_polygon_id="wbk_329393419_sub_davis_bay",
        name="Williston Lake - Davis Bay",
        parent_waterbody_key="329393419",
        coordinates=[[[...]]],  # EPSG:3005 — rough crop around Davis Bay in Finlay Reach
        note="Davis Bay in Finlay Reach of Williston Lake (MU 7-37), within Zone A.",
    ),
    # Zone A remainder = (lake ∩ Region 7A) minus Nation Arm and Davis Bay
    "wbk_329393419_sub_zone_a": SubPolygon(
        sub_polygon_id="wbk_329393419_sub_zone_a",
        name="Williston Lake - Zone A",
        parent_waterbody_key="329393419",
        crop_to_zone="7A",
        exclude_sub_polygon_ids=["wbk_329393419_sub_nation_arm", "wbk_329393419_sub_davis_bay"],
        note="Remainder of Williston Lake in Region 7A, excluding Nation Arm and Davis Bay. MUs 7-30, 7-37, 7-38.",
    ),
    # Zone B = lake ∩ Region 7B boundary (simple, no exclusions)
    "wbk_329393419_sub_zone_b": SubPolygon(
        sub_polygon_id="wbk_329393419_sub_zone_b",
        name="Williston Lake - Zone B",
        parent_waterbody_key="329393419",
        crop_to_zone="7B",
        note="Portion of Williston Lake within Region 7B. MUs 7-31, 7-36.",
    ),
}
```

### How geometries get created

There are two geometry sources, both producing the final shape via `intersection()` with the parent lake polygon:

#### Method 1: Manual crop shapes (`coordinates`)

Sub-polygon coordinates are **simple cropping shapes** — not precise lake outlines. The pipeline uses `intersection()` with the parent lake polygon to produce the final geometry, so the coordinates only need to roughly define which *portion* of the lake each sub-polygon covers.

Workflow:

1. Load the parent lake polygon from the GPKG
2. For each sub-region, draw a **simple bounding polygon** (rectangle, trapezoid, or rough outline) that covers the intended area and extends past the lake shoreline where needed
3. Export each cropping polygon as coordinates **in EPSG:3005** (BC Albers — same CRS as the GPKG and all pipeline geometry operations)
4. Paste into the `SubPolygon.coordinates` field

Benefits:

- **No tracing required** — you don't need to follow the lake shoreline at all. A rectangle that crosses the lake at the right dividing line is sufficient.
- **Overshoot is harmless** — any part of the cropping shape outside the parent polygon is discarded automatically.
- **The parent polygon boundary is authoritative** — sub-polygon edges along the shoreline are always pixel-perfect, inherited from the FWA geometry.
- **Only the dividing lines matter** — the human effort is just deciding *where to split*, not drawing precise outlines.

**CRS requirement:** All coordinates must be EPSG:3005. This matches the GPKG source and avoids reprojection at runtime. When drawing in QGIS, set the project CRS to EPSG:3005 before exporting crop shapes.

#### Method 2: Zone boundary crop (`crop_to_zone`)

When a synopsis sub-region aligns with an existing WMU zone boundary (as with Williston Lake Zone A/B mapping to Regions 7A/7B), the zone boundary polygon is used directly as the cropping shape:

1. Load the WMU layer from the GPKG (`data_accessor.get_layer("wmu")`)
2. Dissolve all MU polygons with `REGION_RESPONSIBLE_ID == zone_id` into a single zone boundary polygon
3. Compute `parent_lake.intersection(zone_boundary)` → sub-polygon geometry

This is fully automated — no hand-drawing needed. The WMU layer is already in the GPKG and used by `metadata_builder.py` for zone assignment.

#### Remainder computation (`exclude_sub_polygon_ids`)

Some sub-polygons are defined as "everything in zone X except sub-regions Y and Z". For these:

1. Compute the base geometry (from `crop_to_zone` or `coordinates`)
2. Compute the union of all sub-polygons listed in `exclude_sub_polygon_ids`
3. Final geometry = `base_geometry.difference(exclusion_union)`

This requires **processing order**: non-remainder sub-polygons must be computed first. The pipeline sorts sub-polygons so those with `exclude_sub_polygon_ids=None` are processed before those with exclusions.

#### Full Williston Lake example

> **VERIFIED from parsed synopsis data (parsed_results.json):**
> - `WILLISTON LAKE (in Zone A)` → Region 7A, MUs 7-30, 7-37, 7-38
> - `NATION ARM (Williston Lake)` → Region 7A, MU 7-30
> - `DAVIS BAY (in Finlay Reach of Williston Lake)` → Region 7A, MU 7-37
> - `WILLISTON LAKE (in Zone B)` → Region 7B, MUs 7-31, 7-36
>
> **Nation Arm and Davis Bay are in Zone A (Region 7A), not Zone B.**
> Zone A is the remainder (minus Nation Arm and Davis Bay).
> Zone B is the simple case — just `lake ∩ Region 7B`.

**Sub-polygon definitions:**

```python
SUB_POLYGONS = {
    # Nation Arm = manual crop shape within Zone A (MU 7-30)
    "wbk_329393419_sub_nation_arm": SubPolygon(
        sub_polygon_id="wbk_329393419_sub_nation_arm",
        name="Williston Lake - Nation Arm",
        parent_waterbody_key="329393419",
        coordinates=[[[...]]],  # EPSG:3005 — rough crop around Nation Arm
        note="Nation Arm of Williston Lake (MU 7-30), within Zone A.",
    ),
    # Davis Bay = manual crop shape within Zone A (MU 7-37)
    "wbk_329393419_sub_davis_bay": SubPolygon(
        sub_polygon_id="wbk_329393419_sub_davis_bay",
        name="Williston Lake - Davis Bay",
        parent_waterbody_key="329393419",
        coordinates=[[[...]]],  # EPSG:3005 — rough crop around Davis Bay in Finlay Reach
        note="Davis Bay in Finlay Reach of Williston Lake (MU 7-37), within Zone A.",
    ),
    # Zone A remainder = (lake ∩ Region 7A) minus Nation Arm and Davis Bay
    "wbk_329393419_sub_zone_a": SubPolygon(
        sub_polygon_id="wbk_329393419_sub_zone_a",
        name="Williston Lake - Zone A",
        parent_waterbody_key="329393419",
        crop_to_zone="7A",
        exclude_sub_polygon_ids=["wbk_329393419_sub_nation_arm", "wbk_329393419_sub_davis_bay"],
        note="Remainder of Williston Lake in Region 7A, excluding Nation Arm and Davis Bay. MUs 7-30, 7-37, 7-38.",
    ),
    # Zone B = lake ∩ Region 7B boundary (simple, no exclusions)
    "wbk_329393419_sub_zone_b": SubPolygon(
        sub_polygon_id="wbk_329393419_sub_zone_b",
        name="Williston Lake - Zone B",
        parent_waterbody_key="329393419",
        crop_to_zone="7B",
        note="Portion of Williston Lake within Region 7B. MUs 7-31, 7-36.",
    ),
}
```

**Geometry computation steps:**

```
Step 1: Load 3 FWA polygons for WBK 329393419, union into parent_lake
Step 2: nation_arm_geom = parent_lake.intersection(Polygon(nation_arm_coordinates))
Step 3: davis_bay_geom = parent_lake.intersection(Polygon(davis_bay_coordinates))
Step 4: Load WMU layer, dissolve Region 7A polygons → zone_7a_boundary
Step 5: zone_a_base = parent_lake.intersection(zone_7a_boundary)
Step 6: zone_a_geom = zone_a_base.difference(union(nation_arm_geom, davis_bay_geom))
Step 7: Load WMU layer, dissolve Region 7B polygons → zone_7b_boundary
Step 8: zone_b_geom = parent_lake.intersection(zone_7b_boundary)
```

Result: 4 non-overlapping sub-polygons that fully tile Williston Lake.

**DirectMatch entries (regulation → sub-polygon mapping):**

```python
DIRECT_MATCHES = {
    "Region 7A": {
        # Zone A entry — fans out to ALL 3 sub-regions within Zone A.
        # Zone A regulations must also apply to Nation Arm and Davis Bay,
        # because those areas are geographically inside Zone A.
        "WILLISTON LAKE (in Zone A) (includes waters 500 m east/upstream of the Causeway Road)": DirectMatch(
            sub_polygon_ids=[
                "wbk_329393419_sub_zone_a",
                "wbk_329393419_sub_nation_arm",
                "wbk_329393419_sub_davis_bay",
            ],
            note="Zone A regulations apply to all areas within Zone A, including Nation Arm and Davis Bay.",
        ),
        # Nation Arm — only its own specific regulations (stacked on top of Zone A)
        "NATION ARM (Williston Lake)": DirectMatch(
            sub_polygon_ids=["wbk_329393419_sub_nation_arm"],
            note="Nation Arm-specific regulations (in addition to Zone A regs).",
        ),
        # Davis Bay — only its own specific regulations (stacked on top of Zone A)
        "DAVIS BAY (in Finlay Reach of Williston Lake)": DirectMatch(
            sub_polygon_ids=["wbk_329393419_sub_davis_bay"],
            note="Davis Bay-specific regulations (in addition to Zone A regs).",
        ),
    },
    "Region 7B": {
        # Zone B entry — single sub-polygon, no nested sub-regions
        "WILLISTON LAKE (in Zone B)": DirectMatch(
            sub_polygon_ids=["wbk_329393419_sub_zone_b"],
            note="Williston Lake Zone B — portion within Region 7B.",
        ),
    },
}
```

**Regulation stacking result:**

| Sub-polygon | Receives regulations from | Result |
|---|---|---|
| **Zone A (remainder)** | "WILLISTON LAKE (in Zone A)" | Zone A regs only |
| **Nation Arm** | "WILLISTON LAKE (in Zone A)" + "NATION ARM (Williston Lake)" | Zone A regs + Nation Arm-specific regs |
| **Davis Bay** | "WILLISTON LAKE (in Zone A)" + "DAVIS BAY (in Finlay Reach...)" | Zone A regs + Davis Bay-specific regs |
| **Zone B** | "WILLISTON LAKE (in Zone B)" | Zone B regs only |

**Key insight:** Geometry carving (via `exclude_sub_polygon_ids`) is for **display** — non-overlapping polygons on the map. Regulation fan-out (via `sub_polygon_ids` in DirectMatch) is for **rule assignment** — Zone A's regs flow to all contained areas including Nation Arm and Davis Bay. The mapper accumulates all rules per feature ID naturally, so stacking requires no special handling.

---

## Linking: `DirectMatch` with `sub_polygon_ids`

Add a new list field to `DirectMatch`:

```python
@dataclass
class DirectMatch:
    # ... existing fields (all plural lists) ...
    sub_polygon_ids: Optional[List[str]] = None  # Links to SubPolygon features
```

All DirectMatch ID fields now use **plural-only lists** (a single-element list for one target). This applies to all fields: `gnis_ids`, `fwa_watershed_codes`, `waterbody_poly_ids`, `waterbody_keys`, `blue_line_keys`, `linear_feature_ids`, and `sub_polygon_ids`.

Then convert the Kootenay Lake skip entries to direct matches:

```python
DIRECT_MATCHES = {
    "Region 4": {
        "KOOTENAY LAKE - MAIN BODY (for location see map on page 34)": DirectMatch(
            sub_polygon_ids=["706389_sub_main_body"],
            note="Main body of Kootenay Lake, synopsis page 34.",
        ),
        "KOOTENAY LAKE - UPPER WEST ARM (for location see map on page 34)": DirectMatch(
            sub_polygon_ids=["706389_sub_upper_west_arm"],
            note="Upper West Arm of Kootenay Lake, synopsis page 34.",
        ),
        "KOOTENAY LAKE - LOWER WEST ARM (for location see map on page 34)": DirectMatch(
            sub_polygon_ids=["706389_sub_lower_west_arm"],
            note="Lower West Arm of Kootenay Lake, synopsis page 34.",
        ),
    },
}
```

The linker resolves `sub_polygon_ids` by looking up each pre-computed `FWAFeature` from the gazetteer (injected at init time — see Pipeline Integration below). Unlike the `unmarked_waterbody_id` path which creates ad-hoc objects, sub-polygons are real `FWAFeature` instances already in the gazetteer metadata dict.

For Williston Lake, convert the skip entries to direct matches (see Full Williston Lake example above for the complete region 7A/7B DirectMatch entries).

---

## Pipeline Integration

The sub-polygon system touches 5 pipeline components. The design goal is to inject sub-polygons into the existing data structures so that downstream code (mapper, exporter, search) operates on them without any special-casing.

### 1. SubPolygonProcessor (new module: `regulation_mapping/sub_polygon_processor.py`)

Extract all geometry computation into a standalone `SubPolygonProcessor` class. This keeps the gazetteer focused on metadata — it receives pre-computed geometries instead of owning the Shapely logic. The processor runs once at pipeline init and hands results to the gazetteer for injection.

```python
class SubPolygonProcessor:
    """Computes sub-polygon geometries from SubPolygon definitions.
    
    Responsibilities:
    - Load parent lake geometries from GPKG (EPSG:3005)
    - Load zone boundary polygons from WMU layer (for crop_to_zone)
    - Compute cropped geometries (Phase 1: non-remainder, Phase 2: remainder)
    - Build metadata dicts inheriting parent attributes
    - Return computed results for gazetteer injection
    """
    
    def __init__(self, data_accessor, parent_metadata_lookup):
        self.data_accessor = data_accessor
        self.parent_metadata_lookup = parent_metadata_lookup  # callable: parent_key → metadata dict
    
    def process(self, sub_polygons: Dict[str, "SubPolygon"]) -> "SubPolygonResults":
        """Compute all sub-polygon geometries and metadata.
        
        Returns SubPolygonResults with:
          - computed_geoms: Dict[str, Shapely geometry]  (sub_id → geom)
          - computed_metadata: Dict[str, dict]  (sub_id → metadata dict)
          - parent_ids_to_remove: Set[str]  (parent polygon IDs to suppress)
        """
        parent_geoms = self._load_parent_geometries(sub_polygons)
        zone_boundaries = self._load_zone_boundaries(sub_polygons)
        
        # Phase 1: Compute non-remainder sub-polygons first
        computed_geoms = {}
        for sub_id, sub in sub_polygons.items():
            if sub.exclude_sub_polygon_ids:
                continue  # Phase 2
            parent_key = sub.parent_waterbody_poly_id or sub.parent_waterbody_key
            parent_geom = parent_geoms[parent_key]
            
            if sub.crop_to_zone:
                crop_shape = zone_boundaries[sub.crop_to_zone]
            else:
                crop_shape = Polygon(sub.coordinates[0], sub.coordinates[1:] if len(sub.coordinates) > 1 else None)
            
            computed_geoms[sub_id] = parent_geom.intersection(crop_shape)
        
        # Phase 2: Compute remainder sub-polygons (subtract excluded siblings)
        for sub_id, sub in sub_polygons.items():
            if not sub.exclude_sub_polygon_ids:
                continue
            parent_key = sub.parent_waterbody_poly_id or sub.parent_waterbody_key
            parent_geom = parent_geoms[parent_key]
            
            if sub.crop_to_zone:
                base_geom = parent_geom.intersection(zone_boundaries[sub.crop_to_zone])
            else:
                base_geom = parent_geom.intersection(
                    Polygon(sub.coordinates[0], sub.coordinates[1:] if len(sub.coordinates) > 1 else None)
                )
            exclusion_geoms = [computed_geoms[exc_id] for exc_id in sub.exclude_sub_polygon_ids]
            base_geom = base_geom.difference(unary_union(exclusion_geoms))
            computed_geoms[sub_id] = base_geom
        
        # Build metadata for each sub-polygon (inherit from parent)
        computed_metadata = {}
        for sub_id, sub in sub_polygons.items():
            parent_meta = self.parent_metadata_lookup(sub.parent_waterbody_poly_id or sub.parent_waterbody_key)
            computed_metadata[sub_id] = {
                "gnis_name": sub.name,
                "gnis_id": parent_meta.get("gnis_id"),
                "zones": parent_meta.get("zones", []),
                "region_names": parent_meta.get("region_names", []),
                "mgmt_units": sub.mgmt_units_override or parent_meta.get("mgmt_units", []),
                "waterbody_key": sub_id,  # Use sub_polygon_id — NOT parent's waterbody_key (Issue 12)
                "area_sqm": computed_geoms[sub_id].area,
            }
        
        # Collect parent IDs to remove from metadata
        parent_ids_to_remove = self._collect_parent_ids(sub_polygons)
        
        return SubPolygonResults(computed_geoms, computed_metadata, parent_ids_to_remove)
    
    def _load_parent_geometries(self, sub_polygons):
        """Load parent lake geometries from GPKG (EPSG:3005)."""
        # ... group by parent, load once per parent, union multi-polygon parents ...
    
    def _load_zone_boundaries(self, sub_polygons):
        """Load zone boundaries from WMU layer (for crop_to_zone)."""
        zone_boundaries = {}
        zone_ids = set(s.crop_to_zone for s in sub_polygons.values() if s.crop_to_zone)
        if not zone_ids:
            return zone_boundaries
        wmu_gdf = self.data_accessor.get_layer("wmu")
        if wmu_gdf.crs and wmu_gdf.crs.to_epsg() != 3005:
            wmu_gdf = wmu_gdf.to_crs(epsg=3005)
        for zone_id in zone_ids:
            zone_rows = wmu_gdf[wmu_gdf["REGION_RESPONSIBLE_ID"] == zone_id]
            zone_boundaries[zone_id] = zone_rows.geometry.union_all()
        return zone_boundaries
    
    def _collect_parent_ids(self, sub_polygons):
        """Collect parent polygon IDs that should be removed from metadata."""
        # ... same logic as previous Phase 5 ...
```

### 2. Gazetteer Metadata Injection (metadata_gazetteer.py)

Add a thin `inject_sub_polygons()` method that receives **pre-computed** results from the processor. The gazetteer only handles metadata dict / name index injection — no Shapely, no GPKG loads.

```python
def inject_sub_polygons(self, results: "SubPolygonResults"):
    """Inject pre-computed sub-polygon entries into metadata and name index.
    
    Args:
        results: Output from SubPolygonProcessor.process() containing
                 computed geometries, metadata dicts, and parent IDs to remove.
    """
    # Inject metadata + name index
    for sub_id, sub_meta in results.computed_metadata.items():
        self.metadata[FeatureType.LAKE][sub_id] = sub_meta
        feature = self._build_feature(sub_id, sub_meta, FeatureType.LAKE)
        normalized = self._normalize_for_index(sub_meta["gnis_name"])
        self.name_index.setdefault(normalized, []).append(feature)
    
    # Store geometries for exporter access
    if not hasattr(self, '_sub_polygon_geometries'):
        self._sub_polygon_geometries = {}
    self._sub_polygon_geometries.update(results.computed_geoms)
    
    # Remove parent polygon entries from metadata and name_index
    for parent_id in results.parent_ids_to_remove:
        # Remove from metadata
        parent_meta = self.metadata[FeatureType.LAKE].pop(parent_id, None)
        # Clean up stale name_index entries pointing to removed parent
        if parent_meta:
            parent_name = parent_meta.get("gnis_name", "")
            normalized = self._normalize_for_index(parent_name)
            if normalized in self.name_index:
                self.name_index[normalized] = [
                    f for f in self.name_index[normalized]
                    if getattr(f, "fwa_id", None) != parent_id
                ]
                if not self.name_index[normalized]:
                    del self.name_index[normalized]
```

Key properties after injection:
- `get_feature_by_id("706389_sub_main_body")` → returns the sub-polygon FWAFeature
- `get_feature_type_from_id("706389_sub_main_body")` → returns `FeatureType.LAKE`
- `get_polygon_metadata("706389_sub_main_body", FeatureType.LAKE)` → returns metadata dict with `area_sqm`, `gnis_name`, etc.
- Parent polygon IDs are removed from metadata **and** name_index → no duplicate output, no stale linker references

### 3. Linker (linker.py)

Add a `sub_polygon_ids` path in `_apply_direct_match`, parallel to the existing `unmarked_waterbody_id` path. Since sub-polygons are already injected into the gazetteer metadata, the linker just looks them up:

```python
if direct_match.sub_polygon_ids:
    for sp_id in direct_match.sub_polygon_ids:
        feature = self.gazetteer.get_polygon_by_id(sp_id)
        if feature:
            features.append(feature)
```

This is much simpler than the `unmarked_waterbody_id` path because the sub-polygon is a real `FWAFeature` in the gazetteer — no ad-hoc object creation needed. A single-target entry like `"KOOTENAY LAKE - MAIN BODY"` uses `sub_polygon_ids=["706389_sub_main_body"]` (one-element list). A fan-out entry like `"WILLISTON LAKE (in Zone A)"` uses `sub_polygon_ids=["zone_a", "nation_arm", "davis_bay"]` (multi-element list). Both resolve through the same code path.

### 3b. Regulation Fan-Out via `sub_polygon_ids`

`sub_polygon_ids` is a list field on `DirectMatch`. When a single synopsis entry should assign regulations to **multiple** sub-polygons, all target IDs go in this one list. Three scenarios:

#### Scenario 1: Whole-lake regulations

The existing `DirectMatch` for `"KOOTENAY LAKE, ALL PARTS"` uses `gnis_ids=["14091"]` to match the parent lake. Once the parent is removed from metadata (parent suppression above), this match returns nothing.

**Solution:** Update to use `sub_polygon_ids` listing all sub-polygons:
```python
"KOOTENAY LAKE, ALL PARTS (Main Body, Upper West Arm and Lower West Arm)": DirectMatch(
    sub_polygon_ids=[
        "706389_sub_main_body",
        "706389_sub_upper_west_arm",
        "706389_sub_lower_west_arm",
    ],
    note="Kootenay Lake all parts — fans out to all 3 sub-polygons.",
),
```

#### Scenario 2: Zone-level regulations with nested sub-regions

This is the critical case for Williston Lake. Nation Arm and Davis Bay are carved out of Zone A's **geometry** (non-overlapping polygons for clean map display), but they are still **inside Zone A for regulation purposes**. Zone A's regulations must apply to all three areas: the Zone A remainder, Nation Arm, and Davis Bay.

The fix: the Zone A `DirectMatch` uses `sub_polygon_ids` with all three targets:

```python
"WILLISTON LAKE (in Zone A) (includes waters 500 m east/upstream of the Causeway Road)": DirectMatch(
    sub_polygon_ids=[
        "wbk_329393419_sub_zone_a",       # Zone A remainder
        "wbk_329393419_sub_nation_arm",    # Also gets Zone A regs
        "wbk_329393419_sub_davis_bay",     # Also gets Zone A regs
    ],
    note="Zone A regulations apply to all areas within Zone A.",
),
```

Meanwhile, Nation Arm and Davis Bay's own synopsis entries use a single-element list — their specific regulations only apply to their specific geometry:

```python
"NATION ARM (Williston Lake)": DirectMatch(
    sub_polygon_ids=["wbk_329393419_sub_nation_arm"],  # Single target
),
"DAVIS BAY (in Finlay Reach of Williston Lake)": DirectMatch(
    sub_polygon_ids=["wbk_329393419_sub_davis_bay"],   # Single target
),
```

**Result:** Nation Arm receives Zone A regs + Nation Arm-specific regs. Davis Bay receives Zone A regs + Davis Bay-specific regs. Zone A remainder receives only Zone A regs. All three have non-overlapping geometry. This is the correct behavior — geometry carving is for display, regulation fan-out is for rule assignment.

#### Scenario 3: Tributary entries

Tributary entries keep their existing `gnis_ids` match (tributaries are streams, not affected by polygon suppression).

#### Linker implementation

All sub-polygon linking uses the same `sub_polygon_ids` list field. The linker resolves each ID and returns all as matched features:

```python
if direct_match.sub_polygon_ids:
    for sp_id in direct_match.sub_polygon_ids:
        feature = self.gazetteer.get_polygon_by_id(sp_id)
        if feature:
            features.append(feature)
```

No singular/plural distinction — a single-target entry is just a one-element list.

### 4. Mapper (regulation_mapper.py)

No changes. The mapper receives `FWAFeature` objects from the linker and processes them identically whether they're real FWA features or sub-polygons. Rule assignment, scope filtering, and merge grouping all work on `fwa_id`. Since sub-polygon IDs are in `metadata[FeatureType.LAKE]`, `get_feature_by_id()` resolves them in `merge_features()` without issue.

### 5. Exporter (geo_exporter.py)

Sub-polygon geometries need to be available in `_polygon_geometries`. Inject them **after** the cache load (not inside the `_load()` closure) so they're always present regardless of caching:

```python
def _load_all_polygon_geometries(self):
    if self._polygon_geometries is not None:
        return
    
    # ... existing _with_cache logic ...
    self._polygon_geometries = self._with_cache(...)
    
    # Inject sub-polygon geometries (always, even from cache)
    if hasattr(self.gazetteer, '_sub_polygon_geometries'):
        for sub_id, geom in self.gazetteer._sub_polygon_geometries.items():
            self._polygon_geometries[f"LAKE_{sub_id}"] = geom
```

Sub-polygons then appear in the polygon export layers (GPKG, GeoJSONSeq, PMTiles) alongside regular lake polygons. The search index picks them up via the normal merge group flow because `get_feature_type_from_id()` returns `FeatureType.LAKE` for sub-polygon IDs.

### 6. Pipeline Init (regulation_pipeline.py)

Update `RegulationPipeline._init_components()` to run the sub-polygon processor and inject results:

```python
# After gazetteer is loaded and GPKG path is set:
from .linking_corrections import SUB_POLYGONS
from .sub_polygon_processor import SubPolygonProcessor

if SUB_POLYGONS:
    processor = SubPolygonProcessor(
        data_accessor=self.gazetteer.data_accessor,
        parent_metadata_lookup=lambda key: self.gazetteer.metadata[FeatureType.LAKE].get(key, {}),
    )
    results = processor.process(SUB_POLYGONS)
    self.gazetteer.inject_sub_polygons(results)
```

This must happen **after** `set_gpkg_path()` (so the data accessor is available for geometry loads) and **before** the linker is created (so the linker's gazetteer has sub-polygon entries).

### 7. Parent Lake Handling

When a lake is subdivided, the **sub-polygons fully replace the parent polygon**. The parent lake polygon is removed from regulation assignment entirely — it no longer appears as a feature in the output.

Strategy:
- `inject_sub_polygons()` removes the parent from `metadata[FeatureType.LAKE]` (via `parent_ids_to_remove` from the processor). This means `get_feature_by_id()` no longer returns the parent, `get_feature_type_from_id()` returns UNKNOWN, and the exporter never loads its geometry.
- Sub-polygons **completely tile** the parent lake area (their union equals the parent polygon). There is no leftover "parent" geometry.
- The sub-region-specific synopsis entries (`"KOOTENAY LAKE - MAIN BODY"`, etc.) link to their respective sub-polygon via `sub_polygon_ids=["706389_sub_main_body"]` (single-element list).
- Whole-lake entries (`"KOOTENAY LAKE, ALL PARTS"`) link to all sub-polygons via `sub_polygon_ids` (multi-element list).
- **Zone-level entries** (`"WILLISTON LAKE (in Zone A)"`) link to the zone remainder **plus all nested sub-regions** via `sub_polygon_ids` (multi-element list). This ensures zone regulations propagate to carved-out sub-regions like Nation Arm and Davis Bay.
- Sub-region-specific entries (`"NATION ARM"`, `"DAVIS BAY"`) link only to their own geometry via `sub_polygon_ids=["single_id"]`. Their specific regulations stack on top of the inherited zone regulations.
- Tributary entries (`"KOOTENAY LAKE'S TRIBUTARIES"`) keep their existing `gnis_ids` match — tributaries are stream features, unaffected by parent polygon removal.
- **Regulation stacking example (Nation Arm):** receives Zone A regs (from Zone A fan-out) + Nation Arm-specific regs (from its own DirectMatch). The mapper sees both sets of rules assigned to the same `fwa_id` and merges them into one regulation set. No special handling needed — this is how the mapper already works when multiple synopsis entries map to the same feature.

This ensures the map shows a clean partition of the lake into sub-regions with no overlapping parent polygon underneath. Geometry carving is purely for display — regulation inheritance is handled by fan-out in DirectMatch entries.

---

## Admin Boundary Partial Overlaps (Problem 2)

Admin boundary intersections are a **separate, computed** version of the same sub-polygon concept. When an admin polygon (e.g. a WMA) partially overlaps a lake:

1. Compute `lake_geom.intersection(admin_geom)` and `lake_geom.difference(admin_geom)`
2. Create sub-polygon features dynamically (not hand-drawn)
3. Assign the admin regulation only to the intersection sub-polygon

### ID scheme for computed sub-polygons:

```
{parent_poly_id}_sub_admin_{layer_key}_{admin_feature_id}
```

Example: Lake polygon `706512` partially inside WMA `5364`:
```
706512_sub_admin_wma_5364       → portion inside WMA
706512_sub_admin_wma_5364_rem   → remainder outside WMA
```

### When to compute vs. skip:

Not every partial overlap needs splitting. Use an **area-ratio threshold**:

```python
overlap_ratio = lake_geom.intersection(admin_geom).area / lake_geom.area

if overlap_ratio > 0.95:
    # Nearly fully contained — assign admin reg to whole lake (no split)
elif overlap_ratio < 0.01:
    # Trivial sliver overlap — skip entirely
else:
    # Meaningful partial overlap — split into sub-polygons
```

### Deferral note

Admin partial overlaps are a lower priority than synopsis sub-regions. The synopsis sub-regions (Kootenay, Williston) are explicitly wrong today (skipped entirely). Admin partial overlaps are merely imprecise (regulation assigned to slightly too much area). Implement synopsis sub-polygons first; admin splitting can reuse the same `SubPolygon` infrastructure later.

---

## Implementation Plan

### Phase 1: SubPolygon Infrastructure
1. Add `SubPolygon` dataclass to `linking_corrections.py`
2. Add `SUB_POLYGONS` dict (empty initially)
3. Add `sub_polygon_ids` field to `DirectMatch` (plural-only list, already done)
4. Create `regulation_mapping/sub_polygon_processor.py` with `SubPolygonProcessor` class
5. Add thin `inject_sub_polygons(results)` method to `MetadataGazetteer`
6. Add `sub_polygon_ids` resolution path in `linker._apply_direct_match()` (already done)
7. Add sub-polygon geometry injection in exporter `_load_all_polygon_geometries()` (after cache load)
8. Add `SubPolygonProcessor` + `inject_sub_polygons()` call in `RegulationPipeline._init_components()` (after `set_gpkg_path`, before linker)

### Phase 2: Kootenay Lake (Manual Crop Shapes)
1. In QGIS: load Kootenay Lake polygon from GPKG (set project CRS to EPSG:3005)
2. Draw 3 simple crop shapes from synopsis page 34 — export as EPSG:3005 coordinates
3. Populate `SUB_POLYGONS` entries for Kootenay Lake (3 entries with `coordinates`)
4. Convert 3 Kootenay Lake `SKIP_ENTRIES` → `DIRECT_MATCHES` with `sub_polygon_ids`
5. Update `"KOOTENAY LAKE, ALL PARTS"` DirectMatch to use `sub_polygon_ids` list

### Phase 3: Williston Lake (Zone Boundary + Manual Crops)
1. Populate Zone A entry with `crop_to_zone="7A"` + `exclude_sub_polygon_ids` for Nation Arm & Davis Bay
2. Populate Zone B entry with `crop_to_zone="7B"` (simple crop, no exclusions needed)
3. In QGIS: draw crop shapes for Nation Arm and Davis Bay — export as EPSG:3005 coordinates
4. Populate `SUB_POLYGONS` entries for Williston (4 entries total)
5. Convert 4 Williston `SKIP_ENTRIES` → `DIRECT_MATCHES` with `sub_polygon_ids`

### Phase 4: Validation
1. Run pipeline and verify sub-polygons appear in outputs
2. Check: each sub-polygon gets correct regulation set from synopsis
3. Check: sub-polygons appear in search index with correct names
4. Check: sub-polygon geometries render correctly on map (no gaps, no overlaps)
5. Check: whole-lake regulations fan out to all sub-polygons
6. Check: parent lake polygons do NOT appear in output
7. Check: tributary entries (`KOOTENAY LAKE'S TRIBUTARIES`) still work

### Phase 5 (Future): Admin Computed Splits
1. In `regulation_mapper`, detect partial admin overlaps using area-ratio threshold
2. Auto-generate `SubPolygon` entries for meaningful partial overlaps
3. Use `{poly_id}_sub_admin_{layer}_{id}` ID scheme
4. Same exporter path — injected into `_polygon_geometries`

---

## Summary

| Aspect | Synopsis Sub-Regions | Admin Partial Overlaps |
|--------|---------------------|----------------------|
| **Source** | Manual crops or zone boundary crops | Computed from admin boundary intersection |
| **When** | Pipeline init (static) | Pipeline run (dynamic) |
| **Example** | Kootenay Lake West Arms, Williston Lake Zone A/B | Lake straddling a park boundary |
| **ID format** | `{poly_id}_sub_{slug}` | `{poly_id}_sub_admin_{layer}_{id}` |
| **Priority** | High (currently skipped entirely) | Lower (currently over-assigns, not missing) |
| **Geometry source** | `coordinates` (EPSG:3005) or `crop_to_zone` (WMU boundary) | `lake.intersection(admin_geom)` |
| **Pipeline changes** | SubPolygonProcessor + gazetteer + linker + exporter + pipeline init | Mapper + exporter |
| **Data model** | `SubPolygon` dataclass + `DirectMatch.sub_polygon_ids` | Same `SubPolygon` infra, auto-generated |

### What stays unchanged
- `FWAFeature` dataclass — untouched (sub-polygons are regular FWAFeature instances)
- Merge grouping — works on `fwa_id` + `reg_set` as always
- Scope filtering — operates on feature lists, sub-polygons are just features
- Tributary enrichment — not applicable to polygons, tributary DirectMatches keep working via `gnis_ids`
- Frontend — sub-polygons render as normal polygon features
- Metadata pickle — no rebuild needed (sub-polygons injected at pipeline init, not metadata build time)

---

## Compatibility Analysis (vs. Current Codebase)

Detailed review of each pipeline component and how the proposal interacts with it.

### Issue 1: GNIS ID Discrepancy for Kootenay Lake

**Status: Needs verification before implementation**

The proposal table lists Kootenay Lake as `GNIS 18851`. But the existing working `DirectMatch` for `"KOOTENAY LAKE, ALL PARTS"` in `linking_corrections.py` line 948 uses `gnis_ids=["14091"]`. The skip entries at lines 1695-1704 reference `GNIS 18851` and `waterbody_key 331076875`. Both IDs may be valid (GNIS ID vs GNIS ID 2), but the actual `WATERBODY_POLY_ID` value in the GPKG must be verified before hardcoding it into sub-polygon IDs like `"706389_sub_main_body"`. Run a quick GPKG query to confirm:
```python
data_accessor.get_attributes("lakes", columns=["WATERBODY_POLY_ID", "GNIS_ID_1", "GNIS_ID_2", "WATERBODY_KEY"])
# Filter for waterbody_key 331076875
```

**Impact:** Low — purely a data verification step. Does not affect architecture.

### Issue 2: Gazetteer Metadata Injection (RESOLVED)

The original proposal only showed creating `FWAFeature` objects but never injecting them into `self.metadata[FeatureType.LAKE]`. This would cause:
- `get_feature_by_id()` → `None` (merge_features drops the feature silently)
- `get_feature_type_from_id()` → `FeatureType.UNKNOWN` (exporter skips it)
- `get_polygon_metadata()` → `None` (no area_sqm, gnis_name in output)

**Resolution:** The revised proposal's `inject_sub_polygons()` method writes directly into `self.metadata[FeatureType.LAKE]` and `self.name_index`, making all downstream lookups work natively.

### Issue 3: Exporter Geometry Cache Bypass (RESOLVED)

The exporter's `_load_all_polygon_geometries()` uses `_with_cache()` which can return cached geometry dicts that don't include sub-polygons. Sub-polygon injection inside the `_load()` closure would only run on cache miss.

**Resolution:** The revised proposal injects sub-polygon geometries **after** the cache load, outside `_with_cache()`. This ensures they're always present.

### Issue 4: CRS Consistency (RESOLVED)

The original proposal stored coordinates as GeoJSON `[[[lon, lat]]]` (WGS84) but all pipeline geometry operations use EPSG:3005 (BC Albers). `intersection()` between WGS84 crop shapes and EPSG:3005 parent polygons would produce garbage.

**Resolution:** The revised proposal specifies all `SubPolygon.coordinates` must be in **EPSG:3005**. Zone boundary crops are inherently EPSG:3005 (loaded from GPKG and reprojected in `inject_sub_polygons()`). No runtime CRS conversion needed.

### Issue 5: Whole-Lake Fan-Out (RESOLVED)

The existing `DirectMatch` for `"KOOTENAY LAKE, ALL PARTS"` uses `gnis_ids=["14091"]` which returns the parent polygon. Once the parent is removed from metadata, this match returns nothing.

**Resolution:** The revised proposal uses the `sub_polygon_ids` list field on `DirectMatch`. The `"ALL PARTS"` entry is converted to list all sub-polygon IDs. The linker resolves each and returns them all.

### Issue 6: Pipeline Init (RESOLVED)

The original proposal didn't address the initialization chain for sub-polygon processing.

**Resolution:** The revised proposal separates geometry computation into `SubPolygonProcessor` (new module), which runs at pipeline init after `set_gpkg_path()` and before linker creation. The processor returns pre-computed results, which the gazetteer's thin `inject_sub_polygons(results)` method writes into metadata + name index.

### Issue 7: Parent Lake Suppression (RESOLVED)

Without explicit suppression, the parent polygon would appear as a fourth overlapping polygon in output alongside its sub-polygons.

**Resolution:** `inject_sub_polygons()` removes parent polygon entries from both `self.metadata[FeatureType.LAKE]` **and** `self.name_index`. For multi-polygon parents (Williston), all polygons sharing the waterbody_key are removed. Sub-polygon entries (containing `"_sub_"` in their ID) are preserved. The `name_index` cleanup ensures the linker cannot find stale parent FWAFeature references via name search after parent suppression.

### Issue 8: Merge Group Compatibility

**Status: Compatible, WITH waterbody_key override (see Issue 12)**

The `merge_features()` method in `regulation_mapper.py` calls `get_feature_by_id(feature_id)` and groups by `(feature_type, grouping_key, reg_set)`. For sub-polygons:
- `get_feature_by_id()` works (metadata injected)
- `feature_type` = LAKE (correct)
- No `blue_line_key` → grouping falls to `waterbody_key` or `feature_id`
- Sub-polygons with different `reg_set` → separate groups (correct)
- **Regulation stacking** (e.g. Nation Arm receiving Zone A regs + its own regs): both regulation sets are assigned to the same `fwa_id`. The mapper accumulates all rules per feature, so stacking works naturally. If the combined rule set differs from Zone A's alone, Nation Arm ends up in a different merge group (correct).

**Important caveat:** If sub-polygons inherit the parent's `waterbody_key`, they would share the same grouping key. Two sub-polygons with identical reg_sets would be merged into one group — losing their separate identity. See Issue 12 for the solution.

### Issue 9: Search Index Compatibility

**Status: Compatible, no changes needed**

`export_search_index()` iterates `merged_groups`, calls `get_feature_type_from_id()` and `get_polygon_metadata()`. Both resolve correctly for sub-polygon IDs after metadata injection. Sub-polygons get their own `gnis_name` (e.g. "Kootenay Lake - Main Body") which appears in search results.

### Issue 10: Linker Pattern (Improvement over unmarked_waterbody_id)

**Status: Cleaner than existing pattern**

The existing `unmarked_waterbody_id` path in `_apply_direct_match()` creates a dynamic `type("obj", (object,), {...})()` — not an `FWAFeature` instance. Sub-polygons avoid this anti-pattern entirely: they're real `FWAFeature` entries in the gazetteer. The linker iterates `sub_polygon_ids` and calls `get_polygon_by_id(sp_id)` for each — a simple loop.

### Issue 11: Zone-Level Regulation Inheritance for Nested Sub-Regions (RESOLVED)

**Problem:** Nation Arm and Davis Bay are carved out of Zone A’s **geometry** (non-overlapping polygons for map display). But they are geographically inside Zone A and must receive Zone A’s regulations in addition to their own sub-region-specific regulations. Without explicit handling, carving them out of Zone A’s geometry would cause them to lose Zone A’s regs entirely.

**Resolution:** The Zone A `DirectMatch` entry uses `sub_polygon_ids` to fan out to all three sub-polygons within Zone A: `zone_a`, `nation_arm`, and `davis_bay`. When the linker processes the “WILLISTON LAKE (in Zone A)” synopsis entry, it assigns Zone A’s regulations to all three features. Meanwhile, “NATION ARM” and “DAVIS BAY” entries use single-element `sub_polygon_ids` lists to add their specific regulations on top.

The key distinction: **geometry carving** (via `exclude_sub_polygon_ids`) is for display — non-overlapping polygons on the map. **Regulation fan-out** (via `sub_polygon_ids` in DirectMatch) is for rule assignment — a zone’s regs flow to all contained areas regardless of geometry carving.

This pattern requires no mapper changes. The mapper already accumulates multiple regulation assignments per feature ID. When Nation Arm receives both Zone A rules and Nation Arm-specific rules, they’re all collected under the same `fwa_id` and determine its final regulation set.

### Issue 12: Sub-Polygon `waterbody_key` Must Be Unique (NEW — CRITICAL)

**Status: Requires change to metadata injection**

**Problem:** The original proposal inherits the parent’s `waterbody_key` for sub-polygons. In `merge_features()`, features group by `(feature_type, grouping_key, reg_set)` where `grouping_key` uses `waterbody_key` when it’s in `linked_waterbody_keys_of_polygon`. If all three Kootenay Lake sub-polygons share `waterbody_key=331076875`, any two with identical reg_sets would be merged into one group — losing their separate map identity. Additionally, sharing the parent’s `waterbody_key` would affect `TributaryEnricher` via the `_get_stream_seeds_for_waterbody()` reverse index.

**Resolution:** Each sub-polygon must use **its own `sub_polygon_id` as its `waterbody_key`**. This ensures:
1. Each sub-polygon forms its own merge group (no cross-sub-polygon merging)
2. No interference with tributary enrichment (no real streams share a sub-polygon waterbody_key)
3. Sub-polygons are always independent features in the output

Update the metadata injection code:
```python
computed_metadata[sub_id] = {
    "gnis_name": sub.name,
    "gnis_id": parent_meta.get("gnis_id"),
    "zones": parent_meta.get("zones", []),
    "region_names": parent_meta.get("region_names", []),
    "mgmt_units": parent_meta.get("mgmt_units", []),
    "waterbody_key": sub_id,  # Use sub_polygon_id — NOT parent’s waterbody_key
    "area_sqm": computed_geoms[sub_id].area,
}
```

### Issue 13: Kootenay Lake Sub-Region MU Assignment (NEW — data verification)

**Status: Verified from parsed synopsis data**

The parsed synopsis data shows different MU assignments per Kootenay Lake sub-region:
- `KOOTENAY LAKE - MAIN BODY` → MU 4-19
- `KOOTENAY LAKE - UPPER WEST ARM` → MU 4-7
- `KOOTENAY LAKE - LOWER WEST ARM` → MU 4-7

The `SubPolygonProcessor` inherits MU data from the parent polygon. If the parent polygon spans both MUs 4-7 and 4-19, each sub-polygon would inherit both, which is correct for zone assignment but imprecise for MU display. Consider allowing `SubPolygon` to optionally override `mgmt_units` for more precise attribution:

```python
@dataclass
class SubPolygon:
    # ... existing fields ...
    mgmt_units_override: Optional[List[str]] = None  # Override parent MU assignment
```

**Impact:** Low — MU is informational. Sub-polygons will still function correctly without this override.

### Issue 14: CRS Difference Between UnmarkedWaterbody and SubPolygon (NEW — documentation)

**Status: By design, needs clear documentation**

`UnmarkedWaterbody.coordinates` use **WGS84** (GeoJSON convention: `[longitude, latitude]`). `SubPolygon.coordinates` use **EPSG:3005** (BC Albers). Both dataclasses live in `linking_corrections.py`.

This is intentional — unmarked waterbodies are small features where WGS84 works fine, while sub-polygon crop shapes need to align precisely with EPSG:3005 parent geometries. Add a clear module-level comment documenting this CRS difference.

### Non-Issues (Confirmed Compatible)

| Component | Why it works |
|-----------|-------------|
| **ScopeFilter** | Passthrough (MVP) — returns all features. Sub-polygons are features. |
| **TributaryEnricher** | Not applicable to polygon features. Tributary `DirectMatch` entries continue using `gnis_ids` for stream lookup. |
| **Frontend** | Sub-polygons render as normal polygon features in PMTiles. No frontend changes. |
| **Metadata pickle** | No rebuild needed. Sub-polygon metadata is injected at pipeline init, not at pickle build time. |
| **ID collision** | Sub-polygon IDs contain `_sub_` which never appears in real FWA numeric IDs. |
| **`_create_polygon_layer()`** | Uses `f"{prefix}{fid}"` to look up `_polygon_geometries`. For sub-polygon `706389_sub_main_body`, lookup key is `LAKE_706389_sub_main_body` — matches the injected key. |
