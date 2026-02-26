# Proposal: Zone-Specific Default Regulations

> ## Corrections & Updates (Post-Review)
>
> ### 1. `export_regulations_json` — Now Uses `regulation_details`
>
> The proposal's section on exporting zone regulations to `regulations.json`
> describes adding a new inline import block in the exporter. This is **no
> longer necessary**. The mapper now populates
> `PipelineResult.regulation_details` (keyed by `rule_id`) during processing
> from all sources (synopsis, provincial, zone). The exporter writes this
> dict directly — no source-specific imports needed.
>
> When implementing `_process_zone_regulations()`, populate
> `self.regulation_details[zone_reg.regulation_id]` with the zone entry
> dict (same pattern as provincial regulation details population). The
> exporter will include it automatically.
>
> ### 2. `_get_reg_names` — `zone_` Exclusion Already Implemented
>
> The `_get_reg_names()` method in `geo_exporter.py` has been updated to
> exclude both `prov_` and `zone_` prefixed regulation IDs. No further
> changes needed when implementing zone regulations.
>
> ### 3. Zone Boundary Feature Deduplication
>
> Features near zone boundaries may appear in multiple zones (e.g.,
> `zones=["4", "7A"]`). The `_build_zone_feature_index()` correctly
> indexes them under each zone, so they receive regulations from both
> zones. This is correct behavior. However, ensure `matched_ids` in
> `_process_zone_regulations()` uses a **set** to avoid duplicate
> `feature_to_regs` entries when the same feature appears via multiple
> zone IDs:
>
> ```python
> matched_ids = set()  # Use set, not list
> for zone_id in zone_reg.zone_ids:
>     ...
>     matched_ids.add(fid)  # Not append
> zone_feature_map[zone_reg.regulation_id] = list(matched_ids)
> ```
>
> ### 4. Performance Warning (Issue 9) — Still Applies
>
> Zone regulations apply to **every feature** in covered zones (~3.4M).
> This dramatically increases `feature_to_regs` and `merge_features()`
> workload. Consider the mitigation options described in Issue 9 before
> running the full additive model.
>
> ### 5. `FeatureType.UNGAZETTED` — Add to Zone Index
>
> The `_build_zone_feature_index()` helper should also include
> `FeatureType.UNGAZETTED` in `fwa_types` so that ungazetted waterbodies
> receive zone default regulations.
>
> ### 6. Waterbody-Specific Zone Preamble Regulations (Direct Match Support)
>
> Some zone preamble text targets a **single specific waterbody** rather
> than all waterbodies in the zone. Example:
>
> > *"GARNET LAKE ANGLING CLOSURE — Due to the illegal introduction of
> > largemouth bass, Garnet Lake has been closed to all angling."*
>
> These don't fit the zone-wide model (`feature_types` + zone membership).
> They are waterbody-specific regulations that happen to appear in the
> zone preamble instead of the waterbody tables.
>
> **Solution:** Add optional direct-match ID fields to `ZoneRegulation`
> (same fields as `DirectMatch` in `linking_corrections.py`). When any
> ID field is populated, the regulation targets **only those specific
> features** — `feature_types` and zone-wide scanning are skipped. When
> all ID fields are `None`, the existing zone-wide behavior applies.
>
> **Processing logic in `_process_zone_regulations()`:**
> ```python
> for zone_reg in active_regs:
>     if zone_reg.has_direct_target():
>         # Resolve specific features from ID fields
>         matched_ids = self._resolve_zone_direct_match(zone_reg)
>     else:
>         # Zone-wide: all features of specified types in the zone
>         matched_ids = self._resolve_zone_wide(zone_reg, zone_index)
> ```
>
> **Updated dataclass fields:**
> ```python
> # Optional: target specific waterbodies (any populated → direct match)
> gnis_ids: Optional[List[str]] = None
> waterbody_poly_ids: Optional[List[str]] = None
> fwa_watershed_codes: Optional[List[str]] = None
> waterbody_keys: Optional[List[str]] = None
> linear_feature_ids: Optional[List[str]] = None
> blue_line_keys: Optional[List[str]] = None
> ungazetted_waterbody_id: Optional[str] = None
> ```
>
> **Example:**
> ```python
> ZoneRegulation(
>     regulation_id="zone_r3_garnet_lake_closure",
>     zone_ids=["3"],
>     rule_text="Due to the illegal introduction of largemouth bass, "
>               "Garnet Lake has been closed to all angling.",
>     restriction={"type": "Closure", "species": ["all"],
>                  "details": "Angling closure — invasive largemouth bass."},
>     notes="Region 3 preamble, 2024-2026 synopsis.",
>     gnis_ids=["<garnet_lake_gnis_id>"],
> )
> ```
>
> This keeps all zone preamble content in one module
> (`zone_base_regulations.py`), whether zone-wide or waterbody-specific.

## Overview

The BC Freshwater Fishing Synopsis defines **zone-level default regulations** at the beginning of each region's section. These are blanket rules that apply to **all waterbodies** (or all of a specific type) within a zone, unless overridden by the waterbody-specific tables that follow.

Examples from the synopsis:
- *"All streams in Region 1 are closed Nov 1–June 30 unless noted in the tables"*
- *"Daily quota: 5 trout (all species combined) in Region 4 streams"*
- *"Single barbless hook required in all Region 6 streams"*
- *"Set lines permitted in lakes of Region 6 and Zone A of Region 7"*

These regulations are **distinct from**:
- **Provincial regulations** (`prov_*`) — blanket rules that apply province-wide via admin polygon intersection (e.g., "no fishing in National Parks")
- **Waterbody-specific regulations** (`reg_*`) — parsed from the synopsis tables and linked to individual FWA features via the linker

Zone regulations fill the gap between these two layers: they are region-specific defaults that apply to every waterbody of a given type within a zone.

## Current State of the Codebase

### What Already Exists

1. **Zone membership on every FWA feature.** The metadata builder (`metadata_builder.py`) intersects all features with the WMU (Wildlife Management Unit) layer and assigns:
   - `zones`: List of zone IDs (e.g., `["6"]`, `["7A"]`)
   - `region_names`: List of region names (e.g., `["Skeena"]`)
   - `mgmt_units`: List of MU codes (e.g., `["6-11"]`)

2. **9 zone IDs** across ~3.4M features:

   | Zone ID | Region Name | Streams | Lakes | Wetlands | Manmade |
   |---------|-------------|---------|-------|----------|---------|
   | 1 | Vancouver Island | 152,031 | 18,219 | 8,995 | 125 |
   | 2 | Lower Mainland | 116,917 | 9,362 | 2,295 | 115 |
   | 3 | Thompson-Nicola | 181,157 | 24,003 | 24,242 | 88 |
   | 4 | Kootenay | 197,688 | 12,141 | 6,461 | 154 |
   | 5 | Cariboo | 307,926 | 60,096 | 80,619 | 68 |
   | 6 | Skeena | 826,033 | 141,906 | 97,552 | 181 |
   | 7A | Omineca | 334,788 | 44,789 | 65,008 | 151 |
   | 7B | Peace | 462,457 | 74,309 | 85,269 | 819 |
   | 8 | Okanagan | 63,667 | 5,213 | 8,061 | 114 |

3. **Provincial regulation pattern** (`provincial_base_regulations.py`):
   - `ProvincialRegulation` dataclass with `restriction` dict, `admin_layer` scope, include flags
   - `_process_provincial_regulations()` in `regulation_mapper.py` — spatial intersection with admin layers
   - `prov_` prefix namespace for regulation IDs
   - Placeholder `feature_types` field (not yet implemented)

4. **Regulation output format** (`regulations.json`):
   - Flat dict: `regulation_id` → `{waterbody_name, rule_text, restriction_type, restriction_details, dates, scope_type, source, ...}`
   - Provincial regulations already appear here with `source: "provincial"`

### What Does NOT Exist Yet

- No `ZoneRegulation` dataclass or `zone_base_regulations.py` module
- No zone-scoped processing in `RegulationMapper`
- No `zone_` regulation ID namespace
- No extraction of zone defaults from the synopsis preamble text (parser only handles waterbody-specific tables)

## Design

### Architecture Decision: Static Definition (Not Parsed)

Zone regulations will be **manually defined** as static data, the same pattern used for provincial regulations. Rationale:

1. **Synopsis preamble text is not currently parsed.** The `extract_synopsis.py` → `parse_synopsis.py` pipeline only processes individual waterbody rows from the regulation tables. Zone-level defaults appear as free-form text in the preamble sections before each region's tables.

2. **Zone defaults change infrequently.** The 2025–2027 synopsis is a 2-year publication. These blanket rules are stable — manually encoding them is trivial compared to building a preamble parser.

3. **Consistency with provincial regulations.** Users familiar with `provincial_base_regulations.py` will immediately understand the pattern.

4. **Accuracy.** Manual entry by a domain expert avoids LLM hallucination risk on nuanced regulation text.

> **Future enhancement:** If a preamble parser is added later, it can populate `ZoneRegulation` entries automatically. The downstream pipeline is identical either way.

### New Module: `regulation_mapping/zone_base_regulations.py`

#### `ZoneRegulation` Dataclass

```python
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional


@dataclass
class ZoneRegulation:
    """
    A zone-level default regulation that applies to all waterbodies of
    specified types within one or more zones, OR to specific waterbodies
    identified by direct-match ID fields.

    **Zone-wide mode** (default): When all direct-match ID fields are None,
    the regulation applies to every FWA feature matching `feature_types`
    within the specified `zone_ids`. Uses the pre-computed `zones` field
    on each feature — no spatial join required.

    **Direct-match mode**: When any ID field is populated (gnis_ids,
    waterbody_poly_ids, etc.), the regulation targets ONLY those specific
    features. `feature_types` and zone-wide scanning are skipped. Use this
    for waterbody-specific regulations that appear in the zone preamble
    instead of the waterbody tables (e.g., "Garnet Lake closed to all
    angling").

    Attributes:
        regulation_id: Unique identifier. MUST start with "zone_".
        zone_ids: List of zone IDs this regulation applies to (e.g., ["1"],
                  ["7A", "7B"]). In direct-match mode, this indicates
                  provenance (which preamble section the regulation comes
                  from) rather than feature targeting.
        rule_text: Human-readable regulation text (from synopsis preamble).
        restriction: Regulation details dict — same schema as synopsis-derived
                     restrictions (type, species, details, dates, etc.).
        notes: Source references (synopsis page numbers, edition, etc.).

        feature_types: Which FWA feature types this regulation applies to.
                       Values: "stream", "lake", "wetland", "manmade".
                       If empty/None, applies to ALL feature types.
                       Ignored in direct-match mode.
        mu_ids: Optional list of specific MU codes (e.g., ["6-1", "6-2"]).
                If set, only features whose mgmt_units overlap this list
                are affected. This handles MU-specific zone defaults.
                If None, all features in the zone are affected.
                Ignored in direct-match mode.

        override_priority: Controls how zone defaults interact with
                          waterbody-specific regulations. See "Stacking
                          Semantics" section below. Default: "additive".

        # Direct-match fields (any populated → direct-match mode)
        gnis_ids: GNIS identifiers — matches all features with these GNIS IDs.
        waterbody_poly_ids: Specific polygon IDs (most precise).
        fwa_watershed_codes: Watershed codes — matches all segments from each.
        waterbody_keys: Matches all polygons sharing a WATERBODY_KEY.
        linear_feature_ids: Specific stream segment IDs.
        blue_line_keys: Matches all features from each BLK.
        ungazetted_waterbody_id: Links to an UngazettedWaterbody entry.
    """

    regulation_id: str
    zone_ids: List[str]
    rule_text: str
    restriction: Dict[str, Any]
    notes: str

    # Scope (zone-wide mode)
    feature_types: Optional[List[str]] = None  # None = all types
    mu_ids: Optional[List[str]] = None          # None = all MUs in zone

    # Stacking behavior
    override_priority: str = "additive"  # "additive" | "default_only"

    # Direct-match fields (any populated → targets specific waterbodies)
    gnis_ids: Optional[List[str]] = None
    waterbody_poly_ids: Optional[List[str]] = None
    fwa_watershed_codes: Optional[List[str]] = None
    waterbody_keys: Optional[List[str]] = None
    linear_feature_ids: Optional[List[str]] = None
    blue_line_keys: Optional[List[str]] = None
    ungazetted_waterbody_id: Optional[str] = None

    def has_direct_target(self) -> bool:
        """Return True if any direct-match ID field is populated."""
        return any([
            self.gnis_ids,
            self.waterbody_poly_ids,
            self.fwa_watershed_codes,
            self.waterbody_keys,
            self.linear_feature_ids,
            self.blue_line_keys,
            self.ungazetted_waterbody_id,
        ])
```

#### Regulation ID Namespace

All zone regulation IDs **MUST** start with `zone_`. This prefix is used downstream:

- **`RegulationMapper`** — processes zone regulations via `_process_zone_regulations()` (new method)
- **`GeoExporter._get_reg_names()`** — excludes `zone_` prefixed IDs from the human-readable regulation names in search index and map tiles (same treatment as `prov_`), since these are regional defaults, not waterbody-specific names
- **`regulations.json`** — zone restrictions appear alongside synopsis and provincial rules, with `source: "zone"` to distinguish them

#### Example Entries

```python
ZONE_BASE_REGULATIONS: List[ZoneRegulation] = [
    # ========================================
    # REGION 1 — Vancouver Island
    # ========================================
    ZoneRegulation(
        regulation_id="zone_r1_stream_closure_default",
        zone_ids=["1"],
        rule_text=(
            "Unless otherwise noted in the tables, all streams in Region 1 "
            "are closed to fishing November 1 to June 30."
        ),
        feature_types=["stream"],
        restriction={
            "type": "Closed",
            "species": ["all"],
            "details": "Streams closed Nov 1–Jun 30 unless noted in tables.",
            "dates": {
                "period": "Nov 1 – Jun 30",
                "type": "closure"
            },
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),
    ZoneRegulation(
        regulation_id="zone_r1_trout_daily_quota",
        zone_ids=["1"],
        rule_text=(
            "Daily quota for trout (all species combined): 2 in streams, "
            "4 in lakes, unless noted in the tables."
        ),
        feature_types=["stream", "lake"],
        restriction={
            "type": "Quota",
            "species": ["trout"],
            "details": "Daily quota: 2 (streams), 4 (lakes). All species combined.",
            "stream_quota": 2,
            "lake_quota": 4,
        },
        notes="Source: Region 1 preamble, 2025-2027 Synopsis.",
    ),

    # ========================================
    # REGION 4 — Kootenay
    # ========================================
    ZoneRegulation(
        regulation_id="zone_r4_stream_closure_default",
        zone_ids=["4"],
        rule_text=(
            "Unless otherwise noted, all streams in Region 4 are closed "
            "to fishing from November 1 to June 30."
        ),
        feature_types=["stream"],
        restriction={
            "type": "Closed",
            "species": ["all"],
            "details": "Streams closed Nov 1–Jun 30 unless noted in tables.",
            "dates": {
                "period": "Nov 1 – Jun 30",
                "type": "closure"
            },
        },
        notes="Source: Region 4 preamble, 2025-2027 Synopsis.",
    ),

    # ========================================
    # REGION 6 — Skeena (set lines example)
    # ========================================
    ZoneRegulation(
        regulation_id="zone_r6_set_lines_lakes",
        zone_ids=["6"],
        rule_text=(
            "Set lines are permitted in lakes of Region 6, subject to "
            "gear restrictions noted in the tables."
        ),
        feature_types=["lake"],
        restriction={
            "type": "Gear Permission",
            "details": "Set lines permitted in lakes.",
        },
        notes="Source: Region 6 preamble, 2025-2027 Synopsis.",
    ),

    # ========================================
    # CROSS-ZONE — Set lines in Zone A of Region 7
    # ========================================
    ZoneRegulation(
        regulation_id="zone_r7a_set_lines_lakes",
        zone_ids=["7A"],
        rule_text=(
            "Set lines are permitted in lakes of Zone A (Region 7A), "
            "subject to gear restrictions noted in the tables."
        ),
        feature_types=["lake"],
        restriction={
            "type": "Gear Permission",
            "details": "Set lines permitted in lakes.",
        },
        notes="Source: Region 7A preamble, 2025-2027 Synopsis.",
    ),

    # ========================================
    # WATERBODY-SPECIFIC (direct-match mode)
    # ========================================
    # Zone preamble regulations that target a single waterbody.
    # These use direct-match ID fields instead of zone-wide scanning.

    ZoneRegulation(
        regulation_id="zone_r3_garnet_lake_closure",
        zone_ids=["3"],  # Provenance: Region 3 preamble
        rule_text=(
            "Due to the illegal introduction of largemouth bass, "
            "Garnet Lake has been closed to all angling. Moving forward, "
            "Garnet Valley Reservoir will be used as a research lake, "
            "contributing to ongoing development of fisheries management "
            "programs and improving angling quality province wide."
        ),
        restriction={
            "type": "Closure",
            "species": ["all"],
            "details": "Angling closure — invasive largemouth bass. "
                       "Research lake designation.",
        },
        notes="Region 3 preamble, 2024-2026 synopsis.",
        gnis_ids=["<garnet_lake_gnis_id>"],  # TODO: verify GNIS ID from GPKG
    ),
]
```

### Processing: `_process_zone_regulations()` in RegulationMapper

This is a new method in `RegulationMapper`, called from `run()` between provincial regulations and merge. It handles two modes:

1. **Zone-wide mode** (default): No ID fields → applies to all matching features in the zone via the zone index.
2. **Direct-match mode**: Any ID field populated → resolves specific features from the gazetteer, same as `_apply_direct_match()` in the linker.

```python
def _process_zone_regulations(self) -> Dict[str, List[str]]:
    """
    Process zone-level default regulations.

    Zone regulations apply either to all FWA features of specified types
    within a zone (zone-wide mode), or to specific waterbodies identified
    by direct-match ID fields (direct-match mode).

    Returns:
        Dict mapping regulation_id -> list of matched feature IDs.
    """
    from .zone_base_regulations import ZONE_BASE_REGULATIONS, ZoneRegulation

    zone_feature_map: Dict[str, List[str]] = {}

    active_regs = [
        r for r in ZONE_BASE_REGULATIONS if not getattr(r, "_disabled", False)
    ]
    if not active_regs:
        logger.info("No active zone base regulations to process")
        return zone_feature_map

    logger.info(f"Processing {len(active_regs)} zone base regulation(s)...")

    # Build zone → feature index (once, shared across zone-wide regs)
    zone_index = self._build_zone_feature_index()

    for zone_reg in active_regs:
        if zone_reg.has_direct_target():
            # Direct-match mode: resolve specific features from ID fields
            matched_ids = self._resolve_zone_direct_match(zone_reg)
        else:
            # Zone-wide mode: all features of specified types in the zone
            matched_ids = self._resolve_zone_wide(zone_reg, zone_index)

        zone_feature_map[zone_reg.regulation_id] = list(matched_ids)

        # Add zone regulation to regulation_names
        self.regulation_names[zone_reg.regulation_id] = zone_reg.rule_text

        # Add to feature_to_regs and feature_to_linked_regulation
        for fid in matched_ids:
            self.feature_to_regs.setdefault(fid, []).append(
                zone_reg.regulation_id
            )
            self.feature_to_linked_regulation[fid].add(zone_reg.regulation_id)

        # Populate regulation_details for export
        self.regulation_details[zone_reg.regulation_id] = {
            "waterbody_name": zone_reg.rule_text[:80],
            "rule_text": zone_reg.rule_text,
            "restriction_type": zone_reg.restriction.get("type", ""),
            "restriction_details": zone_reg.restriction.get("details", ""),
            "dates": zone_reg.restriction.get("dates"),
            "source": "zone",
            "zone_ids": zone_reg.zone_ids,
            "feature_types": zone_reg.feature_types,
            "is_direct_match": zone_reg.has_direct_target(),
        }

        mode = "direct-match" if zone_reg.has_direct_target() else "zone-wide"
        logger.info(
            f"  Zone '{zone_reg.regulation_id}' ({mode}): "
            f"{len(matched_ids):,} features across zones {zone_reg.zone_ids}"
        )

    return zone_feature_map


def _resolve_zone_wide(
    self, zone_reg, zone_index
) -> set:
    """
    Resolve features for a zone-wide regulation using the zone index.
    Returns a set of feature IDs.
    """
    matched_ids = set()
    target_ftypes = self._resolve_feature_types(zone_reg.feature_types)

    for zone_id in zone_reg.zone_ids:
        zone_features = zone_index.get(zone_id, {})
        for ftype in target_ftypes:
            for fid, meta in zone_features.get(ftype, {}).items():
                # Optional MU filter
                if zone_reg.mu_ids:
                    feature_mus = meta.get("mgmt_units", [])
                    if not set(zone_reg.mu_ids) & set(feature_mus):
                        continue
                matched_ids.add(fid)

                # Backfill waterbody keys for merge grouping
                if ftype in (FeatureType.LAKE, FeatureType.WETLAND, FeatureType.MANMADE):
                    if wb_key := meta.get("waterbody_key"):
                        self.linked_waterbody_keys_of_polygon.add(str(wb_key))

    return matched_ids


def _resolve_zone_direct_match(self, zone_reg) -> set:
    """
    Resolve features for a waterbody-specific zone regulation using
    direct-match ID fields. Same resolution logic as the linker's
    _apply_direct_match(), but reads from ZoneRegulation fields.

    Uses the same gazetteer methods as linker.py _apply_direct_match().
    All gazetteer lookups return FWAFeature dataclass instances — access
    attributes via dot notation (f.fwa_id), not dict access (f["fid"]).

    Returns a set of feature IDs (strings).
    """
    matched_ids = set()

    # GNIS IDs → all features with matching GNIS
    if zone_reg.gnis_ids:
        for gnis_id in zone_reg.gnis_ids:
            features = self.gazetteer.search_by_gnis_id(str(gnis_id))
            for f in features:
                matched_ids.add(f.fwa_id)

    # Waterbody poly IDs → specific polygons
    if zone_reg.waterbody_poly_ids:
        for poly_id in zone_reg.waterbody_poly_ids:
            feat = self.gazetteer.get_polygon_by_id(str(poly_id))
            if feat:
                matched_ids.add(feat.fwa_id)

    # FWA watershed codes → all stream segments from each
    if zone_reg.fwa_watershed_codes:
        for wsc in zone_reg.fwa_watershed_codes:
            features = self.gazetteer.search_by_watershed_code(wsc)
            for f in features:
                matched_ids.add(f.fwa_id)

    # Waterbody keys → all polygons sharing the key
    if zone_reg.waterbody_keys:
        for wbk in zone_reg.waterbody_keys:
            features = self.gazetteer.get_waterbody_by_key(str(wbk))
            for f in features:
                matched_ids.add(f.fwa_id)

    # Linear feature IDs → specific stream segments
    if zone_reg.linear_feature_ids:
        for lf_id in zone_reg.linear_feature_ids:
            feat = self.gazetteer.get_stream_by_id(str(lf_id))
            if feat:
                matched_ids.add(feat.fwa_id)

    # Blue line keys → all features from each BLK
    if zone_reg.blue_line_keys:
        for blk in zone_reg.blue_line_keys:
            features = self.gazetteer.search_by_blue_line_key(blk)
            for f in features:
                matched_ids.add(f.fwa_id)

    # Ungazetted waterbody
    if zone_reg.ungazetted_waterbody_id:
        feat = self.gazetteer.get_ungazetted_by_id(zone_reg.ungazetted_waterbody_id)
        if feat:
            matched_ids.add(feat.fwa_id)

    # Backfill waterbody keys for any polygon matches
    for fid in matched_ids:
        feat = self.gazetteer.get_feature_by_id(str(fid))
        if feat and feat.feature_type in (
            FeatureType.LAKE, FeatureType.WETLAND, FeatureType.MANMADE
        ):
            if wb_key := feat.waterbody_key:
                self.linked_waterbody_keys_of_polygon.add(str(wb_key))

    return matched_ids
```

#### Zone Feature Index Builder

Iterates metadata once, builds a `{zone_id: {FeatureType: {fid: meta}}}` lookup. This avoids re-scanning ~3.4M features per zone regulation — the index is built once and reused.

```python
def _build_zone_feature_index(self) -> Dict[str, Dict[FeatureType, Dict[str, dict]]]:
    """
    Build an index: zone_id → FeatureType → {feature_id: metadata_dict}.

    Iterates all FWA features once to create a lookup that
    _process_zone_regulations can query efficiently.
    """
    # FeatureType is already imported at module level via:
    #   from fwa_pipeline.metadata_gazetteer import FeatureType

    index: Dict[str, Dict[FeatureType, Dict[str, dict]]] = {}
    fwa_types = [
        FeatureType.STREAM, FeatureType.LAKE, FeatureType.WETLAND,
        FeatureType.MANMADE, FeatureType.UNGAZETTED,
    ]

    for ftype in fwa_types:
        type_metadata = self.gazetteer.metadata.get(ftype, {})
        for fid, meta in type_metadata.items():
            for zone_id in meta.get("zones", []):
                index.setdefault(zone_id, {}).setdefault(ftype, {})[fid] = meta

    total = sum(
        len(features)
        for zones in index.values()
        for features in zones.values()
    )
    logger.info(f"  Zone feature index: {len(index)} zones, {total:,} entries")
    return index
```

#### Feature Type Resolution Helper

```python
def _resolve_feature_types(
    self, feature_type_strings: Optional[List[str]]
) -> List[FeatureType]:
    """Convert feature type strings to FeatureType enums.

    Args:
        feature_type_strings: ["stream", "lake", ...] or None for all types.

    Returns:
        List of FeatureType enum values.
    """
    # FeatureType is already imported at module level via:
    #   from fwa_pipeline.metadata_gazetteer import FeatureType

    type_map = {
        "stream": FeatureType.STREAM,
        "lake": FeatureType.LAKE,
        "wetland": FeatureType.WETLAND,
        "manmade": FeatureType.MANMADE,
        "ungazetted": FeatureType.UNGAZETTED,
    }

    if not feature_type_strings:
        return list(type_map.values())

    return [type_map[t] for t in feature_type_strings if t in type_map]
```

### Pipeline Integration

#### `RegulationMapper.run()` — Add Phase 2.5

```python
def run(self, regulations: List[Dict]) -> PipelineResult:
    """Full pipeline: Link -> Scope -> Enrich -> Map (all sources) -> Merge."""
    # Phase 1: Synopsis + admin area regulations
    self.process_all_regulations(regulations)

    # Phase 2: Provincial base regulations (admin polygon intersection)
    provincial_feature_map = self._process_provincial_regulations()

    # Phase 2.5: Zone base regulations (zone membership lookup — NEW)
    zone_feature_map = self._process_zone_regulations()

    # Phase 3: Merge with ALL regulation sources present
    self.merged_groups = self.merge_features(self.feature_to_regs)

    logger.info("Processing complete")

    return PipelineResult(
        feature_to_regs=self.feature_to_regs,
        merged_groups=self.merged_groups,
        regulation_names=self.regulation_names,
        feature_to_linked_regulation=dict(self.feature_to_linked_regulation),
        gazetteer=self.gazetteer,
        stats=self.stats,
        provincial_feature_map=provincial_feature_map,
        zone_feature_map=zone_feature_map,        # NEW
        admin_feature_map=self.admin_feature_map,
        admin_regulation_ids=self.admin_regulation_ids,
        admin_area_reg_map=dict(self.admin_area_reg_map),
        regulation_details=self.regulation_details,
    )
```

#### `PipelineResult` — Add `zone_feature_map`

```python
@dataclass(frozen=True)
class PipelineResult:
    """Result of the regulation mapping pipeline."""
    feature_to_regs: Dict[str, List[str]] = field(default_factory=dict)
    merged_groups: Dict[str, MergedGroup] = field(default_factory=dict)
    regulation_names: Dict[str, str] = field(default_factory=dict)
    feature_to_linked_regulation: Dict[str, set] = field(default_factory=dict)
    gazetteer: "MetadataGazetteer" = None
    stats: "PipelineStats" = None
    provincial_feature_map: Dict[str, List[str]] = field(default_factory=dict)
    zone_feature_map: Dict[str, List[str]] = field(default_factory=dict)  # NEW
    admin_feature_map: Dict[str, Dict] = field(default_factory=dict)
    admin_regulation_ids: set = field(default_factory=set)
    admin_area_reg_map: Dict[str, Dict] = field(default_factory=dict)
    regulation_details: Dict[str, Dict[str, Any]] = field(default_factory=dict)
```

#### `GeoExporter._get_reg_names()` — Exclude `zone_` Prefix

The existing `_get_reg_names()` method already excludes `prov_` prefixed regulations from the searchable regulation names. Add the same treatment for `zone_`:

```python
def _get_reg_names(self, reg_ids: List[str]) -> List[str]:
    """Get human-readable regulation names, excluding provincial and zone defaults."""
    base_ids = {r.rsplit("_rule", 1)[0] for r in reg_ids}
    base_ids = {b for b in base_ids if not b.startswith("prov_") and not b.startswith("zone_")}
    base_ids -= self.admin_regulation_ids
    return [
        name for bid in sorted(base_ids)
        if (name := self.regulation_names.get(bid))
    ]
```

#### `GeoExporter` — Export Zone Regulations to `regulations.json`

Zone regulations are exported to `regulations.json` using the same format as provincial regulations. The export loop in `_build_regulations_json()` already iterates `self.regulation_names`, which includes zone regulation IDs after `_process_zone_regulations()` adds them.

For zone regulations, the entry format is:

```json
{
  "zone_r1_stream_closure_default": {
    "waterbody_name": "Region 1 — Stream Closure Default",
    "waterbody_key": null,
    "region": "Region 1 - Vancouver Island",
    "management_units": [],
    "rule_text": "Unless otherwise noted in the tables, all streams in Region 1 are closed to fishing November 1 to June 30.",
    "restriction_type": "Closed",
    "restriction_details": "Streams closed Nov 1–Jun 30 unless noted in tables.",
    "dates": {"period": "Nov 1 – Jun 30", "type": "closure"},
    "scope_type": "zone_default",
    "scope_location": null,
    "includes_tributaries": null,
    "source": "zone",
    "zone_ids": ["1"],
    "feature_types": ["stream"]
  }
}
```

**No code changes needed in the exporter.** As noted in Correction §1, `export_regulations_json()` now writes `self.pipeline_result.regulation_details` directly. Zone regulation entries are populated in `_process_zone_regulations()` (see the `self.regulation_details[zone_reg.regulation_id] = {...}` block above), so they appear in the JSON output automatically.

## Stacking Semantics: Zone vs. Waterbody-Specific Regulations

### The Problem

A waterbody that appears in the synopsis tables already has specific regulations parsed from its row. If we also assign zone defaults to that same waterbody, the feature accumulates **both** regulation sets. This is the correct behavior for some regulation types but not others.

**Example — Additive (correct):**
- Zone default: "Streams closed Nov 1–Jun 30"
- Waterbody-specific: "ADAM RIVER: Steelhead catch & release"
- Result: Adam River has **both** rules. The closure period + the species restriction. Correct.

**Example — Override concern:**
- Zone default: "Daily quota: 2 trout in streams"
- Waterbody-specific: "COWICHAN RIVER: Daily quota: 5 trout"
- Result: Cowichan River has **both** quota rules assigned. The webapp must display/resolve the conflict.

### Resolution: Additive Model (Let the Webapp Resolve)

The pipeline uses an **additive model** — all regulation IDs are assigned to each feature, and the webapp/frontend is responsible for displaying them with appropriate context:

1. **Zone defaults** are always assigned to matching features (they represent the baseline).
2. **Waterbody-specific regulations** are also assigned (they represent overrides/additions).
3. The `regulations.json` output includes `source: "zone"` vs `source: "synopsis"` so the webapp can distinguish them.
4. **Display logic** (webapp responsibility):
   - Show zone defaults as "Regional defaults" or "Zone defaults"
   - Show waterbody-specific regulations as the primary display
   - When both exist for the same species/restriction type, the webapp can present the waterbody-specific version as the active rule and the zone default as context

This is the same approach used for provincial regulations: a feature inside a National Park gets both `prov_nat_parks_closed` and any waterbody-specific regulations. The webapp resolves the display priority.

### The `override_priority` Field (Future)

The `override_priority` field on `ZoneRegulation` is a future hook:

- `"additive"` (default): Always assigned. Accumulates with waterbody-specific rules.
- `"default_only"`: Only assigned to features that have **no** waterbody-specific regulations for the same species/restriction type. This requires post-processing logic that checks for conflicts.

**For the initial implementation, all zone regulations use `"additive"`.** The `default_only` mode can be added later if the webapp team requests conflict resolution at the pipeline level.

## Performance Analysis

### No Spatial Intersection Required

The key performance advantage over provincial regulations: zone regulations use pre-computed metadata lookups, not GPKG spatial intersection.

| Operation | Provincial Regulations | Zone Regulations |
|-----------|----------------------|------------------|
| Scope resolution | Spatial intersect with admin GPKG layer | Dictionary lookup on `zones` field |
| Features per regulation | Hundreds–thousands (admin polygon area) | Tens of thousands–hundreds of thousands (full zone) |
| Time per regulation | Seconds–minutes (spatial join) | < 1 second (metadata scan) |
| Index build | N/A (spatial each time) | Once: ~3.4M features → zone index (~5–10s) |

### Memory Impact

The zone feature index stores references to existing metadata dicts (no copies), so memory overhead is minimal — just the dict structure itself (~50 bytes per entry × ~3.4M entries ≈ ~170 MB for the index).

If memory is a concern, the index can be replaced with a generator-based approach that scans metadata per zone regulation. This trades CPU time for memory but is only needed if the pipeline runs in constrained environments.

### Merge Group Impact

Adding zone regulations to `feature_to_regs` means features in the same zone with the same waterbody-specific regulations **may now have different merged groups** if they're in different zones. This is correct behavior — a stream in Zone 1 has different zone defaults than a stream in Zone 6.

However, this increases the number of distinct `reg_set` values, which increases the number of merged groups in the output. The impact is bounded: at most 9 additional regulation IDs per feature (one per zone regulation type × number of applicable regs). In practice, most features will gain 3–8 zone regulation IDs.

> **⚠ Performance Warning:** Currently, `feature_to_regs` only contains features matched by synopsis or provincial regulations (tens of thousands). Zone regulations apply to **every feature** in covered zones, which would add up to ~3.4M features to `feature_to_regs` and `merge_features()`. This could increase merge runtime and output size by orders of magnitude. Consider either: (a) only enriching features that already have at least one other regulation, or (b) profiling the actual merge impact before committing to the fully additive model. See Issue 9 in Compatibility Analysis for details.

## Compatibility with Existing Pipeline Components

### Linker (`linker.py`)

No changes needed. The linker resolves waterbody names to FWA features. Zone regulations bypass the linker entirely — they're assigned by zone membership in `_process_zone_regulations()`.

### ScopeFilter (`scope_filter.py`)

No changes needed. Zone regulations don't use spatial scope filtering.

### TributaryEnricher (`tributary_enricher.py`)

No changes needed. Zone regulations are assigned to features based on their zone membership, including tributary features that already have zones in their metadata.

### MetadataGazetteer (`metadata_gazetteer.py`)

No changes needed. The gazetteer already exposes `metadata[FeatureType]` dicts that `_build_zone_feature_index()` reads.

### GeoExporter (`geo_exporter.py`)

No changes needed:
1. `_get_reg_names()` — `zone_` prefix exclusion already implemented
2. `export_regulations_json()` — writes `regulation_details` directly; zone entries are populated by the mapper

### Search Index (`export_search_index()`)

No changes needed. Zone regulation IDs appear in `feature_to_regs`, which flows into the search index via merged groups. The `zone_` prefix is excluded from `_get_reg_names()`, so zone defaults don't pollute the waterbody-name search results.

### Webapp

The webapp needs to:
1. Recognize `source: "zone"` entries in `regulations.json`
2. Display zone defaults distinctly from waterbody-specific regulations (e.g., a "Regional Defaults" section)
3. Optionally highlight when a waterbody-specific regulation overrides a zone default

---

## Compatibility Analysis (vs. Current Codebase)

Detailed review of each pipeline component and how the proposal interacts with the actual code. Issues are categorised as RESOLVED (design accounts for them), NEW (needs a fix in the implementation), or VERIFIED (confirmed compatible).

### Issue 1: `PipelineResult` Is Frozen — Code Snippet Omits This (RESOLVED — code fixed)

**Status: Corrected in proposal code snippets**

`PipelineResult` in `regulation_mapper.py` line 73 is decorated with `@dataclass(frozen=True)`. All fields use `field(default_factory=...)` for mutable defaults. The proposal's code snippet has been updated to use `@dataclass(frozen=True)` and `field(default_factory=...)` for all mutable fields, including the new `zone_feature_map: Dict[str, List[str]] = field(default_factory=dict)`.

**Impact:** None — resolved.

### Issue 2: `merged_groups` Type Mismatch in Proposal Text (RESOLVED — code fixed)

**Status: Corrected in proposal code snippets**

The proposal's `PipelineResult` snippet has been updated to use `merged_groups: Dict[str, MergedGroup] = field(default_factory=dict)`, matching the actual type from `merge_features()` (see `regulation_mapper.py` line 688).

**Impact:** None — resolved.

### Issue 3: Missing `linked_waterbody_keys_of_polygon` Backfill (RESOLVED — code fixed)

**Status: Corrected in proposal code snippets**

**Problem:** The existing `_process_provincial_regulations()` method explicitly backfills `self.linked_waterbody_keys_of_polygon` for every polygon feature it matches (lines ~540–548 of `regulation_mapper.py`). This backfill is critical because `merge_features()` uses it to decide whether a polygon's `waterbody_key` should become part of its grouping key.

**Resolution:** The `_process_zone_regulations()` code snippet now includes the efficient O(1) metadata-based backfill directly in the inner loop, reading `waterbody_key` from the metadata dict already in hand:

```python
# In the inner loop (now in proposal body):
matched_ids.append(fid)
if ftype in (FeatureType.LAKE, FeatureType.WETLAND, FeatureType.MANMADE):
    if wb_key := meta.get("waterbody_key"):
        self.linked_waterbody_keys_of_polygon.add(str(wb_key))
```

**Impact:** None — resolved. The original bug (missing backfill causing polygon merge fragmentation) is fixed.

### Issue 4: `_get_reg_names` Filter — Proposal Uses Wrong Code Pattern (RESOLVED — code fixed)

**Status: Corrected in proposal code snippets**

The proposal's `_get_reg_names()` code snippet has been updated to use the actual set-comprehension pattern from `geo_exporter.py` (lines 110–125), extending the `startswith("prov_")` filter:

```python
base_ids = {b for b in base_ids if not b.startswith("prov_") and not b.startswith("zone_")}
```

**Impact:** None — resolved.

### Issue 5: `export_regulations_json` — No Exporter Changes Needed (RESOLVED)

**Status: Fully resolved — no exporter code changes required**

The exporter now writes `self.pipeline_result.regulation_details` directly.
Zone regulation entries are populated in `_process_zone_regulations()` via
`self.regulation_details[zone_reg.regulation_id] = {...}`, so they appear
in `regulations.json` automatically. No inline import of
`zone_base_regulations` is needed in the exporter.

**Impact:** None — resolved.

### Issue 6: Zone Index Build — `FeatureType` Import Path (RESOLVED — imports fixed)

**Status: Corrected in proposal code snippets**

The `_build_zone_feature_index()` and `_resolve_feature_types()` code snippets have been updated to note that `FeatureType` is already imported at module level via `from fwa_pipeline.metadata_gazetteer import FeatureType` (line 21 of `regulation_mapper.py`). The redundant inline imports from `fwa_pipeline.metadata_builder` have been removed.

**Impact:** None — resolved.

### Issue 7: Zone Index and Features with Multiple Zones (VERIFIED)

**Status: Compatible, correctly handles multi-zone features**

Some FWA features near zone boundaries have multiple entries in their `zones` list (e.g., `["4", "7A"]`). The proposed `_build_zone_feature_index()` correctly iterates `meta.get("zones", [])` and inserts the feature into **each** zone's index entry. This means a boundary feature would receive zone regulations from both zones — correct behavior, since the feature genuinely lies in both zones.

The downstream effect: the feature receives both Zone 4 and Zone 7A default regulations, giving it a larger `reg_set`. This could cause it to end up in its own merge group (different from neighbours purely in one zone). This is the correct behaviour — boundary features have genuinely different regulation contexts.

**Impact:** None — works as intended.

### Issue 8: `_resolve_feature_types` — String-to-Enum Mapping (VERIFIED)

**Status: Compatible**

The proposed helper maps `"stream" → FeatureType.STREAM`, etc. The `ZoneRegulation.feature_types` field uses lowercase strings matching the keys. The existing `FeatureType` enum values are `"streams"`, `"lakes"`, `"wetlands"`, `"manmade"` (plural). The proposal correctly maps to enum **members** (not values), so `FeatureType.STREAM` (whose `.value` is `"streams"`) is used for lookup — not the string `"stream"`.

The `type_map` dict in the helper uses `"stream"` (singular) as keys, matching the `ZoneRegulation.feature_types` convention. This is a new convention — different from both `FeatureType.value` (plural) and `ProvincialRegulation.feature_types` (also uses lowercase singular strings like `"stream"`, `"lake"`). Both conventions match, so this is consistent.

**Impact:** None — verified compatible with both conventions.

### Issue 9: `merge_features()` — Zone Regulations Inflate Merge Group Count (VERIFIED — expected)

**Status: Expected behaviour, worth quantifying at runtime**

Currently, unnamed streams sharing the same `blue_line_key` and same `reg_set` merge into one group. Adding zone regulation IDs to every feature in a zone means:

1. **Cross-zone boundary features** may gain different zone reg sets, splitting groups that previously merged.
2. **Features with no synopsis regulations** (unreg'd features) will now appear in `feature_to_regs` (since zone regs apply to all features). This dramatically increases the number of features entering `merge_features()`.

Point 2 is the bigger concern. Currently, `feature_to_regs` contains only features that matched at least one synopsis or provincial regulation. Zone regulations will add **every feature in covered zones** to `feature_to_regs`. For streams-only zone regulations across all 9 zones, that's ~2.6M stream features entering merge. For lake-inclusive regulations, add ~390K lake features. The merge step's runtime and output size will increase substantially.

**Mitigation options (future):**
- Only add features to `feature_to_regs` if they already have at least one other regulation (preserves current behavior, zone regs act as enrichment only)
- Or: accept the increase and let the exporter filter at output time

This is an architectural decision that affects pipeline performance and should be deliberated before implementation. The proposal's "additive" model implies all features get zone regs, but the performance cost of merging ~3.4M features (vs. current tens of thousands) may be prohibitive.

**Impact:** MEDIUM — performance and output size. Not a correctness bug.

### Issue 10: `run()` Return — No `zone_feature_map` Field on Current `PipelineResult` (RESOLVED — code fixed)

**Status: Corrected in proposal code snippets**

The `PipelineResult` snippet now includes `zone_feature_map: Dict[str, List[str]] = field(default_factory=dict)` with the correct `@dataclass(frozen=True)` decoration. Existing callers that don't pass `zone_feature_map` will get an empty dict default — no backward compatibility break.

**Impact:** None — resolved (see Issue 1).

### Issue 11: `regulation_names` Population — One Reg = One `rule_text` (VERIFIED)

**Status: Compatible**

The proposal adds `self.regulation_names[zone_reg.regulation_id] = zone_reg.rule_text` in the processing loop. This is the same pattern used for provincial regulations (line 556 of `regulation_mapper.py`). Zone regulation IDs are single strings (e.g., `zone_r1_stream_closure_default`), not compound `_rule` suffixed IDs, so they appear directly in `regulation_names` and can be resolved in `_get_reg_names()` (after the `rsplit("_rule", 1)[0]` operation, `zone_r1_stream_closure_default` is unchanged since it doesn't end with `_rule{N}`).

**Impact:** None — compatible.

### Issue 12: Search Index — Zone Regulation IDs in `regulation_ids` Field (VERIFIED)

**Status: Compatible, zone regs visible in search but excluded from `regulation_names`**

The `export_search_index()` method groups by `(gnis_name, tuple(sorted(group.regulation_ids)), ftype.value)`. Zone regulation IDs will appear in `group.regulation_ids`, which means search groups will be split by zone (a stream in Zone 1 with `zone_r1_*` IDs has a different key than the same stream in Zone 2). This is correct — different zones have different regulations.

Zone regulation IDs are excluded from `_get_reg_names()` (via the `zone_` prefix filter), so they won't appear in the `regulation_names` or `name_variants` fields of search entries. They will appear in the `regulation_ids` field. The frontend can use the `zone_*` prefix to identify and display them appropriately.

**Impact:** None — works as intended.

### Issue 13: Interaction with Sub-Polygon Proposal (VERIFIED — compatible)

**Status: Fully compatible**

If the sub-polygon proposal (`PROPOSAL_ADMIN_POLYGON_INTERSECTIONS.md`) is implemented:

1. Sub-polygons are injected into `self.gazetteer.metadata[FeatureType.LAKE]` at pipeline init.
2. `_build_zone_feature_index()` iterates `metadata[FeatureType.LAKE]` and reads `meta.get("zones", [])`.
3. Sub-polygons inherit their parent's `zones` field → they appear in the zone index → zone regulations apply to them.
4. Sub-polygon `waterbody_key` is set to their own `sub_polygon_id` (per Issue 12 of the sub-polygon proposal), so `linked_waterbody_keys_of_polygon` backfill (Issue 3 above) won't collide with parent lake keys.

No special handling needed. The two proposals compose cleanly.

**Impact:** None.

### Issue 14: `_disabled` Attribute Convention (VERIFIED)

**Status: Compatible — matches provincial pattern**

The proposal uses `getattr(r, "_disabled", False)` to skip disabled regulations, matching the provincial processing pattern at line 472 of `regulation_mapper.py`. Neither `ZoneRegulation` nor `ProvincialRegulation` declares `_disabled` as a field — it's monkey-patched or absent. `getattr` with default `False` handles both cases correctly.

**Impact:** None.

### Non-Issues (Confirmed Compatible)

| Component | Why it works |
|-----------|-------------|
| **Linker (`linker.py`)** | Zone regulations bypass the linker entirely — assigned by zone membership, not waterbody name matching. No changes needed. |
| **ScopeFilter (`scope_filter.py`)** | Zone regulations don't use spatial scope filtering. No interaction. |
| **TributaryEnricher** | Zone regulations apply to features based on their zone assignment, which tributaries already have in their metadata. No enrichment step needed — zone membership is inherent. |
| **MetadataGazetteer** | The gazetteer already exposes `metadata[FeatureType]` dicts that `_build_zone_feature_index()` reads. No method additions needed on the gazetteer. |
| **Frontend/Webapp** | Zone regs appear in `regulations.json` with `source: "zone"`. Frontend needs to handle this new source type for display, but no pipeline-side compatibility risk. |
| **Tests** | Existing tests don't construct `PipelineResult` without defaults. Adding `zone_feature_map` with `field(default_factory=dict)` is backward-compatible. |

---

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `regulation_mapping/zone_base_regulations.py` | **CREATE** | `ZoneRegulation` dataclass + `ZONE_BASE_REGULATIONS` list |
| `regulation_mapping/regulation_mapper.py` | MODIFY | Add `_process_zone_regulations()`, `_build_zone_feature_index()`, `_resolve_feature_types()`, `_resolve_zone_wide()`, `_resolve_zone_direct_match()`, update `run()` |
| `regulation_mapping/regulation_mapper.py` | MODIFY | Add `zone_feature_map` to `PipelineResult` |
| `regulation_mapping/__init__.py` | MODIFY | Export `ZoneRegulation` if needed |

## Implementation Phases

### Phase 1: Core Infrastructure
1. Create `zone_base_regulations.py` with `ZoneRegulation` dataclass
2. Add 2–3 example zone regulations (Region 1 stream closure, Region 1 trout quota)
3. Add `_process_zone_regulations()` and helpers to `RegulationMapper`
4. Update `run()` to call zone processing
5. Update `PipelineResult`

### Phase 2: Full Zone Regulation Data Entry
1. Read each region's preamble text from the synopsis
2. Encode all zone-level defaults as `ZoneRegulation` entries
3. Verify feature counts per zone regulation match expectations

### Phase 3: Webapp Integration
1. No exporter changes needed — `_get_reg_names()` zone exclusion and `regulation_details` export are already implemented
2. Update webapp to display zone defaults with appropriate UI treatment

### Phase 4: Validation
1. Run pipeline — verify zone regulations appear in `regulations.json`
2. Spot-check: pick waterbodies from each zone, confirm they receive correct zone defaults
3. Verify merged group counts — confirm reasonable increase from zone regulation addition
4. Verify search index — zone defaults should NOT appear as waterbody names

## CLI Test (following provincial_base_regulations.py pattern)

The module will include a `__main__` block for standalone testing:

```python
def _run_zone_test():
    """Test zone base regulations against FWA metadata."""
    from fwa_pipeline.metadata_gazetteer import MetadataGazetteer, FeatureType
    from project_config import get_config

    config = get_config()
    gazetteer = MetadataGazetteer(config.fwa_metadata_path)

    # Build zone index
    index = {}
    for ftype in [FeatureType.STREAM, FeatureType.LAKE, FeatureType.WETLAND,
                  FeatureType.MANMADE, FeatureType.UNGAZETTED]:
        for fid, meta in gazetteer.metadata.get(ftype, {}).items():
            for z in meta.get("zones", []):
                index.setdefault(z, {}).setdefault(ftype, {})[fid] = meta

    print(f"\nZone feature index: {len(index)} zones")
    for z in sorted(index.keys()):
        total = sum(len(v) for v in index[z].values())
        breakdown = ", ".join(f"{ft.name}={len(feats)}" for ft, feats in index[z].items())
        print(f"  Zone {z}: {total:,} features ({breakdown})")

    print(f"\n{len(ZONE_BASE_REGULATIONS)} zone regulation(s) defined:")
    for zr in ZONE_BASE_REGULATIONS:
        # Count matching features
        count = 0
        type_map = {"stream": FeatureType.STREAM, "lake": FeatureType.LAKE,
                     "wetland": FeatureType.WETLAND, "manmade": FeatureType.MANMADE}
        target_types = [type_map[t] for t in (zr.feature_types or type_map.keys())]

        for zone_id in zr.zone_ids:
            for ft in target_types:
                count += len(index.get(zone_id, {}).get(ft, {}))

        print(f"  {zr.regulation_id}: zones={zr.zone_ids}, "
              f"types={zr.feature_types or 'ALL'}, features={count:,}")

if __name__ == "__main__":
    _run_zone_test()
```

## Open Questions

### Q1: Should Zone Regulations Apply to Wetlands?

The synopsis preamble focuses on streams and lakes. Wetlands are typically not mentioned in zone defaults. Options:
- **A) Exclude wetlands by default** — set `feature_types=["stream", "lake"]` on most zone regs
- **B) Include wetlands unless explicitly excluded** — some closures logically extend to wetlands
- **Recommendation:** Exclude wetlands by default. Add wetlands only when the synopsis text explicitly mentions "all waters" (which includes wetlands).

### Q2: How Many Zone Regulations Per Region?

Estimated from the synopsis preamble text per region:
- Stream closure default: 1 per region (~9 total)
- Lake open season default: 1 per region (~9 total)
- Species quotas (trout, char, etc.): 2–5 per region (~20–40 total)
- Gear restrictions (barbless hooks, bait, set lines): 1–3 per region (~10–20 total)
- **Total estimate: 50–80 zone regulations**

### Q3: Should the `zone_` Prefix Be More Specific?

Options:
- `zone_r1_...` — includes region number (recommended, matches synopsis structure)
- `zone_7a_...` — uses zone ID directly
- `zone_van_island_...` — uses region name abbreviation
- **Recommendation:** `zone_r{N}_...` pattern (e.g., `zone_r1_stream_closure`, `zone_r7a_set_lines_lakes`)

### Q4: Interaction with Sub-Polygon Proposal

If the sub-polygon proposal (PROPOSAL_ADMIN_POLYGON_INTERSECTIONS.md) is implemented first, sub-polygons will inherit zones from their parent lake. Zone regulations will automatically apply to sub-polygons through the same mechanism — `_process_zone_regulations()` iterates metadata and checks `zones`, which sub-polygons will have.

No special handling needed. The two proposals are fully compatible.
