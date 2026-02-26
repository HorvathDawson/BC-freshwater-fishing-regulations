# Proposal: Zone-Specific Default Regulations

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

## Prerequisites

The following already exist in the codebase:

- **Zone membership on every FWA feature** — `metadata_builder.py` assigns `zones`, `region_names`, `mgmt_units` from WMU intersection (9 zone IDs, ~3.4M features)
- **Provincial regulation pattern** — `ProvincialRegulation` dataclass, `_process_provincial_regulations()`, `prov_` prefix namespace, `regulation_details` export
- **`_get_reg_names()`** already excludes `zone_` prefixed IDs (implemented alongside `prov_` exclusion)
- **`export_regulations_json()`** writes `regulation_details` directly — no source-specific imports needed

## Design

### Architecture Decision: Static Definition (Not Parsed)

Zone regulations are **manually defined** as static data (same pattern as `provincial_base_regulations.py`). The synopsis preamble is free-form text not currently parsed, zone defaults change infrequently (2-year publication cycle), and manual entry avoids LLM hallucination risk on nuanced regulation text. If a preamble parser is added later, it can populate `ZoneRegulation` entries automatically — the downstream pipeline is identical.

### New Module: `regulation_mapping/zone_base_regulations.py`

#### `ZoneRegulation` Dataclass

```python
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional

from fwa_pipeline.metadata_builder import FeatureType


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
                       Uses FeatureType enum values (e.g., FeatureType.STREAM,
                       FeatureType.LAKE). If empty/None, applies to ALL feature
                       types. Ignored in direct-match mode.
        mu_ids: Optional list of specific MU codes (e.g., ["6-1", "6-2"]).
                If set, only features whose mgmt_units overlap this list
                are affected. This handles MU-specific zone defaults.
                If None, all features in the zone are affected.
                Ignored in direct-match mode.

        # Direct-match fields (any populated → direct-match mode)
        gnis_ids: GNIS identifiers — matches all features with these GNIS IDs.
        waterbody_poly_ids: Specific polygon IDs (most precise).
        fwa_watershed_codes: Watershed codes — matches all segments from each.
        waterbody_keys: Matches all polygons sharing a WATERBODY_KEY.
        linear_feature_ids: Specific stream segment IDs.
        blue_line_keys: Matches all features from each BLK.
        sub_polygon_ids: Links to synthetic sub-polygon features.
        ungazetted_waterbody_id: Links to an UngazettedWaterbody entry.
    """

    regulation_id: str
    zone_ids: List[str]
    rule_text: str
    restriction: Dict[str, Any]
    notes: str

    # Scope (zone-wide mode)
    feature_types: Optional[List[FeatureType]] = None  # None = all types
    mu_ids: Optional[List[str]] = None          # None = all MUs in zone

    # Direct-match fields (any populated → targets specific waterbodies)
    gnis_ids: Optional[List[str]] = None
    waterbody_poly_ids: Optional[List[str]] = None
    fwa_watershed_codes: Optional[List[str]] = None
    waterbody_keys: Optional[List[str]] = None
    linear_feature_ids: Optional[List[str]] = None
    blue_line_keys: Optional[List[str]] = None
    sub_polygon_ids: Optional[List[str]] = None
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
            self.sub_polygon_ids,
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
        feature_types=[FeatureType.STREAM],
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
        feature_types=[FeatureType.STREAM, FeatureType.LAKE],
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
            "feature_types": [ft.value for ft in zone_reg.feature_types] if zone_reg.feature_types else None,
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

    # Sub-polygon IDs → synthetic sub-polygon features
    if zone_reg.sub_polygon_ids:
        for sp_id in zone_reg.sub_polygon_ids:
            feat = self.gazetteer.get_polygon_by_id(sp_id)
            if feat:
                matched_ids.add(feat.fwa_id)

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
    #   from fwa_pipeline.metadata_builder import FeatureType

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
_ALL_FWA_TYPES = [
    FeatureType.STREAM, FeatureType.LAKE, FeatureType.WETLAND,
    FeatureType.MANMADE, FeatureType.UNGAZETTED,
]

def _resolve_feature_types(
    self, feature_types: Optional[List[FeatureType]]
) -> List[FeatureType]:
    """Return the feature types to include, defaulting to all types.

    Args:
        feature_types: Explicit list of FeatureType enums, or None for all.

    Returns:
        List of FeatureType enum values.
    """
    return feature_types if feature_types else self._ALL_FWA_TYPES
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
        name_variation_aliases=dict(self.name_variation_aliases),
    )
```

#### `PipelineResult` — Add `zone_feature_map`

```python
@dataclass(frozen=True)
class PipelineResult:
    """Result from full regulation processing pipeline. Contains all state needed for export."""
    feature_to_regs: Dict[str, List[str]] = field(default_factory=dict)
    merged_groups: Dict[str, MergedGroup] = field(default_factory=dict)
    regulation_names: Dict[str, str] = field(default_factory=dict)
    feature_to_linked_regulation: Dict[str, Set[str]] = field(default_factory=dict)
    gazetteer: Optional[MetadataGazetteer] = None
    stats: Optional[RegulationMappingStats] = None
    provincial_feature_map: Dict[str, List[str]] = field(default_factory=dict)
    zone_feature_map: Dict[str, List[str]] = field(default_factory=dict)  # NEW
    admin_feature_map: Dict[str, List[str]] = field(default_factory=dict)
    admin_regulation_ids: set = field(default_factory=set)
    admin_area_reg_map: Dict[str, Dict[str, set]] = field(default_factory=dict)
    regulation_details: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    name_variation_aliases: Dict[str, List[str]] = field(default_factory=dict)
```

#### Already Implemented (No Changes Needed)

- **`_get_reg_names()`** — already excludes `zone_` prefix
- **`export_regulations_json()`** — writes `regulation_details` directly; zone entries populated by mapper

## Stacking Semantics: Additive Model

Zone defaults stack additively with waterbody-specific regulations. Both are assigned to matching features; the webapp resolves display priority:

- **Additive (common):** Zone closure + waterbody species restriction → both apply. Correct.
- **Apparent conflict:** Zone quota + waterbody-specific quota → both assigned. Webapp shows waterbody-specific as the active rule, zone default as context.

This matches the provincial regulation pattern: a feature inside a National Park gets both `prov_nat_parks_closed` and any waterbody-specific regulations. The `source: "zone"` vs `source: "synopsis"` field in `regulations.json` enables frontend differentiation.

> If pipeline-level conflict resolution is needed later, add `override_priority` to `ZoneRegulation` with `"default_only"` mode that skips features already having waterbody-specific rules for the same restriction type.

## Performance

Zone regulations use pre-computed metadata lookups (dict scan on `zones` field), not GPKG spatial intersection. Index build: one pass over ~3.4M features (~5–10s). Per-regulation resolution: < 1 second.

**Memory:** The zone index stores references to existing metadata dicts (no copies). ~50 bytes overhead per entry × ~3.4M entries ≈ ~170 MB.

> **⚠ Performance Warning:** Currently `feature_to_regs` contains only features matched by synopsis or provincial regulations (tens of thousands). Zone regulations apply to **every feature** in covered zones, adding up to ~3.4M features to `feature_to_regs` and `merge_features()`. This could increase merge runtime and output size by orders of magnitude. Mitigations: (a) only enrich features that already have at least one other regulation, or (b) profile the actual merge impact before committing to the fully additive model.

## Compatibility

All existing pipeline components are compatible without modification:

| Component | Status |
|-----------|--------|
| **Linker** | No interaction — zone regs bypass name-based linking entirely |
| **ScopeFilter** | No interaction — zone regs don't use spatial scope filtering |
| **TributaryEnricher** | No interaction — zone membership is inherent in feature metadata |
| **MetadataGazetteer** | Compatible — `metadata[FeatureType]` dicts read directly by zone index builder |
| **GeoExporter** | `_get_reg_names()` zone exclusion + `regulation_details` export already implemented |
| **Search Index** | Zone IDs appear in `regulation_ids` but excluded from `regulation_names`/`name_variants` |
| **Tests** | `zone_feature_map` defaults to `{}` — backward compatible |
| **Sub-polygon proposal** | Sub-polygons inherit parent `zones` → zone regs apply automatically |

**Design notes:**
- Multi-zone boundary features (e.g., `zones=["4", "7A"]`) correctly receive regulations from both zones via the zone index
- `matched_ids` uses a **set** to prevent duplicate `feature_to_regs` entries
- `FeatureType.UNGAZETTED` is included in the zone index so ungazetted waterbodies receive zone defaults
- `_disabled` convention matches provincial pattern (`getattr(r, "_disabled", False)`)
- All three regulation types (`AdminDirectMatch`, `ProvincialRegulation`, `ZoneRegulation`) use `feature_types: Optional[List[FeatureType]]` with `None` = all types — consistent interface across the pipeline

---

## Implementation Effort Breakdown

### Summary

| Area | Files | Lines Changed | Effort |
|------|-------|---------------|--------|
| New module (dataclass + data) | 1 new | ~150–300 | Low |
| RegulationMapper (processing) | 1 modify | ~130 added | Medium |
| PipelineResult (1 field) | 1 modify | ~2 | Trivial |
| __init__.py (optional export) | 1 modify | ~2 | Trivial |
| Webapp (TypeScript) | 2 modify | ~30 | Low |
| **Total** | **4–5 files** | **~315–465** | **Low–Medium** |

No existing tests break (new field defaults to `{}`). No exporter changes.

### File-by-File Breakdown

#### 1. `regulation_mapping/zone_base_regulations.py` — CREATE (~150–300 lines)

New file, mirrors `provincial_base_regulations.py` structure:

| Section | Lines | Notes |
|---------|-------|-------|
| Module docstring | ~30 | Explains purpose, ID conventions, zone-wide vs direct-match modes |
| `ZoneRegulation` dataclass | ~45 | 5 required fields, 2 scope fields, 8 direct-match fields, `has_direct_target()` |
| `ZONE_BASE_REGULATIONS` list | ~50–200 | Phase 1: 2–3 examples. Phase 2: 50–80 entries. Each entry ~8–15 lines |
| `_run_zone_test()` + `__main__` | ~30 | CLI test, same pattern as provincial module |

This is the **only new file**. The dataclass is fully defined in the proposal — just copy and add entries.

#### 2. `regulation_mapping/regulation_mapper.py` — MODIFY (~130 lines added)

Five new methods + two small edits:

| Change | Lines | Complexity |
|--------|-------|------------|
| `_process_zone_regulations()` | ~45 | Low — follows `_process_provincial_regulations()` pattern exactly. Import, iterate active regs, dispatch to zone-wide or direct-match resolver, populate `regulation_names`, `feature_to_regs`, `feature_to_linked_regulation`, `regulation_details`. |
| `_resolve_zone_wide()` | ~20 | Low — nested loop over zone index, optional MU filter, waterbody key backfill. |
| `_resolve_zone_direct_match()` | ~45 | Low — sequential ID field lookups via existing gazetteer methods. Identical pattern to `linker.py:_apply_direct_match()` but reads from `ZoneRegulation` fields instead of `DirectMatch`. |
| `_build_zone_feature_index()` | ~15 | Trivial — single pass over `gazetteer.metadata`, builds `{zone_id: {FeatureType: {fid: meta}}}`. |
| `_resolve_feature_types()` | ~5 | Trivial — returns input or default `_ALL_FWA_TYPES` list. |
| `run()` — add Phase 2.5 call | ~2 | Trivial — one line: `zone_feature_map = self._process_zone_regulations()`, one line: pass to `PipelineResult`. |
| `PipelineResult` — add field | ~2 | Trivial — `zone_feature_map: Dict[str, List[str]] = field(default_factory=dict)` |

**No existing methods are modified** except `run()` (2 lines) and `PipelineResult` (1 field). All new code is additive.

#### 3. `regulation_mapping/__init__.py` — MODIFY (optional, ~2 lines)

Add `ZoneRegulation` to imports/`__all__` if external consumers need it. Not strictly required — only `regulation_mapper.py` imports from the new module.

#### 4. Webapp — MODIFY (~30 lines across 2 files)

The webapp already has a pattern for handling `source: "provincial"` — zone regulations slot in with minimal effort:

**`webapp/src/services/regulationsService.ts`** (~10 lines):
- Extend `Regulation.source` type: `'synopsis' | 'provincial' | 'zone'` (1 line)
- Extend `provincialRuleTexts` filter on load to also collect `source === 'zone'` rule texts (1 line change), OR rename to `nonSynopsisRuleTexts` and filter `source !== 'synopsis'` (cleaner, ~3 lines)

**`webapp/src/components/InfoPanel.tsx`** (~20 lines):
- Add zone-specific grouping branch alongside the existing provincial branch (~5 lines). Zone regs carry `zone_ids` in `regulation_details` — can display as "Region X — Zone Default" header.
- Add "Zone Regulation" badge (copy provincial badge, change color/text) (~12 lines)
- Zone regs don't have `scope_location` in the admin sense, so the existing `SCOPE_LOCATION_LABELS` fallback is fine — no changes needed there.

**No changes needed in**: `SearchBar.tsx`, `Map.tsx`, `DisambiguationMenu.tsx` — these all funnel through `filterOutProvincialNames()` which, once extended, handles zone names automatically.

### What Does NOT Need Changing

| Component | Why |
|-----------|-----|
| `geo_exporter.py` | `_get_reg_names()` already excludes `zone_` prefix. `export_regulations_json()` writes `regulation_details` directly. |
| `linker.py` | Zone regs bypass linking entirely. |
| `scope_filter.py` | Not used by zone regs. |
| `tributary_enricher.py` | Zone membership is inherent in metadata. |
| `metadata_gazetteer.py` | Existing `metadata[FeatureType]` dicts + gazetteer search methods are sufficient. |
| `regulation_pipeline.py` | Calls `mapper.run()` — zone processing is internal to mapper. |
| Search index / PMTiles export | Zone regulation IDs flow through existing `merged_groups` → export pipeline. |
| Existing tests | `PipelineResult` new field has `default_factory=dict` — backward compatible. |

### Effort Estimate by Phase

| Phase | Scope | Effort | Risk |
|-------|-------|--------|------|
| **Phase 1: Infrastructure** | Dataclass + 3 example entries + 5 mapper methods + `run()` update + `PipelineResult` field | **2–3 hours** | Low — all patterns already exist in provincial regs |
| **Phase 2: Data Entry** | Read 9 region preambles, encode 50–80 `ZoneRegulation` entries | **3–5 hours** | Low — manual but mechanical. Highest time cost. |
| **Phase 3: Webapp** | Type update + name filter + badge + grouping | **1–2 hours** | Low — extends existing provincial pattern |
| **Phase 4: Validation** | Pipeline run + spot-check + merge group review | **1–2 hours** | Medium — performance warning applies (see below) |
| **Total** | | **7–12 hours** | |

### Key Risk: Merge Performance (Phase 4)

The single real risk is the merge step. Currently `feature_to_regs` holds ~10–50K features (synopsis + provincial matches). Zone regulations will add **every feature in covered zones** to `feature_to_regs`:

- 9 zone-wide stream closures → ~2.6M stream features enter `merge_features()`
- 9 zone-wide lake regulations → ~390K lake features

`merge_features()` calls `get_feature_by_id()` per feature (gazetteer dict lookup) and groups by `(grouping_key, reg_set)`. With ~3M features this is likely **minutes, not seconds**. The tqdm progress bar helps visibility but doesn't reduce the work.

**Recommended approach for Phase 1:** Start with 2–3 zone regulations targeting a smaller zone (e.g., Zone 8 — Okanagan, ~77K features) and profile the merge step before adding all 9 zones. This answers the performance question early with low risk.

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
        all_types = [FeatureType.STREAM, FeatureType.LAKE,
                     FeatureType.WETLAND, FeatureType.MANMADE]
        target_types = zr.feature_types or all_types

        for zone_id in zr.zone_ids:
            for ft in target_types:
                count += len(index.get(zone_id, {}).get(ft, {}))

        type_names = [ft.value for ft in target_types] if zr.feature_types else "ALL"
        print(f"  {zr.regulation_id}: zones={zr.zone_ids}, "
              f"types={type_names}, features={count:,}")

if __name__ == "__main__":
    _run_zone_test()
```

## Open Questions

### Q1: Should Zone Regulations Apply to Wetlands?

The synopsis preamble focuses on streams and lakes. Wetlands are typically not mentioned in zone defaults. Options:
- **A) Exclude wetlands by default** — set `feature_types=[FeatureType.STREAM, FeatureType.LAKE]` on most zone regs
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

**Recommendation:** `zone_r{N}_...` pattern (e.g., `zone_r1_stream_closure`, `zone_r7a_set_lines_lakes`). Matches synopsis structure and is readable.
