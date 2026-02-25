"""
Provincial Base Regulations
============================

Defines province-wide fishing regulations that apply universally to BC waters.
These are NOT parsed from the synopsis PDF — they are core BC fishing regulations
that apply to ALL features within administrative boundaries or of specific types.

Overview
--------
Provincial regulations supplement the synopsis-derived, waterbody-specific
regulations. They capture blanket rules like "fishing prohibited in all
National Parks" that cannot be extracted from the synopsis tables.

Two regulation scopes (auto-detected from fields):

1. ``admin_layer`` is set → Applies to all FWA features within ALL polygons
   of that layer (or a subset selected by ``code_filter`` / ``feature_ids``
   / ``feature_names``).
   - Example: "Fishing prohibited in all National Parks"
   - Example: "Fishing prohibited in all Ecological Reserves"

2. ``feature_types`` is set → Applies to all FWA features of specific types
   (e.g. all streams, all lakes).
   - Example: "All streams — single barbless hook required"
   - NOT IMPLEMENTED YET — placeholder for future use.

Regulation IDs & the ``prov_`` Prefix
--------------------------------------
Every ``ProvincialRegulation.regulation_id`` **must** start with ``prov_``.
This prefix is used downstream to distinguish provincial rules from
synopsis-derived rules:

* **RegulationMapper** (``regulation_mapper.py``) processes provincial
  regulations via ``_process_provincial_regulations()`` and assigns the
  resulting ``regulation_id`` values to intersecting FWA features.
* **GeoExporter** (``geo_exporter.py``) uses the ``prov_`` prefix in
  ``_get_reg_names()`` to **exclude** provincial regulations from the
  human-readable regulation names that appear in the search index and
  map tiles — these broad rules are not waterbody-specific names.
* The ``restriction`` dict for each regulation is exported alongside
  synopsis-derived rules in the final ``regulations.json`` output.

Admin Layer Types
-----------------
Layer keys correspond to the layers fetched by ``fetch_data.py`` and stored
in the GeoPackage. Each key maps to a BC Data Catalogue layer:

    - ``"parks_nat"``       → National Parks (CLAB_NATIONAL_PARKS)
    - ``"parks_bc"``        → Provincial Parks & Ecological Reserves
                              (TA_PARK_ECORES_PA_SVW)
    - ``"wma"``             → Wildlife Management Areas
                              (TA_WILDLIFE_MGMT_AREAS_SVW)
    - ``"watersheds"``      → Named Watersheds (FWA_NAMED_WATERSHEDS_POLY)
    - ``"historic_sites"``  → Historic Sites
                              (HIST_HISTORIC_ENVIRONMNT_PA_SV)

Code Filtering
--------------
When ``code_filter`` is set, only admin polygons whose classification code
matches one of the given values are used for intersection. The relevant
code field for each layer is defined in ``ADMIN_LAYER_CONFIG`` (from
``metadata_gazetteer.py``). For example, the ``"parks_bc"`` layer uses
``PROTECTED_LANDS_CODE``: filter value ``"OI"`` selects Ecological Reserves.

Adding New Regulations
----------------------
1. Create a new ``ProvincialRegulation`` entry in
   ``PROVINCIAL_BASE_REGULATIONS``.
2. Use the ``prov_`` prefix for ``regulation_id``.
3. Set ``admin_layer`` (for boundary-scoped) **or** ``feature_types``
   (for type-scoped, once implemented).
4. Populate ``restriction`` with at least ``type`` and ``details`` keys.
5. Run the CLI test: ``python -m regulation_mapping.provincial_base_regulations``
   to verify layer intersection counts.
6. Re-run the regulation pipeline to propagate the new regulation to all
   affected FWA features.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional

from fwa_pipeline.metadata_gazetteer import ADMIN_LAYER_CONFIG


@dataclass
class ProvincialRegulation:
    """
    A provincial base regulation that applies universally.

    Scope type is auto-detected:
    - If admin_layer is set → applies to all FWA features inside admin polygons
    - If feature_types is set → applies to all FWA features of those types (future)

    Attributes:
        regulation_id: Unique identifier (e.g., "prov_nat_parks_closed")
        rule_text: Human-readable regulation text
        restriction: Regulation details dict (type, species, details, etc.)
        notes: Additional context or source references

        admin_layer: Layer name for admin boundary scope
        feature_ids: Specific feature IDs to match within the admin layer
        feature_names: Name(s) to search for in the admin layer
        code_filter: Classification codes to pre-filter the layer by (e.g.,
                     ["OI"] for ecological reserves in parks_bc). Only effective
                     when the layer defines a code_field in ADMIN_LAYER_CONFIG.
        include_streams: Include stream features in spatial intersection
        include_lakes: Include lake features in spatial intersection
        include_wetlands: Include wetland features in spatial intersection
        include_manmade: Include manmade waterbody features in spatial intersection

        feature_types: List of FWA feature types for type-based scope (future)
    """

    regulation_id: str
    rule_text: str
    restriction: Dict[str, Any]
    notes: str

    # Admin boundary scope
    admin_layer: Optional[str] = None
    feature_ids: Optional[List[int]] = None
    feature_names: Optional[List[str]] = None
    code_filter: Optional[List[str]] = None

    # Feature type inclusion for admin boundary spatial intersection
    include_streams: bool = True
    include_lakes: bool = True
    include_wetlands: bool = True
    include_manmade: bool = True

    # Feature type scope (future - all streams, all lakes, etc.)
    feature_types: Optional[List[str]] = None

    @property
    def scope_type(self) -> str:
        """Infer scope type from which fields are set."""
        if self.admin_layer:
            return "admin_boundary_all"
        elif self.feature_types:
            return "feature_type_all"
        return "unknown"


# ============================================================================
# Provincial Base Regulations
#
# Active entries are processed by RegulationMapper._process_provincial_regulations().
# Commented-out entries are future placeholders — uncomment and implement the
# feature_types scope path in RegulationMapper before enabling them.
#
# Each entry's ``restriction`` dict is included verbatim in the exported
# regulations.json under the regulation_id key.
# ============================================================================

PROVINCIAL_BASE_REGULATIONS: List[ProvincialRegulation] = [
    # ========================================
    # NATIONAL PARKS - Fishing Prohibited
    # ========================================
    ProvincialRegulation(
        regulation_id="prov_nat_parks_closed",
        rule_text=(
            "Freshwater fishing is prohibited in National Parks unless opened "
            "under the National Parks Fishing Regulations. Where open, anglers "
            "require a National Park Fishing Permit to fish in park waters. "
            "A provincial angling licence is not valid unless otherwise stated "
            "for any fresh water within National Parks or National Park Reserves. "
            "All fresh waters within Pacific Rim National Park Reserve, Gwaii Haanas "
            "National Park Reserve and Gulf Islands National Park Reserve are closed "
            "to fishing."
        ),
        admin_layer="parks_nat",
        include_streams=True,
        include_lakes=True,
        include_wetlands=True,
        include_manmade=True,
        restriction={
            "type": "Closed",
            "species": ["all"],
            "details": (
                "Fishing prohibited unless opened under National Parks Fishing Regulations. "
                "National Park Fishing Permit required where open."
            ),
        },
        notes=(
            "Applies to all 7 National Parks in BC: Kootenay, Yoho, Glacier, "
            "Mount Revelstoke, Pacific Rim, Gwaii Haanas, Gulf Islands. "
            "Source: Provincial Regulations, 2025-2027 Synopsis, pages 9-10."
        ),
    ),
    # ========================================
    # ECOLOGICAL RESERVES - Fishing Prohibited
    # ========================================
    ProvincialRegulation(
        regulation_id="prov_eco_reserves_closed",
        rule_text="Fishing is prohibited in Ecological Reserves in B.C.",
        admin_layer="parks_bc",
        code_filter=["OI"],  # PROTECTED_LANDS_CODE 'OI' = Ecological Reserve
        include_streams=True,
        include_lakes=True,
        include_wetlands=True,
        include_manmade=True,
        restriction={
            "type": "Closed",
            "species": ["all"],
            "details": "Fishing prohibited in all Ecological Reserves.",
        },
        notes=(
            "Applies to all Ecological Reserves province-wide. "
            "Uses parks_bc layer filtered by PROTECTED_LANDS_CODE='OI'. "
            "Source: Provincial Regulations, 2025-2027 Synopsis, page 9."
        ),
    ),
    #
    # ========================================
    # FUTURE PROVINCIAL REGULATIONS (PLACEHOLDERS)
    # ========================================
    #
    # --- Barbed Hook Ban (All Streams) ---
    # ProvincialRegulation(
    #     regulation_id="prov_streams_barbless_hooks",
    #     rule_text=(
    #         "It is unlawful to use barbed hooks or a hook with more than one point "
    #         "in any river, stream, creek or slough in B.C. "
    #         "Note: the use of barbed hooks in lakes is permitted, unless noted "
    #         "in the Regional Water-Specific Tables."
    #     ),
    #     feature_types=["stream"],
    #     restriction={
    #         "type": "Gear Restriction",
    #         "details": "Barbless hooks only; single-point hooks only.",
    #     },
    #     notes="Source: Provincial Regulations, 2025-2027 Synopsis, page 8.",
    # ),
    #
    # --- Single Line Limit ---
    # ProvincialRegulation(
    #     regulation_id="prov_single_line",
    #     rule_text=(
    #         "Your basic fishing licence entitles you to angle with one fishing line "
    #         "to which only one hook, one artificial lure OR one artificial fly is attached."
    #     ),
    #     feature_types=["stream", "lake", "wetland", "manmade"],
    #     restriction={
    #         "type": "Gear Restriction",
    #         "details": "One fishing line; one hook, lure, or fly.",
    #     },
    #     notes="Source: Provincial Regulations, 2025-2027 Synopsis, page 8.",
    # ),
    #
    # --- Snagging Prohibition ---
    # ProvincialRegulation(
    #     regulation_id="prov_no_snagging",
    #     rule_text=(
    #         "It is unlawful to snag (foul hook) fish. "
    #         "Any fish willfully or accidentally snagged must be released immediately."
    #     ),
    #     feature_types=["stream", "lake", "wetland", "manmade"],
    #     restriction={
    #         "type": "Method Restriction",
    #         "details": "Snagging (foul hooking) prohibited. Release immediately if snagged.",
    #     },
    #     notes="Source: Provincial Regulations, 2025-2027 Synopsis, page 8.",
    # ),
    #
    # --- Protected Species ---
    # ProvincialRegulation(
    #     regulation_id="prov_protected_species",
    #     rule_text=(
    #         "It is illegal to fish for, or catch and retain any protected species. "
    #         "If you accidentally catch one, you must release it right away where you captured it."
    #     ),
    #     feature_types=["stream", "lake", "wetland", "manmade"],
    #     restriction={
    #         "type": "Protected Species",
    #         "species": [
    #             "Cultus Lake Sculpin", "Enos Lake Stickleback",
    #             "Misty Lake Stickleback", "Nooksack Dace",
    #             "Paxton Lake Stickleback", "Rocky Mountain Sculpin",
    #             "Shorthead Sculpin", "Salish Sucker",
    #             "Vananda Creek Stickleback", "Vancouver Lamprey",
    #             "Western Brook Lamprey (Morrison Creek population)",
    #             "White Sturgeon (Nechako, Upper Fraser, Kootenay and Columbia populations)",
    #         ],
    #         "details": "Catch and release of protected species required immediately.",
    #     },
    #     notes="Source: Provincial Regulations, 2025-2027 Synopsis, page 9.",
    # ),
    #
    # --- Chumming Prohibition ---
    # ProvincialRegulation(
    #     regulation_id="prov_no_chumming",
    #     rule_text=(
    #         "Chumming - attempting to attract fish by depositing any substance "
    #         "in the water - is prohibited."
    #     ),
    #     feature_types=["stream", "lake", "wetland", "manmade"],
    #     restriction={
    #         "type": "Method Restriction",
    #         "details": "Chumming prohibited.",
    #     },
    #     notes="Source: Provincial Regulations, 2025-2027 Synopsis, page 8.",
    # ),
    #
    # --- Fin Fish Bait Ban ---
    # ProvincialRegulation(
    #     regulation_id="prov_finfish_bait_ban",
    #     rule_text=(
    #         "The use of fin fish (dead or alive) or parts of fin fish other than roe "
    #         "is prohibited throughout the province, with limited exceptions."
    #     ),
    #     feature_types=["stream", "lake", "wetland", "manmade"],
    #     restriction={
    #         "type": "Bait Restriction",
    #         "details": (
    #             "Fin fish bait prohibited. Exceptions: sturgeon fishing in Region 2 "
    #             "(Fraser River, Lower Pitt River, Lower Harrison River) and set lining "
    #             "in lakes of Region 6 or Zone A of Region 7."
    #         ),
    #     },
    #     notes="Source: Provincial Regulations, 2025-2027 Synopsis, page 8.",
    # ),
    #
    # --- Lake Invertebrate Bait Ban ---
    # ProvincialRegulation(
    #     regulation_id="prov_lake_invertebrate_bait_ban",
    #     rule_text=(
    #         "No person shall use as bait or possess for that purpose any freshwater "
    #         "invertebrate at a lake."
    #     ),
    #     feature_types=["lake"],
    #     restriction={
    #         "type": "Bait Restriction",
    #         "details": "Freshwater invertebrate bait prohibited in lakes.",
    #     },
    #     notes="Source: Provincial Regulations, 2025-2027 Synopsis, page 8.",
    # ),
    #
    # --- No Fishing Near Fish Facilities ---
    # ProvincialRegulation(
    #     regulation_id="prov_no_fishing_near_facilities",
    #     rule_text=(
    #         "No fishing within 23 m downstream of the lower entrance to any fishway, "
    #         "canal, obstacle or leap. No fishing within a 100 m radius of any government "
    #         "facility operated for counting, passing or rearing fish."
    #     ),
    #     feature_types=["stream"],
    #     restriction={
    #         "type": "Closed",
    #         "details": (
    #             "No fishing within 23m downstream of fishway/canal/obstacle/leap. "
    #             "No fishing within 100m of fish counting/passing/rearing facilities."
    #         ),
    #     },
    #     notes="Source: Provincial Regulations, 2025-2027 Synopsis, page 9.",
    # ),
    #
    # --- No Spear Fishing Regions 1, 2, 4 ---
    # ProvincialRegulation(
    #     regulation_id="prov_no_spearfishing_r124",
    #     rule_text=(
    #         "No spear fishing of any kind is permitted in Region 1, 2, and 4."
    #     ),
    #     feature_types=["stream", "lake", "wetland", "manmade"],
    #     restriction={
    #         "type": "Method Restriction",
    #         "details": "Spear fishing prohibited (Regions 1, 2, 4 only).",
    #     },
    #     notes=(
    #         "Region-restricted provincial regulation. "
    #         "Source: Provincial Regulations, 2025-2027 Synopsis, page 8."
    #     ),
    # ),
    #
    # --- Hatchery Steelhead Annual Quota ---
    # ProvincialRegulation(
    #     regulation_id="prov_steelhead_annual_quota",
    #     rule_text=(
    #         "The annual province-wide quota for hatchery steelhead is 10. "
    #         "All wild steelhead must be released."
    #     ),
    #     feature_types=["stream"],
    #     restriction={
    #         "type": "Quota",
    #         "species": ["steelhead"],
    #         "details": "Annual quota: 10 hatchery steelhead. All wild steelhead must be released.",
    #     },
    #     notes="Source: Provincial Regulations, 2025-2027 Synopsis, page 7.",
    # ),
    #
    # --- No Live Fish Possession ---
    # ProvincialRegulation(
    #     regulation_id="prov_no_live_fish",
    #     rule_text=(
    #         "It is unlawful to have any live fish in your possession in the wild, "
    #         "or move any live fish or live aquatic invertebrates around the province, "
    #         "or transplant them into any waters of B.C."
    #     ),
    #     feature_types=["stream", "lake", "wetland", "manmade"],
    #     restriction={
    #         "type": "Possession Restriction",
    #         "details": (
    #             "No live fish possession in the wild. No moving live fish/invertebrates. "
    #             "No livewells, stringers, or live bait. Highgrading is illegal."
    #         ),
    #     },
    #     notes="Source: Provincial Regulations, 2025-2027 Synopsis, page 8.",
    # ),
]


# ============================================================================
# CLI Provincial Regulation Test
#
# Run with:  python -m regulation_mapping.provincial_base_regulations
#
# Loads the FWA metadata and admin layers, then spatially intersects each
# active regulation against FWA features. Prints a summary table showing
# how many admin polygons matched and how many FWA features fall within them.
# Useful for verifying layer data and code_filter values.
# ============================================================================


def _run_provincial_test():
    """
    Test provincial base regulations against the GPKG admin layers.

    For each active regulation, loads the admin layer, applies code_filter if
    set, then spatially intersects with FWA features and reports counts.
    """
    import shutil
    from pathlib import Path

    from fwa_pipeline.metadata_gazetteer import MetadataGazetteer, ADMIN_LAYER_CONFIG
    from project_config import get_config

    # --- Terminal formatting helpers ---

    RED = "\033[91m"
    YELLOW = "\033[93m"
    GREEN = "\033[92m"
    CYAN = "\033[96m"
    RESET = "\033[0m"

    def tw(default=80):
        try:
            return shutil.get_terminal_size((default, 20)).columns
        except Exception:
            return default

    def divider(char="="):
        print(char * tw())

    def header(text):
        print()
        divider("=")
        print(text)
        divider("=")
        print()

    config = get_config()
    gpkg_path = config.fwa_data_gpkg

    header("PROVINCIAL BASE REGULATIONS TEST")

    # Load gazetteer + admin layers
    print("Loading FWA metadata...")
    gazetteer = MetadataGazetteer(config.fwa_metadata_path)

    from fwa_pipeline.metadata_builder import FeatureType as _FT

    print(
        f"Loaded {len(gazetteer.metadata.get(_FT.STREAM, {})):,} streams, "
        f"{len(gazetteer.metadata.get(_FT.LAKE, {})):,} lakes"
    )

    print(f"\nSetting GPKG path for spatial operations: {gpkg_path.name}")
    gazetteer.set_gpkg_path(gpkg_path)

    from fwa_pipeline.metadata_builder import (
        ADMIN_FEATURE_TYPES as _AFT,
        ADMIN_LAYER_CONFIG as _ALC,
    )

    print("\nAdmin layer metadata loaded from pickle:")
    for lk, cfg in _ALC.items():
        ftype = cfg["feature_type"]
        layer_data = gazetteer.metadata.get(ftype, {})
        code_field = cfg.get("code_field")
        extra = ""
        if code_field and layer_data:
            from collections import Counter as _Ctr

            code_counts = _Ctr(
                v.get("admin_code") for v in layer_data.values() if v.get("admin_code")
            )
            if code_counts:
                extra = f"  | codes: {dict(code_counts)}"
        print(f"  {lk} ({ftype.name}): {len(layer_data)} features{extra}")

    # Process each regulation
    active_regs = [
        r for r in PROVINCIAL_BASE_REGULATIONS if not getattr(r, "_disabled", False)
    ]
    print(f"\n{len(active_regs)} active provincial regulation(s) to test")

    total_features = 0
    results = []

    for prov_reg in active_regs:
        header(f"REGULATION: {prov_reg.regulation_id}")
        print(f"  Rule:         {prov_reg.rule_text[:100]}...")
        print(f"  Admin layer:  {prov_reg.admin_layer}")
        print(f"  Code filter:  {prov_reg.code_filter}")
        print(
            f"  Include:      streams={prov_reg.include_streams}, lakes={prov_reg.include_lakes}, "
            f"wetlands={prov_reg.include_wetlands}, manmade={prov_reg.include_manmade}"
        )

        if not prov_reg.admin_layer:
            print(f"  {YELLOW}Skipped (no admin_layer set){RESET}")
            results.append((prov_reg.regulation_id, "SKIPPED", 0, 0))
            continue

        # Search admin layer (returns List[FWAFeature] from pickle)
        admin_features = gazetteer.search_admin_layer(
            layer_key=prov_reg.admin_layer,
            feature_ids=prov_reg.feature_ids,
            feature_names=prov_reg.feature_names,
            code_filter=prov_reg.code_filter,
        )

        if not admin_features:
            print(f"  {RED}No admin features found in metadata!{RESET}")
            results.append((prov_reg.regulation_id, "NO_FEATURES", 0, 0))
            continue

        n_polys = len(admin_features)
        names = [f.gnis_name for f in admin_features if f.gnis_name]
        if names:
            if len(names) <= 10:
                for nm in names:
                    print(f"    - {nm}")
            else:
                for nm in names[:5]:
                    print(f"    - {nm}")
                print(f"    ... and {len(names) - 5} more")
        print(f"\n  {CYAN}Admin features matched: {n_polys}{RESET}")

        # Spatial intersection with FWA features
        print(f"  Running spatial intersection...")
        matched_features = gazetteer.find_features_in_admin_area(
            admin_features=admin_features,
            layer_key=prov_reg.admin_layer,
            include_streams=prov_reg.include_streams,
            include_lakes=prov_reg.include_lakes,
            include_wetlands=prov_reg.include_wetlands,
            include_manmade=prov_reg.include_manmade,
            gpkg_path=gpkg_path,
        )

        n_features = len(matched_features)
        total_features += n_features

        # Breakdown by feature type
        from collections import Counter

        type_counts = Counter(f.geometry_type for f in matched_features)
        print(f"\n  {GREEN}FWA features matched: {n_features}{RESET}")
        for ftype, count in sorted(type_counts.items()):
            print(f"    {ftype}: {count}")

        # Sample feature names
        named = [f for f in matched_features if f.gnis_name]
        if named:
            print(f"\n  Sample named features ({min(10, len(named))} of {len(named)}):")
            for f in named[:10]:
                mu_str = ",".join(f.mgmt_units) if f.mgmt_units else "-"
                print(f"    - {f.gnis_name} ({f.geometry_type}) MUs: {mu_str}")

        results.append((prov_reg.regulation_id, "OK", n_polys, n_features))

    # Summary table
    header("SUMMARY")
    col1, col2, col3, col4 = 35, 12, 10, 12
    hdr_row = f"{'Regulation':<{col1}} {'Status':<{col2}} {'Polygons':>{col3}} {'Features':>{col4}}"
    print(hdr_row)
    print("-" * len(hdr_row))
    for reg_id, status, polys, feats in results:
        color = (
            GREEN
            if status == "OK" and feats > 0
            else (RED if status != "OK" else YELLOW)
        )
        print(
            f"{color}{reg_id:<{col1}} {status:<{col2}} {polys:>{col3}} {feats:>{col4}}{RESET}"
        )

    print(f"\nTotal FWA features affected by provincial regulations: {total_features}")
    print()


if __name__ == "__main__":
    _run_provincial_test()
