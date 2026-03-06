"""
Direct matches, skip entries, name variation links and admin matches for waterbody linking.

This module contains manual corrections for the regulation linking pipeline:

1. DIRECT_MATCHES: Explicit FWA feature mappings
   - Manual labels that directly specify which FWA feature(s) to link
   - Bypasses MU filtering (for boundary issues or incorrect MU data)
   - Takes priority over natural search
   - Keys use regulation name_verbatim (exact name from regulation text)
   - Includes former NameVariation entries, now converted to GNIS ID lookups

2. SKIP_ENTRIES: Waterbodies that should not be linked
   - Not found: Searched extensively but couldn't locate in FWA data
   - Ignored: Intentionally skipped (cross-listings, reservoirs on parent rivers)

3. NAME_VARIATION_LINKS: Alternate names for already-linked waterbodies
   - Skips linking (prevents duplicate geometry mapping)
   - Passes the alternate name downstream as a searchable alias
   - Used when the same waterbody appears twice in the synopsis under different names

4. FEATURE_NAME_VARIATIONS: Display names assigned to features by BLK/WBK
   - Assigns a name to unnamed features (e.g., side channels of a river)
   - Causes the feature to group SEPARATELY from the mainstem on the front end
   - The assigned name becomes a searchable name_variant and display_name

5. ADMIN_DIRECT_MATCHES: Administrative boundary feature mappings

Format:
- Use \"ALL REGIONS\" for wildcard patterns that apply everywhere
- Wildcard patterns use * as placeholder, e.g., \"* LAKE'S TRIBUTARIES\"
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from fwa_pipeline.metadata_builder import FeatureType
from .admin_target import AdminTarget


@dataclass
class DirectMatch:
    """
    Maps a regulation name directly to exact FWA identifier(s).

    Pure ID-based lookup - no name searching needed.
    All ID fields are lists — use a single-element list for one target.
    Use any FWA system identifier:

    - gnis_ids: Best for lakes — matches ALL polygons with these GNIS IDs
    - fwa_watershed_codes: Best for streams — matches ALL segments from each watershed code
    - waterbody_poly_ids: For specific polygons by WATERBODY_POLY_ID (most precise)
    - waterbody_keys: For all polygons sharing WATERBODY_KEYs (e.g., all 3 Williston Lake polygons)
    - linear_feature_ids: For specific stream segment IDs
    - blue_line_keys: Matches ALL stream segments AND polygons with these BLKs
    - sub_polygon_ids: Links to synthetic sub-polygon features (see SubPolygon)

    Can combine multiple ID types to match both polygons and streams
    (e.g., slough polygon + tributary streams).

    Priority: Use gnis_ids for lakes, fwa_watershed_codes for streams when available.
    Use waterbody_poly_ids for specific polygons, waterbody_keys for all polygons sharing a key.

    Attributes:
        note: Explanation of why this mapping exists
        gnis_ids: List of GNIS identifiers (matches all features with these GNIS IDs)
        fwa_watershed_codes: List of FWA watershed codes (matches all segments from each)
        waterbody_poly_ids: List of WATERBODY_POLY_IDs (most precise polygon lookup)
        waterbody_keys: List of WATERBODY_KEYs (matches all polygons for each key)
        linear_feature_ids: List of stream segment IDs (for specific stream segments)
        blue_line_keys: List of Blue Line Keys (matches all features from each BLK)
        ungazetted_waterbody_id: Links to a custom UngazettedWaterbody entry
        sub_polygon_ids: List of SubPolygon IDs (links to synthetic sub-polygon features)
        additional_info: Extra text injected as a "Note" rule on the regulation.
            Use for permit requirements, special access info, or other context
            that should appear alongside the synopsis-parsed rules.
    """

    note: str
    gnis_ids: Optional[List[str]] = None
    fwa_watershed_codes: Optional[List[str]] = None
    waterbody_poly_ids: Optional[List[str]] = None
    waterbody_keys: Optional[List[str]] = None
    linear_feature_ids: Optional[List[str]] = None
    blue_line_keys: Optional[List[str]] = None
    ungazetted_waterbody_id: Optional[str] = None  # Links to custom UngazettedWaterbody
    sub_polygon_ids: Optional[List[str]] = None  # Links to SubPolygon features
    additional_info: Optional[str] = None


@dataclass
class AdminDirectMatch:
    """
    Maps a regulation name to administrative boundary polygon(s).

    Used for synopsis regulations that apply to all FWA features within
    a specific administrative area (park, WMA, watershed, etc.).
    The matched polygon(s) are spatially intersected with FWA features
    to assign regulations to all streams/lakes/etc. within the boundary.

    Each admin polygon is identified by an ``AdminTarget(layer, feature_id)``
    pair, enabling multi-layer matching within a single regulation.

    Admin Layer Types (matching fetch_data.py / GPKG layer names):
        - "parks_bc"        → Provincial Parks & Ecological Reserves
        - "parks_nat"       → National Parks
        - "wma"             → Wildlife Management Areas
        - "watersheds"      → Named Watersheds
        - "historic_sites"  → Historic Sites

    Attributes:
        admin_targets: List of AdminTarget(layer, feature_id) pairs.
            Each pair identifies a specific polygon within an admin layer.
            Supports multi-layer matching (e.g., parks + watersheds).
        note: Explanation of the match
        feature_types: Which FWA feature types to include in the spatial
                       intersection. Uses FeatureType enum values. If None,
                       includes all types (STREAM, LAKE, WETLAND, MANMADE).
        additional_info: Extra text injected as a "Note" rule on the regulation.
            Use for permit requirements, special access info, or other context
            that should appear alongside the synopsis-parsed rules.
    """

    admin_targets: List[AdminTarget]
    note: str
    feature_types: Optional[List[FeatureType]] = None  # None = all types
    additional_info: Optional[str] = None


@dataclass
class UngazettedWaterbody:
    """
    Represents a custom waterbody not in the FWA gazetteer.

    Used for waterbodies that appear in regulations but have no corresponding
    polygon or line feature in the Freshwater Atlas.  These are injected into
    the gazetteer at pipeline startup so they participate in linking, merging,
    and geographic export like any other FWA feature.

    Geometry Types:
    - point: Single coordinate [easting, northing]
    - linestring: List of coordinates [[e1, n1], [e2, n2], ...]
    - polygon: List of coordinate rings [[[e1, n1], [e2, n2], [e1, n1]]]

    All coordinates are in **EPSG:3005** (BC Albers).

    Attributes:
        ungazetted_id: Unique identifier (e.g., "UNGAZ_MARSH_POND_R2")
        name: Display name of the waterbody
        geometry_type: Type of geometry - "point", "linestring", or "polygon"
        coordinates: Coordinates in EPSG:3005 (BC Albers):
                     - Point: [easting, northing]
                     - LineString: [[e1, n1], [e2, n2], ...]
                     - Polygon: [[[e1, n1], [e2, n2], [e1, n1]]] (first ring is exterior)
        zones: List of zone numbers this waterbody appears in (e.g., ["2"])
        mgmt_units: List of management unit codes (e.g., ["2-4"])
        note: Explanation of where coordinates came from and why this entry was created
        source_url: Optional URL reference for location/documentation
    """

    ungazetted_id: str
    name: str
    geometry_type: str  # "point", "linestring", or "polygon"
    coordinates: Any  # EPSG:3005 — Point: [e, n], LineString: [[e, n], ...], Polygon: [[[e, n], ...]]
    zones: List[str]
    mgmt_units: List[str]
    note: str
    source_url: Optional[str] = None


@dataclass
class NameVariationLink:
    """
    Maps an alternate regulation name to its primary regulation entry.

    Used when the same waterbody appears in the synopsis under two different
    names (e.g., "CAMERON SLOUGH" and 'LEWIS ("Cameron") SLOUGH').  The
    primary entry links to FWA features normally; this entry:
    - Skips linking (prevents duplicate geometry mapping)
    - Passes the alternate name downstream so the search index includes it
      as a searchable name variant for the same waterbody

    Common scenarios:
    - Historical / Indigenous name → current official name
    - Quoted alternate name → full parenthetical name
    - "See X" redirect entries

    Attributes:
        primary_name: The exact name_verbatim of the primary regulation
                      entry that this is an alternate for (must match a
                      key in DIRECT_MATCHES or a naturally-linked entry).
        note: Explanation of the name relationship.
    """

    primary_name: str
    note: str


@dataclass
class FeatureNameVariation:
    """
    Assigns a **display_name_override** to FWA feature(s) identified by
    blue_line_keys and/or waterbody_keys.

    Used for unnamed features like side channels of a river that share
    regulations with the mainstem but need their own display name and
    separate search grouping on the front end.  Adding a
    FeatureNameVariation causes:

    - ``MergedGroup.display_name_override`` to be set on matching groups
    - ``MergedGroup.display_name`` to return this override (highest priority)
    - The search grouping stage (``_build_waterbodies_list``) to place
      the feature in its own physical group — separated from features
      that share the same watershed code but lack this override
    - A distinct ``frontend_group_id`` so clicking the side channel on the
      map highlights only the side channel, not the mainstem

    Feature merging (``merge_features``) is unaffected — BLKs already
    differ between mainstem and side channel, so they naturally form
    separate ``MergedGroup`` objects.

    At least one of ``blue_line_keys`` or ``waterbody_keys`` must be provided.
    Use lists to target multiple BLKs/WBKs with the same assigned name
    (e.g., a side channel that spans multiple stream segments).

    Attributes:
        name: Display name override to assign (e.g., "Adams River Side Channel")
        note: Explanation of why this name variation exists
        blue_line_keys: List of Blue Line Keys identifying stream features
        waterbody_keys: List of Waterbody Keys identifying polygon features
    """

    name: str
    note: str
    blue_line_keys: Optional[List[str]] = None
    waterbody_keys: Optional[List[str]] = None

    def __post_init__(self):
        if not self.blue_line_keys and not self.waterbody_keys:
            raise ValueError(
                "FeatureNameVariation requires at least one of blue_line_keys or waterbody_keys"
            )


@dataclass
class SkipEntry:
    """
    Marks a regulation waterbody name that should not be linked.

    Use for two scenarios:
    1. Not found: Searched extensively but couldn't locate in FWA data
       - Wrong MU in regulation
       - Lake exists but not in FWA dataset
       - Name exists in different region only

    2. Ignored: Intentionally skipped linking
       - Cross-listed entries between regions
       - Reservoirs using parent river regulations
       - Entries requiring custom polygon subdivision

    NOTE: Alternate / historical names for the same waterbody should use
    NameVariationLink instead — this preserves the alias for search.

    Attributes:
        note: Explanation of why entry is skipped
        not_found: If True, searched but couldn't locate in FWA data
        ignored: If True, intentionally skipped (duplicate/redirect/etc)
    """

    note: str
    not_found: bool = False
    ignored: bool = False


class ManualCorrections:
    """
    Manages direct feature matches, admin direct matches,
    skip entries, and ungazetted waterbodies.

    Provides lookup methods to check for corrections by region and name.
    """

    def __init__(
        self,
        direct_matches: Dict[str, Dict[str, DirectMatch]],
        skip_entries: Dict[str, Dict[str, SkipEntry]],
        ungazetted_waterbodies: Dict[str, UngazettedWaterbody],
        admin_direct_matches: Optional[Dict[str, Dict[str, AdminDirectMatch]]] = None,
        name_variation_links: Optional[Dict[str, Dict[str, NameVariationLink]]] = None,
        feature_name_variations: Optional[Dict[str, List[FeatureNameVariation]]] = None,
    ):
        self.direct_matches = direct_matches
        self.skip_entries = skip_entries
        self.ungazetted_waterbodies = ungazetted_waterbodies
        self.admin_direct_matches = admin_direct_matches or {}
        self.name_variation_links = name_variation_links or {}
        self.feature_name_variations = feature_name_variations or {}

    @staticmethod
    def _resolve_region_dict(lookup_dict: dict, region: str) -> Optional[dict]:
        """Look up region key exactly — no fallbacks.

        Returns the dict for the exact region key, or None.
        No 7A→7 or 7→7A fallback — the caller must use the correct key.
        """
        return lookup_dict.get(region)

    def get_skip_entry(self, region: str, name_verbatim: str) -> Optional[SkipEntry]:
        """Get skip entry for a regulation name in a region."""
        entries = self._resolve_region_dict(self.skip_entries, region)
        if entries is None:
            return None
        return entries.get(name_verbatim)

    def get_direct_match(
        self, region: str, name_verbatim: str
    ) -> Optional[DirectMatch]:
        """Get direct match for a regulation name in a region."""
        entries = self._resolve_region_dict(self.direct_matches, region)
        if entries is None:
            return None
        return entries.get(name_verbatim)

    def get_admin_direct_match(
        self, region: str, name_verbatim: str
    ) -> Optional[AdminDirectMatch]:
        """Get admin direct match for a regulation name in a region."""
        entries = self._resolve_region_dict(self.admin_direct_matches, region)
        if entries is None:
            return None
        return entries.get(name_verbatim)

    def get_ungazetted_waterbody(
        self, ungazetted_id: str
    ) -> Optional[UngazettedWaterbody]:
        """Get ungazetted waterbody by ID."""
        return self.ungazetted_waterbodies.get(ungazetted_id)

    def get_name_variation_link(
        self, region: str, name_verbatim: str
    ) -> Optional[NameVariationLink]:
        """Get name variation link for a regulation name in a region."""
        entries = self._resolve_region_dict(self.name_variation_links, region)
        if entries is None:
            return None
        return entries.get(name_verbatim)

    def get_all_feature_name_variations(self) -> List[FeatureNameVariation]:
        """Return all FeatureNameVariation entries across all regions."""
        all_entries: List[FeatureNameVariation] = []
        for entries in self.feature_name_variations.values():
            all_entries.extend(entries)
        return all_entries


# NOTE: NAME_VARIATIONS has been removed. All name variations have been converted
# to DirectMatch entries using GNIS IDs for guaranteed matching.
# The NameVariation dataclass is kept for backward compatibility but is no longer used.


# Direct feature matches - explicit FWA feature mappings by ID
# These take priority over natural search
# Keys use exact regulation name_verbatim
# Format: {"Region X": {"EXACT REGULATION NAME": DirectMatch(...)}}
DIRECT_MATCHES: Dict[str, Dict[str, DirectMatch]] = {
    "Region 1": {
        "LONG LAKE (Nanaimo)": DirectMatch(
            gnis_ids=["17501"],
            note="Disambiguate using GNIS ID",
        ),
        "LANGFORD LAKE": DirectMatch(
            gnis_ids=["16325"],
            note="FWA has MU 1-2, regulation has MU 1-21 (boundary issue)",
        ),
        "NOLA LAKE": DirectMatch(
            gnis_ids=["1816"],
            note="FWA has MU 1-10, regulation has MU 1-9 (boundary issue)",
        ),
        "PANTHER LAKE": DirectMatch(
            gnis_ids=["21965"],
            note="FWA has MU 1-6, regulation has MU 1-5 (boundary issue)",
        ),
        "PRIOR LAKE": DirectMatch(
            gnis_ids=["26972"],
            note="Direct GNIS ID match",
        ),
        "PROVOST DAM": DirectMatch(
            waterbody_keys=["329101355"],
            note="Unnamed lake/reservoir in FWA. Federal Fisheries Act Schedule: https://laws-lois.justice.gc.ca/eng/regulations/SOR-2008-120/section-sched743254-20220221.html",
        ),
        "PROSPECT LAKE": DirectMatch(
            gnis_ids=["26995"],
            note="Direct GNIS ID match",
        ),
        # "HALL LAKE": DirectMatch(
        #     gnis_ids=["36138"],
        #     note="FWA name is 'Hall Lakes' (plural); correct MU but polygon may be smaller than expected",
        # ),
        "HEALY LAKE'S OUTLET STREAM": DirectMatch(
            gnis_ids=["26468"],
            note="Healy Lake's outlet stream is the South Englishman River (GNIS 26468) in Region 1 MU 1-5.",
        ),
        "MUCHALAT RIVER": DirectMatch(
            fwa_watershed_codes=[
                "930-508366-413291-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Direct watershed code match",
        ),
        "THETIS LAKE": DirectMatch(
            gnis_ids=["21695"],
            note="2 polygons with GNIS 21695 - Thetis Lake",
        ),
        "MAIN LAKE (Quadra Island)": DirectMatch(
            gnis_ids=["12126"],
            note="2 polygons with GNIS 12126 - Main Lake on Quadra Island",
        ),
        "ILLUSION LAKES": DirectMatch(
            gnis_ids=["9801"],
            note="7 polygons with GNIS 9801 - Illusion Lakes",
        ),
        '"ANDERSON" LAKE': DirectMatch(
            gnis_ids=["1657"],
            note="Lake in MU 1-3. FWA has MU 1-6, regulation has MU 1-3 (boundary issue). Piscivorous rainbow trout and kokanee population. BC Lakes Database: survey_id 1129, WBID 00105SANJ, watershed code 930-063200-41400. ACAT 9030. Location found from map.",
        ),
        # Haida Gwaii waterbodies (MUs 6-12, 6-13 now managed as Region 1)
        "COPPER CREEK": DirectMatch(
            fwa_watershed_codes=[
                "950-012855-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice",
        ),
        "DATLAMEN CREEK": DirectMatch(
            fwa_watershed_codes=[
                "940-862496-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice",
        ),
        "DEENA CREEK": DirectMatch(
            fwa_watershed_codes=[
                "950-976286-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice",
        ),
        "HONNA RIVER": DirectMatch(
            fwa_watershed_codes=[
                "940-098825-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice",
        ),
        "MAMIN RIVER": DirectMatch(
            fwa_watershed_codes=[
                "940-885049-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice",
        ),
        "PALLANT CREEK": DirectMatch(
            fwa_watershed_codes=[
                "950-069770-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice",
        ),
        "TLELL RIVER": DirectMatch(
            fwa_watershed_codes=[
                "940-051976-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice",
        ),
        "YAKOUN RIVER": DirectMatch(
            fwa_watershed_codes=[
                "940-906664-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice",
        ),
        "BUTTLE LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["16747"],
            note="Tributaries of Buttle Lake - links to parent waterbody (GNIS 16747)",
        ),
        "(Lower) CAMPBELL LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["17768"],
            note="Tributaries of Campbell Lake - links to parent waterbody (GNIS 17768 - Campbell Lake)",
        ),
        "JOHN HART LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["18905"],
            note="Tributaries of John Hart Lake - links to parent waterbody (GNIS 18905)",
        ),
        '"PETE\'S POND" Unnamed lake at the head of San Juan River': DirectMatch(
            waterbody_keys=["329504349"],
            note="Unnamed lake at the head of San Juan River. BC Lakes Database survey_id 1155: 'A RECONNAISSANCE SURVEY OF PETE'S POND'. Regulation MU 1-3.",
        ),
        'UNNAMED LAKE "A" - MAP A (below)': DirectMatch(
            waterbody_keys=["329500453"],
            note="Unnamed lake in MU 1-10. Location found from map in regulations.",
        ),
        'UNNAMED LAKE "B" - MAP A (below)': DirectMatch(
            waterbody_keys=["329500505"],
            note="Unnamed lake in MU 1-10. Location found from map in regulations.",
        ),
        'UNNAMED LAKE "C" - MAP B (below)': DirectMatch(
            waterbody_keys=["329500435"],
            note="Unnamed lake in MU 1-10. Location found from map in regulations.",
        ),
        'UNNAMED LAKE "D" - MAP B (below)': DirectMatch(
            waterbody_keys=["329500416"],
            note="Unnamed lake in MU 1-10. Location found from map in regulations.",
        ),
        'UNNAMED LAKE "E" - MAP B (below)': DirectMatch(
            waterbody_keys=["329500498"],
            note="Unnamed lake in MU 1-10. Location found from map in regulations.",
        ),
        'UNNAMED LAKE "F" - MAP B (below)': DirectMatch(
            waterbody_keys=["329500447"],
            note="Unnamed lake in MU 1-10. Location found from map in regulations.",
        ),
        'UNNAMED LAKE "G" - MAP B (below)': DirectMatch(
            waterbody_keys=["329500561"],
            note="Unnamed lake in MU 1-10. Location found from map in regulations.",
        ),
        'UNNAMED LAKE "H" - MAP B (below)': DirectMatch(
            waterbody_keys=["328988117"],
            note="Unnamed lake in MU 1-10. Location found from map in regulations.",
        ),
        'UNNAMED LAKE "I" - MAP B (below)': DirectMatch(
            waterbody_keys=["329500475"],
            note='Unnamed lake in MU 1-10 ("Elmer Lake" on Google Maps). Location found from map in regulations.',
        ),
        "MINE LAKE": DirectMatch(
            waterbody_keys=["329095512"],
            note="Lake in MU 1-15. Also labelled as 'Main Lake' in gazette (https://www.canoevancouverisland.com/canoe-kayak-vancouver-island-directory/main-lake-canoe-chain-quadra-island/). Location found from map.",
        ),
        # --- Converted from NameVariation ---
        "TOQUART LAKE": DirectMatch(
            gnis_ids=["26752"],
            note="GNIS name: Toquaht Lake. Spelling mismatch in synopsis.",
        ),
        "TOQUART RIVER": DirectMatch(
            gnis_ids=["2915"],
            note="GNIS name: Toquaht River. Spelling mismatch in synopsis.",
        ),
        "MAGGIE LAKE": DirectMatch(
            gnis_ids=["12098"],
            note="GNIS name: Makii Lake. Renamed in gazette (https://apps.gov.bc.ca/pub/bcgnws/names/62541.html).",
        ),
        "MAHATTA RIVER": DirectMatch(
            gnis_ids=["30809"],
            note="GNIS name: Mahatta Creek. Gazetteer lists as creek.",
        ),
        '"BIG QUALICUM" RIVER': DirectMatch(
            gnis_ids=["27804"],
            note="GNIS name: Qualicum River. Duplicate entry with 'QUALICUM RIVER' in same region.",
        ),
        '"MAXWELL LAKE" (Lake Maxwell)': DirectMatch(
            gnis_ids=["15120"],
            note="GNIS name: Lake Maxwell. Name order differs in gazetteer.",
        ),
        '"STOWELL LAKE" (Lake Stowell)': DirectMatch(
            gnis_ids=["15506"],
            note="GNIS name: Lake Stowell. Name order differs in gazetteer.",
        ),
        '"WESTON LAKE"': DirectMatch(
            gnis_ids=["26096"],
            note="GNIS name: Lake Weston. Name order differs in gazetteer.",
        ),
    },
    "Region 2": {
        "VEDDER RIVER": DirectMatch(
            fwa_watershed_codes=[
                "100-064535-057628-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            gnis_ids=["29662"],
            note="Links to both Vedder River (stream) and Vedder Canal (polygon with GNIS_NAME_2: Vedder River). Canal is irrigation diversion from the river system.",
        ),
        "STAWAMUS RIVER": DirectMatch(
            fwa_watershed_codes=[
                "900-102882-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="FWA has MU 2-8, regulation has MU 2-9 (boundary issue)",
        ),
        "MIAMI CREEK": DirectMatch(
            fwa_watershed_codes=[
                "100-077501-243752-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="FWA has MU 2-18, regulation has MU 2-19 (boundary issue)",
        ),
        "HOPE SLOUGH": DirectMatch(
            fwa_watershed_codes=[
                "100-072260-716322-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="FWA has MUs 2-3, 2-4, regulation has MU 2-8 (boundary issue)",
        ),
        "LUCILLE LAKE": DirectMatch(
            gnis_ids=["37388"],
            note="FWA has MU 2-6, regulation has MU 2-9 (boundary issue); name order: 'lake lucille' in gazetteer",
        ),
        "LILLOOET LAKE, LILLOOET RIVER": DirectMatch(
            gnis_ids=["19926", "10313"],
            note="Combined entry for Lillooet Lake (GNIS 19926) and Lillooet River (GNIS 10313). Regulation MU 2-9.",
        ),
        "CHILLIWACK / VEDDER RIVERS (does not include Sumas River) (see map on page 24)": DirectMatch(
            gnis_ids=["8634", "3062", "29662"],
            note="Combined entry for Chilliwack River (GNIS 8634) and Vedder River (GNIS 3062) and Vedder Canal (GNIS 29662). Regulation MU 2-4.",
        ),
        'LONZO ("Marshall") CREEK': DirectMatch(
            gnis_ids=["1860"],
            note="FWA name is Marshall Creek (GNIS 1860) in Region 2 MU 2-4. Lonzo is alternate name.",
        ),
        "CHILLIWACK LAKE": DirectMatch(
            gnis_ids=["13745"],
            note="FWA has MU 2-3, regulation has MU 2-4 (boundary issue)",
        ),
        "MCLENNAN CREEK": DirectMatch(
            fwa_watershed_codes=[
                "100-052188-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="UNCERTAIN: Could be McLennan Creek (100-052188, MU 2-4) OR McLean Creek (100-025956-073866, MU 2-8). Currently matched to McLennan Creek but regulation specifies MU 2-8 which matches McLean Creek. User should verify correct creek and update if needed.",
        ),
        "NELSON CREEK": DirectMatch(
            fwa_watershed_codes=[
                "900-088087-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "100-019698-194371-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            ],
            note="Two candidates in MU 2-8: (1) Nelson Creek in West Vancouver (WSC 900-088087) - MOST LIKELY match, or (2) Nelson Creek (ditch) near Maillardville Coquitlam (WSC 100-019698-194371). Linked to both for completeness.",
        ),
        "TWIN LAKES": DirectMatch(
            gnis_ids=["30212"],
            note="2 polygons with GNIS 30212 - Twin Lakes",
        ),
        "SOUTH ALOUETTE RIVER": DirectMatch(
            fwa_watershed_codes=[
                "100-025956-057184-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="South Alouette River. Regulation MU 2-8.",
        ),
        "BRUNETTE RIVER'S TRIBUTARIES": DirectMatch(
            gnis_ids=["10070"],
            # fwa_watershed_codes=["100-019698-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"],
            note="Tributaries of Brunette River - links to parent waterbody (GNIS 10070 - Brunette River). Regulation MU 2-8.",
        ),
        "CHEHALIS LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["13012"],
            note="Tributaries of Chehalis Lake - links to parent waterbody (GNIS 13012 - Chehalis Lake). Regulation MU 2-19.",
        ),
        "SQUAMISH RIVER'S TRIBUTARIES": DirectMatch(
            gnis_ids=["25671"],
            # fwa_watershed_codes=["900-105574-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"],
            note="Tributaries of Squamish River - links to parent waterbody (GNIS 25671 - Squamish River). Regulation MU 2-6.",
        ),
        "SQUAMISH POWERHOUSE CHANNEL": DirectMatch(
            linear_feature_ids=[
                "189026121",
                "189025898",
                "189025602",
                "189025634",
                "189025628",
                "189025635",
                "189025602",
                "189025634",
            ],
            note="Squamish Powerhouse Channel in Region 2 MU 2-6. Links to 5 specific stream segments representing the channel. Reference: https://a100.gov.bc.ca/pub/acat/documents/r40717/09_CMS_05_powerhouse_1388682280782_8673845708.pdf",
        ),
        'WAHLEACH ("Jones") LAKE\'S TRIBUTARIES': DirectMatch(
            gnis_ids=["22474"],
            note='Tributaries of Wahleach Lake - links to parent waterbody (GNIS 22474 - Wahleach Lake). Alternate name: "Jones" Lake. Regulation MU 2-3.',
        ),
        "BURNABY LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["10633"],
            note="Tributaries of Burnaby Lake - links to parent waterbody (GNIS 10633)",
        ),
        "MINNEKHADA MARSH": DirectMatch(
            waterbody_keys=["329291857"],
            note="Wetland in Minnekhada Regional Park. BC Lakes Database surveys: 'Minnekhada Regional Park Inventory - 2017; SU17-270318' and 'Minnekhada Regional Park Invasives - 2016; SU16-235849'. Regulation MU 2-8.",
        ),
        "CEDAR LAKE": DirectMatch(
            waterbody_keys=["329522925"],
            note="Lake on boundary with MU 2-2. FWA has MU 2-17, regulation has MU 2-2 (boundary issue). Location found from map.",
        ),
        "CAP SHEAF LAKES": DirectMatch(
            waterbody_keys=["329197919"],
            note="Lake in MU 2-16. Location found from map (https://www.alltrails.com/explore/recording/afternoon-hike-at-placer-mountain-0d770c4?p=-1&sh=li9ufv).",
        ),
        "CHEAM LAKE": DirectMatch(
            waterbody_keys=["329177896"],
            note="Wetland in MU 2-3. Polygon found in FWA wetlands layer. Location found from map.",
        ),
        "GREEN TIMBERS LAKE": DirectMatch(
            waterbody_keys=["360887617"],
            note="Lake in MU 2-4. Polygon found in FWA manmade waterbodies layer. Location found from map.",
        ),
        "JERRY SULINA PARK POND": DirectMatch(
            waterbody_keys=["329292590"],
            note="Unnamed pond in MU 2-8. Location found from map.",
        ),
        "MACLEAN PONDS": DirectMatch(
            waterbody_keys=["329292212"],
            note="Unnamed in FWA lakes layer. ID 070111626. Location found from map.",
        ),
        "MARSH POND": DirectMatch(
            ungazetted_waterbody_id="UNGAZ_MARSH_POND_R2",
            note="No polygon in FWA. Using custom ungazetted waterbody with coordinates from KML point in Aldergrove Regional Park.",
        ),
        '"MOSS POTHOLE" LAKES': DirectMatch(
            waterbody_keys=[
                "329178304",
                "329178235",
                "329178228",
                "329177990",
                "329178239",
                "329177977",
                "329178164",
                "329178222",
                "329178406",
                "329178011",
            ],
            note="Group of lakes in MU 2-18. Multiple polygons. Location found from map.",
        ),
        "WEAVER LAKE and WEAVER CREEK": DirectMatch(
            gnis_ids=["25954", "25951"],
            note="MU 2-19. GNIS 25954 (Weaver Lake) and GNIS 25951 (Weaver Creek).",
        ),
        "HATZIC LAKE AND SLOUGH": DirectMatch(
            gnis_ids=["16118", "16120", "37373"],
            note="MU 2-8. GNIS 16118 (Hatzic Lake), GNIS 16120 (Hatzic Slough) and GNIS 37373 (Lower Hatzic Slough).",
        ),
        "NICOMEN SLOUGH": DirectMatch(
            gnis_ids=["21105"],
            waterbody_poly_ids=[
                "700091672",
                "700091686",
                "700091381",
                "700091224",
                "700012846",
                "700000668",
                "700091322",
            ],
            note="Nicomen Slough (GNIS 21105) in Region 2. Includes 7 specific waterbody polygons plus the GNIS stream match.",
        ),
        # --- Converted from NameVariation ---
        '"ERROCK" ("Squakum") LAKE': DirectMatch(
            gnis_ids=["1757"],
            note="GNIS name: Lake Errock. Name order differs in gazetteer.",
        ),
        "MCKAY CREEK": DirectMatch(
            gnis_ids=["32057"],
            note="GNIS name: Mackay Creek. Spelling correction.",
        ),
        "SARDIS PARK POND": DirectMatch(
            gnis_ids=["37696"],
            note="GNIS name: Sardis Pond. Name simplification.",
        ),
        '"JONES" LAKE': DirectMatch(
            gnis_ids=["22474"],
            note="GNIS name: Wahleach Lake. Labelled as Wahleach Lake in GIS (https://www.bchydro.com/community/recreation_areas/jones_lake.html).",
        ),
        '"PAQ" LAKE': DirectMatch(
            gnis_ids=["2159"],
            note="GNIS name: Lily Lake. Known locally as Lily Lake.",
        ),
        "SWELTZER CREEK": DirectMatch(
            gnis_ids=["23072"],
            note="GNIS name: Sweltzer River. Labelled as Sweltzer River in GIS.",
        ),
        "BEAR (Mahood) CREEK": DirectMatch(
            gnis_ids=["12117", "12114"],
            note="GNIS name: Mahood Creek. Two GNIS entries for same waterbody.",
        ),
        '"MARSHALL" CREEK': DirectMatch(
            gnis_ids=["1860"],
            note="GNIS name: Marshall Creek. Remove quotes.",
        ),
        "FRASER RIVER (Upstream Of The Cpr Bridge At Mission)": DirectMatch(
            gnis_ids=[
                "39325",  # Fraser River
                "10494",  # Annacis Channel
                "11566",  # Bedford Channel
                "17805",  # Cannery Channel
                "17368",  # Enterprise Channel
                "14905",  # Morey Channel
                "22631",  # Parsons Channel
                "21224",  # Sapperton Channel
                "14010",  # Gilmour Slough
                "11481",  # Greyell Slough
                "13499",  # Maria Slough
                "25152",  # Tilbury Slough
                "8341",  # Williamson Slough
            ],
            note="Fraser River (GNIS 39325) upstream of the CPR Bridge at Mission, plus all named channels and sloughs without their own regulation entries. Region 2. Nicomen Slough and Strawberry Slough excluded — they have separate regulation entries.",
        ),
    },
    "Region 3": {
        "COLDWATER RIVER'S TRIBUTARIES": DirectMatch(
            gnis_ids=["18066"],
            # fwa_watershed_codes=["100-190442-244975-337574-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"],
            note="Tributaries of Coldwater River - links to parent waterbody (GNIS 18066 - Coldwater River). Regulation MU 3-13.",
        ),
        "RAINBOW LAKE": DirectMatch(
            waterbody_keys=["329452040"],
            note="Unnamed lake in Region 3 MU 3-12; found via BC Lakes Database (survey_id 4782: 'A RECONNAISSANCE SURVEY OF RAINBOW LAKE'). FWA has no GNIS ID.",
        ),
        "LLOYD LAKE": DirectMatch(
            gnis_ids=["33438"],
            note="FWA has MU 3-29, regulation has MU 3-30 (boundary issue)",
        ),
        "MCARTHUR ISLAND SLOUGH": DirectMatch(
            # waterbody_keys=["329564232"],
            # linear_feature_ids=[
            #     "703312800",
            #     "703312651",
            #     "703313162",
            #     "703312745",
            #     "703312290",
            # ],
            blue_line_keys=["355994157"],
            note="Slough polygon + tributary stream segments",
        ),
        "MAKA CREEK": DirectMatch(
            fwa_watershed_codes=[
                "100-190442-244975-232973-504304-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "100-190442-244975-119256-796149-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            ],
            note="Two candidates in MU 3-13: (1) WSC 100-190442-244975-232973-504304 (103 segments) - MOST LIKELY match as it is found inside fisheries sensitive wildlife area, or (2) WSC 100-190442-244975-119256-796149 (9 segments). Linked to both for completeness.",
        ),
        "NAHATLATCH LAKE (east and west)": DirectMatch(
            gnis_ids=["16470"],
            note="2 polygons with GNIS 16470 - Nahatlatch Lake (east and west)",
        ),
        "DEEP LAKE": DirectMatch(
            waterbody_keys=["329321237"],
            note="Unnamed lake in MU 3-28. Location found from stocked lake map.",
        ),
        "JACKPINE LAKE": DirectMatch(
            waterbody_keys=["329320955"],
            note="Unnamed lake in MU 3-28. Location found from map. Other Jackpine Lakes exist in Region 5 (GNIS 16938, MU 5-2) and Region 8 (GNIS 16941, MU 8-11).",
        ),
        '"NORMAN" LAKE (unnamed lake approximately 600 m southeast of Durand Lake)': DirectMatch(
            waterbody_keys=["329563878"],
            note="Unnamed lake in MU 3-19, approximately 600 m southeast of Durand Lake. BC Lakes Database: WBID 00719THOM. ACAT 15541. Other Norman Lakes exist in Region 7 and Region 8. Location found from map.",
        ),
        "ROSE LAKE": DirectMatch(
            waterbody_keys=["329534365"],
            note="Lake in MU 3-20. BC Lakes Database: WBID 00776STHM. ACAT 15585. Other Rose Lakes exist in Region 2, 5, and 6. Location found from map.",
        ),
        "CLANWILLIAM LAKE": DirectMatch(
            waterbody_keys=["329518210"],
            note="Unnamed in FWA lakes layer. MU 3-34. BC Lakes Database: WBID 523394. ACAT ObjectID 1122331271. Gazetted name: CLANWILLIAM LAKE. Location found from map.",
        ),
        "LITTLE DUM LAKE": DirectMatch(
            waterbody_keys=["329321212"],
            note="Lake in MU 3-28. BC Lakes Database: WBID 00618LNTH. ACAT ObjectID 15306 ('A Reconnaissance Survey Of Dum 2'). Found in stocked lakes map. Part of Dum Lake group (gazette: https://apps.gov.bc.ca/pub/bcgnws/names/27972.html).",
        ),
        "LITTLE LAC DES ROCHES (at west end of Lac Des Roches)": DirectMatch(
            waterbody_keys=["329320894"],
            note="Lake in MU 3-30, at west end of Lac Des Roches. Location found from map.",
        ),
        '"LITTLE PETER HOPE" LAKE (unnamed lake approximately 200 m southwest of Peter Hope Lake)': DirectMatch(
            waterbody_keys=["329316229"],
            note="Unnamed lake in MU 3-20, approximately 200 m southwest of Peter Hope Lake. BC Lakes Database: WBID 00497LNIC. ACAT ObjectID 10044. FWA Watershed Code: 120-246600-53700-23700-5090-0000-000-000-000-000-000-000. Location found from map.",
        ),
        "LORENZO LAKE": DirectMatch(
            waterbody_keys=["329354793"],
            note="Lake in MU 3-39. BC Lakes Database: WBID 02102MAHD. ACAT ObjectID 7147. FWA Watershed Code: 129-360400-23900-98400-4800-9150-000-000-000-000-000-000. Location found from map.",
        ),
        "LOWER KANE LAKE": DirectMatch(
            waterbody_keys=["329316143"],
            note="Lake in MU 3-13. BC Lakes Database: WBID 01088LNIC. ACAT ObjectID 31446. FWA Watershed Code: 120-246600-33700-41300-7100-0000-000-000-000-000-000-000. Same regulations as Upper Kane Lake. Location found from map.",
        ),
        "UPPER KANE LAKE": DirectMatch(
            waterbody_keys=["329316130"],
            note="Lake in MU 3-13. BC Lakes Database: WBID 01083LNIC. ACAT ObjectID 4123. FWA Watershed Code: 120-246600-33700-41300-7100-0000-000-000-000-000-000-000. Same regulations as Lower Kane Lake. Location found from map.",
        ),
        "SICAMOUS NARROWS": DirectMatch(
            linear_feature_ids=["703030326"],
            # waterbody_poly_ids=["700189163"],  # Future: river polygon when river polygons are added to matching
            note="Specific segment of Shuswap River in MU 3-26. Linear feature ID 703030326 represents the Sicamous Narrows portion. Also associated with river polygon 700189163 (will match in future when river polygons are added to linking system).",
        ),
        "TULIP LAKE": DirectMatch(
            waterbody_keys=["329534212"],
            note="Lake in MU 3-20. BC Lakes Database: WBID 00762STHM. ACAT ObjectID 49860. FWA Watershed Code: 128-123700-73700-41000-0000-0000-000-000-000-000-000-000. Location found from map.",
        ),
        # --- Converted from NameVariation ---
        '"MORGAN" LAKE': DirectMatch(
            gnis_ids=["14911"],
            note="GNIS name: Morgan Lake. Remove quotes.",
        ),
        "KWOTLENEMO (Fountain) LAKE": DirectMatch(
            gnis_ids=["5719"],
            note="GNIS name: Kwotlenemo (Fountain) Lake. Parenthetical included in GNIS.",
        ),
        "FRASER RIVER": DirectMatch(
            gnis_ids=["39325"],
            note="Fraser River in Region 3. Channels and sloughs are in Zone 2 only.",
        ),
    },
    "Region 4": {
        '"ALTA" LAKE': DirectMatch(
            waterbody_keys=["328965040"],
            note="Unnamed lake in MU 4-3. Location found from map. Low confidence - Alta Lake GNIS 7915 is in Region 2 MUs 2-11, 2-9.",
        ),
        '"ALCES" LAKE': DirectMatch(
            waterbody_keys=["329247797"],
            note="Lake in MU 4-24. BC Lakes Database: WBID 00374KOTR, ACAT 280743. Gazetted name: MOOSE LAKE. Location found from map.",
        ),
        "BRIDAL LAKE": DirectMatch(
            gnis_ids=["37990"],
            note="FWA has MU 4-8, regulation has MU 4-7 (boundary issue)",
        ),
        "HALL LAKE": DirectMatch(
            gnis_ids=["36134"],
            note="FWA has MU 4-20, regulation has MU 4-34 (boundary issue)",
        ),
        "HIAWATHA LAKE": DirectMatch(
            gnis_ids=["21359"],
            note="FWA has MU 4-4, regulation has MU 4-3 (boundary issue)",
        ),
        "ROCK ISLAND LAKE": DirectMatch(
            gnis_ids=["26621"],
            note="Named 'Rock Isle Lake' in FWA; has both polygon and KML point with same name/MU",
        ),
        "CONNOR LAKE": DirectMatch(
            gnis_ids=["19304"],
            note="3 polygons with GNIS 19304 - Connor Lakes (plural in FWA)",
        ),
        "CONNOR LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["19304"],
            note="Tributaries of Connor Lakes - links to parent waterbody (3 polygons with GNIS 19304)",
        ),
        "COLUMBIA LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["18123"],
            note="Tributaries of Columbia Lake - links to parent waterbody (GNIS 18123 - Columbia Lake). Regulation MU 4-25.",
        ),
        "DUNCAN LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["8109"],
            note="Tributaries of Duncan Lake - links to parent waterbody (GNIS 8109 - Duncan Lake). Regulation MU 4-27.",
        ),
        "ELK RIVER'S TRIBUTARIES (see exceptions)": DirectMatch(
            gnis_ids=["16880"],
            # fwa_watershed_codes=["300-625474-584724-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"],
            note="Tributaries of Elk River - links to parent waterbody (GNIS 16880 - Elk River). Regulation MUs 4-2, 4-23.",
        ),
        "FLATHEAD RIVER'S TRIBUTARIES": DirectMatch(
            gnis_ids=["19843"],
            # fwa_watershed_codes=["300-602565-854327-993941-902282-132363-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"],
            note="Tributaries of Flathead River - links to parent waterbody (GNIS 19843 - Flathead River). Regulation MU 4-1.",
        ),
        "KOOTENAY LAKE, ALL PARTS (Main Body, Upper West Arm and Lower West Arm)": DirectMatch(
            gnis_ids=["14091"],
            note="Kootenay Lake - Main Body, Upper West Arm and Lower West Arm. GNIS 14091 - Kootenay Lake. Regulation MU 4-19.",
        ),
        # TODO: Kootenay Lake subdivisions — these currently link to the full lake polygon.
        # Need to create custom polygon subdivisions based on the regulation map on page 34
        # to separate Main Body, Upper West Arm, and Lower West Arm.
        "KOOTENAY LAKE - MAIN BODY (for location see map on page 34)": DirectMatch(
            gnis_ids=["14091"],
            note="TODO: Currently links to full Kootenay Lake (GNIS 14091). Needs custom polygon subdivision to isolate Main Body (excluding West Arm zones). MU 4-4. See regulation map page 34.",
        ),
        "KOOTENAY LAKE - UPPER WEST ARM (for location see map on page 34)": DirectMatch(
            gnis_ids=["14091"],
            note="TODO: Currently links to full Kootenay Lake (GNIS 14091). Needs custom polygon subdivision to isolate Upper West Arm. MU 4-4. See regulation map page 34.",
        ),
        "KOOTENAY LAKE - LOWER WEST ARM (for location see map on page 34)": DirectMatch(
            gnis_ids=["14091"],
            note="TODO: Currently links to full Kootenay Lake (GNIS 14091). Needs custom polygon subdivision to isolate Lower West Arm. MU 4-4. See regulation map page 34.",
        ),
        "KOOTENAY LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["14091"],
            note="Tributaries of Kootenay Lake - links to parent waterbody (GNIS 14091 - Kootenay Lake). Regulation MUs 4-7, 4-19.",
        ),
        "KINBASKET (McNaughton) LAKE": DirectMatch(
            gnis_ids=["3133"],
            note="Kinbasket Lake (alternate name: McNaughton Lake). GNIS 3133 - 2 polygons spanning MUs 4-34, 4-36, 4-37, 4-38, 4-39, 4-40, 7-2. Regulation MU 4-36.",
        ),
        "KINBASKET (McNaughton) LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["3133"],
            note="Tributaries of Kinbasket Lake - links to parent waterbody (GNIS 3133 - Kinbasket Lake). Regulation MU 4-36.",
        ),
        "LOWER ARROW LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["18644"],
            note="Tributaries of Lower Arrow Lake - links to parent waterbody (GNIS 18644 - Lower Arrow Lake). Regulation MU 4-14.",
        ),
        "UPPER ARROW LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["8405"],
            note="Tributaries of Upper Arrow Lake - links to parent waterbody (GNIS 8405 - Upper Arrow Lake).",
        ),
        "LAKE REVELSTOKE": DirectMatch(
            gnis_ids=["39145"],
            note="GNIS 39145 - Revelstoke Lake. Regulation uses 'Lake Revelstoke' name order.",
        ),
        "REVELSTOKE LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["39145"],
            note="Tributaries of Revelstoke Lake - links to parent waterbody (GNIS 39145 - Revelstoke Lake). Regulation MU 4-38.",
        ),
        "PREMIER LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["25274"],
            note="Tributaries of Premier Lake - links to parent waterbody (GNIS 25274 - Premier Lake). Regulation MU 4-21.",
        ),
        "SLOCAN LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["27954"],
            note="Tributaries of Slocan Lake - links to parent waterbody (GNIS 27954 - Slocan Lake). Regulation MU 4-17.",
        ),
        "TROUT LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["28481"],
            note="Tributaries of Trout Lake - links to parent waterbody (GNIS 28481 - Trout Lake). Regulation MU 4-30.",
        ),
        "CHAMPION LAKE NO. 3": DirectMatch(
            waterbody_keys=["329262654"],
            note="Lake in MU 4-8. BC Lakes Database: WBID 00352LARL, ACAT 4926. Location found from map.",
        ),
        "IDLEWILD LAKE (old Cranbrook Reservoir)": DirectMatch(
            waterbody_keys=["329524502"],
            note="Lake in MU 4-3. BC Lakes Database: WBID 01249SMAR, ACAT 52677. Alternate name: old Cranbrook Reservoir. Location found from map.",
        ),
        '"MCCLAIN" LAKE': DirectMatch(
            waterbody_keys=["329220417"],
            note="Lake in MU 4-34, approximately 750 m south of Mitten Lake. BC Lakes Database: WBID 00799KHOR, ACAT 22239. Spelling variations: McClain/McLain/McLean. Location found from map.",
        ),
        "SALMO RIVER'S TRIBUTARIES": DirectMatch(
            gnis_ids=["20528"],
            note="Tributaries of Salmo River - links to parent waterbody (GNIS 20528 - Salmo River). Regulation MU 4-8.",
        ),
        "SLEWISKIN (Macdonald) CREEK": DirectMatch(
            gnis_ids=["6159"],
            note="FWA name is McDonald Creek (GNIS 6159). Regulation uses 'SLEWISKIN (Macdonald) CREEK' with alternate name 'Macdonald'. Note spelling variation: Macdonald vs McDonald. Left tributary. MU 4-15.",
        ),
        "ECHOES LAKE (near Kimberley)": DirectMatch(
            gnis_ids=["7369"],
            note="2 polygons with GNIS 7369 - Echoes Lakes (plural in FWA)",
        ),
        "MOYIE LAKE": DirectMatch(
            gnis_ids=["15779"],
            note="2 polygons with GNIS 15779 - Moyie Lake spans MUs 4-4 and 4-5",
        ),
        "MOSES CREEK": DirectMatch(
            fwa_watershed_codes=[
                "300-751058-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Moses Creek in MU 4-39. No GNIS name in FWA. Reference: Fish Collection Permit CB12-80431 Moses Creek Hydro Project Fisheries Impact Assessment (ACAT Report ID 37080) - proposed hydroelectric project utilizing flows of Moses and Beattie Creeks near Revelstoke.",
        ),
        'LEWIS ("Cameron") SLOUGH': DirectMatch(
            waterbody_keys=["329524002"],
            note="Found in EAUBC Lakes dataset",
        ),
        '"LOST" LAKE': DirectMatch(
            waterbody_keys=["329123527"],
            note="Near Elkford; found on AllTrails: https://www.alltrails.com/poi/canada/british-columbia/elkford/lost-lake; Using specific waterbody_key (GNIS 18009 is a different Lost Lake)",
        ),
        "TWIN LAKES": DirectMatch(
            waterbody_keys=["329524100", "328989162"],
            note="2 polygons near Premier Lake (accessed via 15min hike from overflow camping area); first lake encountered is Twin Lakes (Yankee); FWA MU 4-21, regulation MU 4-34 (boundary issue); Found via stocked lakes map",
        ),
        '"SPRING" LAKE': DirectMatch(
            waterbody_keys=["329069329"],
            note="Unnamed lake approximately 1.5 km west/northwest of the west end of Tie Lake in MU 4-22. Medium confidence.",
        ),
        '"LITTLE MITTEN" LAKE (approx. 400 m west of Mitten Lake)': DirectMatch(
            waterbody_keys=["329220422"],
            note="Unnamed lake approximately 400m west of Mitten Lake in MU 4-34; found via BC Lakes Database ('Kootenay Fisheries Field Report Little Mitten Lake 00753KHOR', located approx. 13 km SW of Parsons). FWA has no GNIS ID.",
        ),
        "FISHER MAIDEN LAKE": DirectMatch(
            waterbody_keys=["329524272"],
            note="Found via fish stocking records and ACAT reports (https://a100.gov.bc.ca/pub/acat/public/viewReport.do?reportId=51502). FWA has MU 4-17, regulation has MU 4-26 (boundary issue).",
        ),
        "CHAMPION LAKES NO. 1 & 2": DirectMatch(
            waterbody_keys=["328974978", "329262641"],
            note="MU 4-8. BC Lakes Database: WBID 00339LARL (Champion Lake #1 Lower, survey_id 2030) and WBID 00346LARL (Champion Lake #2 Middle, survey_id). ACAT ObjectID 4923. Gazetted name: CHAMPION LAKES. Map reference: https://nrs.objectstore.gov.bc.ca/kuwyyf/champion_lakes_map_230d86f8ad.pdf. Location found from map.",
        ),
        "SAM'S FOLLY LAKE": DirectMatch(
            waterbody_keys=["328966176"],
            note="Lake in MU 4-34. BC Lakes Database: WBID 00398COLR, ACAT 2716. Location found from map.",
        ),
        "WIGWAM RIVER (downstream of the access road adjacent to km 42 on the Bighorn (Ram) Forest Service Road)": DirectMatch(
            gnis_ids=["27703"],
            note="Wigwam River in MU 4-2. Regulation specifies specific reach: 'downstream of the access road adjacent to km 42 on the Bighorn (Ram) Forest Service Road'. Links to entire river (GNIS 2311 - Wigwam River) as specific reach boundaries not in FWA data. NOTE: Divide is approximately at Linear Feature ID 706869683 for future reference.",
        ),
        "WIGWAM RIVER (upstream of the Forest Service recreation site adjacent to km 42 on the Bighorn (Ram) Forest Service Road)": DirectMatch(
            gnis_ids=["27703"],
            note="Wigwam River in MU 4-2. Regulation specifies specific reach: 'upstream of the Forest Service recreation site adjacent to km 42 on the Bighorn (Ram) Forest Service Road'. Links to entire river (GNIS 2311 - Wigwam River) as specific reach boundaries not in FWA data. NOTE: Divide is approximately at Linear Feature ID 706869683 for future reference.",
        ),
        # --- Converted from NameVariation ---
        "CARIBOU LAKES": DirectMatch(
            gnis_ids=["37054", "37055"],
            note="GNIS names: North Caribou Lake (37054) and South Caribou Lake (37055). Split into two lakes in FWA.",
        ),
        "ARROW PARK (Mosquito) CREEK": DirectMatch(
            gnis_ids=["14970"],
            note="GNIS name: Mosquito Creek. MU 4-18 near Arrow Park community. GNIS 7335 (also Mosquito Creek) is in MU 4-32.",
        ),
        "EDWARDS LAKE": DirectMatch(
            gnis_ids=["10669"],
            note="GNIS name: Edwards Lakes (plural). Plural variation.",
        ),
        "QUINN CREEK": DirectMatch(
            gnis_ids=["1961"],
            note="GNIS name: Quinn (Queen) Creek. Full name includes parenthetical.",
        ),
        "PEND D'OREILLE RIVER (Includes the reservoirs behind Waneta Dam and Seven Mile Dam)": DirectMatch(
            gnis_ids=["4927"],
            note="GNIS name: Pend-D'Oreille River. Hyphenation differs in gazetteer.",
        ),
        "GARBUTT LAKE": DirectMatch(
            gnis_ids=["23378"],
            note="GNIS name: Norbury Lake. Official name is Norbury (Garbutt) Lake.",
        ),
        "KOOCANUSA RESERVOIR": DirectMatch(
            gnis_ids=["14083"],
            note="GNIS name: Lake Koocanusa. Name variation.",
        ),
        "BURTON CREEK": DirectMatch(
            gnis_ids=["37939"],
            note="GNIS name: Burton (Trout) Creek. Full name includes parenthetical.",
        ),
        "LAKE REVELSTOKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["39145"],
            note="GNIS name: Revelstoke Lake. Tributary entry - links to parent waterbody.",
        ),
        "LITTLE SLOCAN LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["30277", "18652"],
            note="GNIS names: Upper Little Slocan Lake (30277) and Lower Little Slocan Lake (18652). Tributary entry - links to both parent waterbodies.",
        ),
        "PEND D'OREILLE RIVER'S TRIBUTARIES (except Salmo River[Includes Tributaries])": DirectMatch(
            gnis_ids=["4927"],
            note="GNIS name: Pend-D'Oreille River. Tributary entry - links to parent waterbody (hyphenated form).",
        ),
        # Kootenay River and all named branches/channels on the same watershed
        "KOOTENAY RIVER (downstream of Idaho border)": DirectMatch(
            gnis_ids=[
                "14097",  # Kootenay River
                "39068",  # East Branch Kootenay River
                "2123",  # Old Kootenay River Channel
            ],
            note="Kootenay River and all named branches/channels sharing the same watershed code in Zone 4.",
        ),
        # Columbia River and all named channels on the same watershed
        "COLUMBIA RIVER": DirectMatch(
            gnis_ids=[
                "37414",  # Columbia River
                "17696",  # Back Channel
                "7958",  # Baldy Channel
                "8203",  # Hotsprings Channel
            ],
            note="Columbia River and all named channels sharing the same watershed code in Zone 4.",
        ),
    },
    "Region 5": {
        "ATNARKO/BELLA COOLA RIVERS [Includes Tributaries] EXCEPT: Burnt Bridge Creek upstream of Sitkatapa Creek, Hunlen Creek upstream of Hunlen Falls, and Young Creek upstream of Hwy 20 (see separate entries for these three waters)": DirectMatch(
            gnis_ids=["11611", "17209"],
            note="Links to both Bella Coola River (GNIS 11611) and Atnarko River (GNIS 17209). Bella Coola River flows from the coast (ocean) upstream to confluence of Atnarko and Talchako Rivers. Regulation MUs 5-6, 5-8, 5-11. IMPORTANT - Exceptions handling: Three tributary streams have separate regulation entries with different rules: (1) Burnt Bridge Creek upstream of Sitkatapa Creek, (2) Hunlen Creek upstream of Hunlen Falls, and (3) Young Creek upstream of Hwy 20. These exceptions will be handled as separate waterbody entries in the regulations with their own specific restrictions. When matching, the main Atnarko/Bella Coola rivers entry will link to the entire river systems, and the exception entries will link to specific upstream portions of the tributaries. Users querying these locations will see both the main river regulations AND the specific exception regulations if they apply.",
        ),
        "BABY CHARLOTTE LAKE": DirectMatch(
            waterbody_keys=["329021828"],
            note="Lake in MU 5-6. Location found from map.",
        ),
        '"CRUISE" LAKE': DirectMatch(
            waterbody_keys=["329021826"],
            note="Unnamed lake in MU 5-6, approximately 500 m south of Stewart Lake. BC Lakes Database: WBID 01304ATNA, ACAT 32593. Location found from map.",
        ),
        '"GEESE" LAKE (2 km northeast of Eliguk Lake)': DirectMatch(
            waterbody_keys=["329126819"],
            note="Lake in MU 5-12, 2 km northeast of Eliguk Lake. Medium confidence. Location found from map.",
        ),
        '"KESTREL" LAKE': DirectMatch(
            waterbody_keys=["329586239"],
            note="Lake in MU 5-2. BC Lakes Database: WBID 00238TWAC. Gazetted name: KESTREL LAKE. Location found from map.",
        ),
        "CHILKO LAKE": DirectMatch(
            gnis_ids=["33890"],
            note="GNIS 33890 - Tŝilhqox Biny (current name) / Chilko Lake (former name). FWA has MU 5-4, regulation has MU 5-4 (correct MU).",
        ),
        "ALEXIS LAKE": DirectMatch(
            gnis_ids=["9356"],
            note="GNIS 9356 - Tigulhdzin (current name) / Alexis Lake (former name). Name was officially changed.",
        ),
        "STUM LAKE": DirectMatch(
            gnis_ids=["16247"],
            note="GNIS 16247 - Tegunlin (current name) / Stum Lake (former name). Name was officially changed.",
        ),
        "CHILKO LAKE'S tributary streams": DirectMatch(
            gnis_ids=["33890"],
            note="Tributaries of Chilko Lake - links to parent waterbody (GNIS 33890 - Tŝilhqox Biny / Chilko Lake). FWA has MU 5-4, regulation has MU 5-4.",
        ),
        "TANYA LAKE'S TRIBUTARIES": DirectMatch(
            gnis_ids=["23919"],
            note="Tributaries of Tanya Lakes - links to parent waterbody (GNIS 23919 - Tanya Lakes). FWA has MU 5-10, regulation has MU 5-10.",
        ),
        "BIG LAKE (approx. 30 km west of Likely)": DirectMatch(
            gnis_ids=["13037"],
            note="Disambiguate using GNIS ID",
        ),
        "BIG LAKE (approx. 10 km west of 100 Mile House)": DirectMatch(
            gnis_ids=["33210"],
            note="Disambiguate using GNIS ID",
        ),
        "BLUE LAKE (near Alexandria)": DirectMatch(
            gnis_ids=["14474"],
            note="Disambiguate using GNIS ID",
        ),
        "BLUE LAKE (Soda Creek area)": DirectMatch(
            gnis_ids=["38199"],
            note="Disambiguate using GNIS ID",
        ),
        "BRIDGE LAKE": DirectMatch(
            gnis_ids=["33293"],
            note="FWA has MU 5-1, regulation has MU 5-2 (boundary issue)",
        ),
        "NIMPO LAKE": DirectMatch(
            gnis_ids=["21146"],
            note="FWA has MU 5-6, regulation has MU 5-12 (boundary issue)",
        ),
        "KATHERINE LAKE": DirectMatch(
            gnis_ids=["11358"],
            note="FWA has MU 5-2, regulation has MU 5-15 (boundary issue - lake is close to border)",
        ),
        "JACK OF CLUBS LAKE": DirectMatch(
            gnis_ids=["16935"],
            note="FWA has MU 5-15, regulation has MU 5-2 (boundary issue)",
        ),
        "HUSH LAKE": DirectMatch(
            gnis_ids=["22322"],
            note="FWA has MU 5-2, regulation has MU 5-15 (boundary issue - lake is on the border)",
        ),
        '"SANDY" LAKE': DirectMatch(
            waterbody_keys=["329481525"],
            note="Unnamed lake approximately 3.2 km south of Le Bourdais Lake in MU 5-2. Confirmed via FDIS fish observation (Waterbody ID 48340, Project: Inventory, Rudy, Maud Creek, Sandy Lake; 2019). Reference: http://a100.gov.bc.ca/pub/acat/public/viewReport.do?reportId=58961. Note: There is a different Sandy Lake (GNIS 21196) in MU 5-16 which is NOT this one.",
        ),
        '"BLUFF" LAKE': DirectMatch(
            waterbody_keys=["329354763"],
            note="Unnamed lake approximately 25 km NE of Lac La Hache in Region 5; found via BC Lakes Database (survey_id 20677: 'A Reconnaissance Survey of Bluff Lake'). FWA has no GNIS ID.",
        ),
        "FISH LAKE (unnamed lake approx. 2 km northwest of McClinchy Lake)": DirectMatch(
            waterbody_keys=["329238149"],
            note="Unnamed lake in Region 5; found via BC Lakes Database (survey_id 315: 'UNTITLED REPORT: WINTER LIMNOLOGY DATA FOR FISH LAKE'). FWA has no GNIS ID.",
        ),
        '"SLIM" LAKE': DirectMatch(
            waterbody_keys=["329554444"],
            note="Unnamed lake in Taseko River drainage approximately 4 km north of Cone Hill in MU 5-4; found via BC Lakes Database (survey_id 5210: 'Lake Survey: Slim Lake 00811TASR', associated with Taseko Mines Limited Fish Lake Project). FWA has no GNIS ID.",
        ),
        '"WHALE" LAKE (Gustafsen Lake area)': DirectMatch(
            waterbody_keys=["329116791"],
            note="Whale Lake in Gustafsen Lake area; found via BC Lakes Database (survey_id from 1981: 'A Reconnaissance Survey of Whale Lake 00389DOGC, 1981'). FWA has MU 5-15, regulation has MU 5-2 (boundary issue).",
        ),
        '"RYE" LAKE': DirectMatch(
            waterbody_keys=["329480817"],
            note="Unnamed lake approximately 1.6 km downstream of Joan Lake in MU 5-2; found via ACAT report (https://a100.gov.bc.ca/pub/acat/public/viewReport.do?reportId=23024). FWA has no GNIS ID.",
        ),
        '"PIGEON LAKE #1"': DirectMatch(
            waterbody_keys=["329116844"],
            note="Unnamed lake adjacent to Dog Creek Road, approximately 9 km west of Gustafsen Lake and 19 km north of Meadow Lake Road in MU 5-2; found via BC Lakes Database ('A Reconnaissance Survey of Pigeon #1 (alias) Lake, WBID: 00525DOGC'). FWA has no GNIS ID.",
        ),
        '"SINKHOLE" LAKE': DirectMatch(
            waterbody_keys=["329495189"],
            note="Unnamed lake approximately 100 m east of Sneezie Lake in MU 5-2; found via BC Lakes Database ('A Reconnaissance Survey of Unnamed Lake 5964', WSC 100-385000-98600-98900-5160-5530, located approx. 19.5 km east of Lac La Hache). FWA has no GNIS ID.",
        ),
        "SARDINE LAKE": DirectMatch(
            waterbody_keys=["329480905"],
            note="Found via BC Lakes Database ('Fish Tissue Sample for Sardine lake', WBID: 00444QUES, December 1992). Regulation MU 5-2.",
        ),
        "MAYDOE LAKE": DirectMatch(
            waterbody_keys=["329021823", "329021799"],
            note="Known as Cowboy (Maydoe) Lakes in FWA; 2 polygons found via ACAT report ('Reconnaissance Survey of Cowboy (Maydoe) Lakes - 1997', WBIDs: 01344ATNA and 01372ATNA). Regulation MU 5-6.",
        ),
        'SUNSHINE ("Ant") LAKE': DirectMatch(
            gnis_ids=["10522"],
            note="GNIS 10522 - Ant Lake. Regulation uses alternate name 'Sunshine' with 'Ant' in parentheses. Regulation MU 5-11.",
        ),
        "WENTWORTH LAKES": DirectMatch(
            waterbody_keys=["329430971", "329430952"],
            note="2 polygons found via BC Lakes Database ('A Reconnaissance Survey of Unnamed Lake (Upper Wentworth)', WBID: 00628NAZR and 'A Reconnaissance Survey of Wentworth Lake'). Regulation MU 5-13.",
        ),
        '"SNAG" LAKE': DirectMatch(
            waterbody_keys=["329060886"],
            note="Unnamed lake approximately 60 km ESE of 100 Mile House (West King Area) in MU 5-1; found via BC Lakes Database (survey_id 20655: 'A Reconnaissance Survey of Snag Lake'). FWA has no GNIS ID.",
        ),
        "BEAVER CREEK chain of lakes": DirectMatch(
            gnis_ids=["11119"],
            note="Beaver Creek chain of lakes in MU 5-2. FWA has name 'Beaver Creek' (GNIS 11119).",
        ),
        '"DOG" LAKE': DirectMatch(
            waterbody_keys=["329116771"],
            note="Unnamed lake in MU 5-2, approximately 6 km south/southwest of the confluence of Dog and Pigeon Creeks. BC Lakes Database: WBID 00289DOGC. ACAT 6700. Other Dog Lakes exist in Region 4 (GNIS 24393, MU 4-25) and Region 6 (GNIS 24396, MU 6-16). Location found from map. Medium confidence.",
        ),
        '"GRIZZLY" LAKE (unnamed lake approx. 4.5 km upstream of Maeford Lake)': DirectMatch(
            waterbody_keys=["329074474"],
            note="Lake in MU 5-15. BC Lakes Database: WBID 00514CARR, watershed code 160-466100-28200-84200. ACAT 54245. Location found from map. Low confidence.",
        ),
        "PADDY LAKE": DirectMatch(
            waterbody_keys=["329354753"],
            note="Lake in MU 5-1, also known as Squirrel Lake. Paddy Lake Recreation Site located here (REC5960). Referred to as 'Paddy Squirrel Lake' in BC Lakes bathymetric maps. Another Paddy Lake (GNIS 21895) exists in Region 6 MU 6-26. Another Squirrel Lake listed in Region 6 MU 6-1. Location found from map.",
        ),
        '"GRASSY" LAKE (unnamed lake approx. 1 km southwest of West King Lake)': DirectMatch(
            waterbody_keys=["329061097"],
            note="Unnamed lake in MU 5-1, approximately 1 km southwest of West King Lake. BC Lakes Database: WBID 00707BRID, watershed code 129-360400-23900-98400-9950-9850-283. ACAT 54219. Other Grassy Lakes exist in Region 4 (GNIS 19444, MU 4-16) and Region 8 (GNIS 19443, MU 8-25). Location found from map.",
        ),
        '"HIGH" LAKE (unnamed lake approx. 4 km north of Bridge Lake)': DirectMatch(
            waterbody_keys=["329060853"],
            note="Unnamed lake in MU 5-1, approximately 4 km north of Bridge Lake. BC Lakes Database: WBID 00697BRID. ACAT 24310. Other High Lakes exist in Region 4 (GNIS 21399, MU 4-35) and Region 8 (GNIS 21400, MU 8-22). Location found from map.",
        ),
        '"LITTLE BISHOP" LAKE (approx. 1.7 km northeast of Bishop Lake)': DirectMatch(
            waterbody_keys=["329430970"],
            note="Lake in MU 5-13, approximately 1.7 km northeast of Bishop Lake. Location found from map.",
        ),
        '"LITTLE JONES" LAKE': DirectMatch(
            waterbody_keys=["328987837"],
            note="Unnamed lake in MU 5-2, approximately 13 km east/southeast of 150 Mile House on the north side of Jones Creek. Confirmed via stocked lakes map. Location found from map.",
        ),
        "MERIDIAN LAKE": DirectMatch(
            waterbody_keys=["329354797"],
            note="Unnamed lake in MU 5-1, in Jim Creek system (North Thompson River watershed), approximately 55 km east of 100 Mile House. BC Lakes Database: ACAT 54214. FWA watershed code: 129-360400-23900-98400-4800. Location found from map.",
        ),
        'WEST ROAD ("Blackwater") RIVER': DirectMatch(
            fwa_watershed_codes=[
                "100-500560-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="West Road River (Blackwater River). Regulation MUs 5-12, 5-13.",
        ),
        # --- Converted from NameVariation ---
        "BALLON LAKE": DirectMatch(
            gnis_ids=["18257"],
            note="GNIS name: Baillon Lake. Spelling correction.",
        ),
        '"AGNUS" LAKE': DirectMatch(
            gnis_ids=["44057"],
            note="GNIS name: Agnus Lake. Remove quotes.",
        ),
        "WHALE LAKE (Canim Lake area)": DirectMatch(
            gnis_ids=["35686"],
            note="GNIS name: Whale Lake. Parenthetical area qualifier in regulation name.",
        ),
        "FRASER RIVER": DirectMatch(
            gnis_ids=["39325"],
            note="Fraser River in Region 5. Channels and sloughs are in Zone 2 only.",
        ),
    },
    "Region 6": {
        "UNNAMED LAKE (approx. 500 m south of Natalkuz Lake)": DirectMatch(
            waterbody_keys=["329318485"],
            note="Unnamed lake in MU 6-1, approximately 500 m south of Natalkuz Lake. Location found from map.",
        ),
        "GLACIER (Redslide) CREEK (unnamed tributary to Nanika River)": DirectMatch(
            fwa_watershed_codes=[
                "400-431358-585806-708951-288577-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Medium confidence. Historic Alcan water diversion boundary (1950): diverted Nanika watershed waters upstream of Glacier Creek, ~4km below Kidprice Lake",
        ),
        "LAKELSE RIVER": DirectMatch(
            fwa_watershed_codes=[
                "400-174068-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="FWA has MU 6-11, regulation has MU 6-10 (boundary issue)",
        ),
        "WAHLA LAKE": DirectMatch(
            gnis_ids=["22471"],
            note="FWA has MU 6-1, regulation has MU 6-2 (boundary issue - neighboring MUs but lake is 7km from boundary)",
        ),
        "DUNALTER LAKE (Irrigation Lake)": DirectMatch(
            gnis_ids=["2687"],
            note="FWA has MU 6-8, regulation has MU 6-9 (boundary issue). Alternate name 'Irrigation Lake' confirmed by Irrigation Lake Park near the lake.",
        ),
        "OWEEGEE LAKE": DirectMatch(
            gnis_ids=["21851"],
            note="FWA has MU 6-17, regulation has MU 6-16 (boundary issue - lake is less than 1.5km from border)",
        ),
        "LOST LAKE": DirectMatch(
            waterbody_keys=["329242402"],
            note="FWA has MU 6-21, regulation has MU 6-15 (boundary issue). Lake near Terrace; incorrect GNIS_ID 39158 candidate found in MU 6-21. Using specific waterbody_key for correct lake. Location: https://www.google.com/maps/place/Lost+Lake/@54.6002328,-128.6552761,4390m | Reference: https://www.cbc.ca/news/canada/british-columbia/goldfish-invasion-closes-b-c-fishing-lake-1.5184045",
        ),
        "TOMS LAKE": DirectMatch(
            waterbody_keys=["329126712"],
            note="Unnamed lake in Region 6 MU 6-1; found via BC Lakes Database (survey_id 73: 'UNTITLED REPORT: LIMNOLOGY DATA FOR TOMS LAKE'). FWA has no GNIS ID.",
        ),
        "CHIPMUNK LAKE": DirectMatch(
            waterbody_keys=["329126718"],
            note="Unnamed lake in Region 6; found via BC Lakes Database (survey_id 65: 'UNTITLED REPORT: LIMNOLOGY DATA FOR CHIPMUNK LAKE'). FWA has no GNIS ID.",
        ),
        "TAGISH LAKE": DirectMatch(
            gnis_ids=["23158"],
            note="2 polygons with GNIS 23158 - Tagish Lake",
        ),
        "RANCHERIA RIVER'S TRIBUTARIES": DirectMatch(
            fwa_watershed_codes=[
                "200-692231-770914-177748-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Tributaries of Little Rancheria River - links to parent waterbody. FWA has MU 6-24, regulation has MU 6-25 (boundary issue).",
        ),
        '"DIANA" CREEK': DirectMatch(
            linear_feature_ids=[
                "244068868",
                "244068735",
                "244068720",
                "244068664",
                "244068622",
                "244068592",
                "244068446",
                "244068441",
            ],
            note="Diana Creek in MU 6-14. Specific stream segments identified by linear feature IDs.",
        ),
        '"EAST GRIBBELL" CREEK': DirectMatch(
            fwa_watershed_codes=[
                "915-679587-924307-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="East Gribbell Creek in MU 6-3, on Ursula Channel. Ungazetted stream. Waterbody ID 00000KHTZ. Cutthroat Trout observed 1994-01-01. Source: FISS Database survey_id 77237 '01-JAN-94 Fisheries Assessment of East Gribbell Creek on Ursula Channel'.",
        ),
        '"OLDFIELD" CREEK': DirectMatch(
            fwa_watershed_codes=[
                "915-755598-329884-406409-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Oldfield Creek in MU 6-14, tributary of Hays Creek. Oldfield Creek Fish Hatchery located here.",
        ),
        '"SEELEY" CREEK (outlet of Seeley Lake)': DirectMatch(
            fwa_watershed_codes=[
                "400-426207-245962-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Seeley Creek (outlet of Seeley Lake) in MU 6-9.",
        ),
        'WEST ROAD ("Blackwater") RIVER': DirectMatch(
            fwa_watershed_codes=[
                "100-500560-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="West Road River (Blackwater River). Regulation MU 6-1.",
        ),
        'WEST ROAD ("Blackwater") RIVER\'S TRIBUTARIES': DirectMatch(
            fwa_watershed_codes=[
                "100-500560-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Tributaries of West Road River - links to parent waterbody. Regulation MU 6-1. NOTE: Different regulations apply to Region 6 vs Region 7 - regulations must be applied separately for each region.",
        ),
        # --- Converted from NameVariation ---
        "SUSTUT LAKES": DirectMatch(
            gnis_ids=["25384"],
            note="GNIS name: Sustut Lake (singular). Plural/singular variation.",
        ),
        "KSI HLGINX RIVER (formerly Ishkheenickh River)": DirectMatch(
            gnis_ids=["4069"],
            note="GNIS name: Ksi Hlginx. GNIS drops 'River' suffix.",
        ),
        "KSI SGASGINIST CREEK (formerly Seaskinnish Creek)": DirectMatch(
            gnis_ids=["4791"],
            note="GNIS name: Ksi Sgasginist. GNIS drops 'Creek' suffix.",
        ),
        "KSI SII AKS RIVER (formerly Tseax River)": DirectMatch(
            gnis_ids=["3828"],
            note="GNIS name: Ksi Sii Aks. GNIS drops 'River' suffix.",
        ),
        "KSI X'ANMAS RIVER (formerly Kwinamass River)": DirectMatch(
            gnis_ids=["3815"],
            note="GNIS name: Ksi X'anmas. GNIS drops 'River' suffix.",
        ),
        'HEVENOR ("McQueen") CREEK': DirectMatch(
            gnis_ids=["37312"],
            note="GNIS name: Hevenor Creek. Primary name without parenthetical.",
        ),
        "MCDONNEL LAKE": DirectMatch(
            gnis_ids=["22882", "30846"],
            note="GNIS name: McDonell Lake. Two lakes with same name in zone 6 (GNIS 22882 and 30846). Spelling correction.",
        ),
        "SKEENA RIVER/KISPIOX RIVER CONFLUENCE": DirectMatch(
            ungazetted_waterbody_id="UNGAZ_SKEENA_KISPIOX_CONFLUENCE_R6",
            note=(
                "Confluence of Skeena River and Kispiox River in Region 6 MU 6-8. "
                "No FWA polygon or stream feature at the exact confluence point. "
                "Coordinates from BC Albers projection."
            ),
        ),
    },
    # Region 7 split into 7A and 7B — all entries placed in 7A initially.
    # Run linking to determine which entries belong in 7B, then move them.
    "Region 7A": {
        "EYE LAKE": DirectMatch(
            waterbody_keys=["329376649"],
            note="Lake in MU 7-26. BC Lakes Database: WBID 00041MIDR, ACAT 3257. Location found from map.",
        ),
        "TSITNIZ LAKE": DirectMatch(
            gnis_ids=["29262"],
            note="FWA has MU 7-8, regulation has MU 7-9 (boundary issue - lake is about 3km from border)",
        ),
        "SQUARE LAKE (located in Crooked River Provincial Park)": DirectMatch(
            waterbody_keys=["329102648"],
            note="FWA has MU 7-12, regulation has MU 7-16 (boundary issue). Crooked River Provincial Park is partly in both 7-24 and 7-16; the lake is in 7-24 which is very close to 7-16.",
        ),
        "STELLAKO RIVER": DirectMatch(
            fwa_watershed_codes=[
                "100-567134-374775-948201-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="FWA has MU 7-13, regulation has MU 7-12 (boundary issue)",
        ),
        "NATION RIVER": DirectMatch(
            fwa_watershed_codes=[
                "200-948755-937012-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="FWA has MUs 7-28, 7-29, regulation has MU 7-30 (boundary issue)",
        ),
        "NAUTLEY RIVER": DirectMatch(
            fwa_watershed_codes=[
                "100-567134-374775-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Nautley River in MU 7-13.",
        ),
        "LA SALLE LAKES": DirectMatch(
            gnis_ids=["2482"],
            note="FWA has MU 7-5, regulation has MU 7-3 (boundary issue). GNIS ID matches both polygons.",
        ),
        "LITTLE LOST LAKE": DirectMatch(
            waterbody_keys=["328993555"],
            note="Unnamed lake in Region 7 MU 7-3; found via BC Lakes Database (survey_id 6447: 'A RECONNAISSANCE SURVEY OF UNNAMED (LITTLE LOST) LAKE'). FWA has no GNIS ID. Note: GNIS 10869 exists for 'Little Lost Lake' but is in Region 1 MU 1-6 (different lake).",
        ),
        '"LITTLE TOMAS" LAKE': DirectMatch(
            waterbody_keys=["329537273"],
            note="Unnamed lake in Region 7 MU 7-25; found via BC Lakes Database (Report ID 3676: 'FORT ST. JAMES LAKE INVENTORY 1996 RECONNAISSANCE SURVEY OF UNNAMED LAKE 73 K113 (Little Tomas)', WBID: 01199STUL).",
        ),
        '"LOWER BEAVERPOND" LAKE (lowermost of the two Beaverpond lakes)': DirectMatch(
            waterbody_keys=["329654564"],
            note="Unnamed lake in Region 7 MU 7-38 (lowermost of the two Beaverpond lakes); found via BC Lakes Database (Report ID 6399: 'A Reconnaissance Survey of Lower Beaver Pond Lake 00849UOMI'). Survey date: Mar 1, 1995.",
        ),
        "EMERALD LAKE": DirectMatch(
            gnis_ids=["31097"],
            note="FWA has MU 7-16, regulation has MU 7-15 (boundary issue). Confirmed correct location via stocked lake maps - lake is in MU 7-16.",
        ),
        "DEM LAKE": DirectMatch(
            gnis_ids=["38071"],
            note="FWA has MU 7-26, regulation has MU 7-25 (boundary issue - lake is approximately 2.5km from border)",
        ),
        "CHUBB LAKE": DirectMatch(
            gnis_ids=["13840"],
            note="FWA has MU 7-8, regulation has MU 7-10 (boundary issue - neighboring MUs but lake not close to border, likely mislabeled MU)",
        ),
        "DINA CREEK": DirectMatch(
            fwa_watershed_codes=[
                "200-948755-936810-110196-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Dina Creek in MU 7-30. Unnamed creek that flows to Dina Lakes. Medium confidence.",
        ),
        "BEAR LAKE (Crooked River Provincial Park)": DirectMatch(
            gnis_ids=["11062"],
            note="FWA has MU 7-24, regulation has MU 7-16 (boundary issue). Crooked River Provincial Park is mostly in 7-24 but extends into 7-16; the lake is in 7-24.",
        ),
        'WEST ROAD ("Blackwater") RIVER\'S TRIBUTARIES': DirectMatch(
            fwa_watershed_codes=[
                "100-500560-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="Tributaries of West Road River - links to parent waterbody. Regulation MU 7-10. NOTE: Different regulations apply to Region 6 vs Region 7 - regulations must be applied separately for each region.",
        ),
        "LYNX LAKE": DirectMatch(
            gnis_ids=["19238"],
            note="FWA has MU 7-10, regulation has MU 7-15 (boundary issue - neighboring MUs but lake is 21km from border, likely mislabeled MU in regulations)",
        ),
        '"MT. MILLIGAN" LAKE': DirectMatch(
            waterbody_keys=["329424582"],
            note="Unnamed lake in Region 7 MU 7-28 (located approximately 7.5 km south/southeast of Mt. Milligan); found via BC Lakes Database (Report ID 4121: 'Mount Milligan Lake 2004 Fish Stocking Assessment 01479NATR', WBID: 01479NATR).",
        ),
        "SHANDY LAKE": DirectMatch(
            waterbody_keys=["329382061"],
            note="Lake in Region 7 MU 7-5; found via BC Lakes Database (survey_id 6484: 'BATHYMETRIC OF SHANDY LAKES', WBID 18411). Survey date: Aug 1, 1974.",
        ),
        "HART LAKE (Fort St. James)": DirectMatch(
            waterbody_keys=["329353216"],
            note="Unnamed lake near Fort St. James; found via BC Lakes Database (survey_id 5255: 'A RECONNAISSANCE SURVEY OF UNNAMED \"HART\" LAKE'). FWA has no GNIS ID, regulation has MU 7-25.",
        ),
        "YELLOWHEAD LAKE": DirectMatch(
            gnis_ids=["30397"],
            note="2 polygons with GNIS 30397 - Yellowhead Lake",
        ),
        "TATLATUI LAKE": DirectMatch(
            gnis_ids=["25404"],
            note="2 polygons with GNIS 25404 - Tatlatui Lake",
        ),
        "TEBBUTT LAKE": DirectMatch(
            waterbody_keys=["329537362"],
            note="Lake in Region 7 MU 7-13; found via BC Lakes Database (Report ID 9703: 'Tebbutt Lake - Reconnaissance Survey 1987 02283STUL', WBID: 02283STUL). Survey date: Jul 1, 1987.",
        ),
        "TACHEEDA LAKES (north and south)": DirectMatch(
            gnis_ids=["3956"],
            note="2 polygons with GNIS 3956 - Tacheeda Lakes (north and south)",
        ),
        'UNNAMED LAKE ("Kinglet Lake") located 100 m west of Butterfly Lake': DirectMatch(
            waterbody_keys=["329343551"],
            note='Unnamed lake locally known as "Kinglet Lake", 100m west of Butterfly Lake in MU 7-15. Identified via BC stocking records. Reference: https://www.env.gov.bc.ca/omineca/esd/faw/stocking/kinglet/kinglet_redstart_lakes2003.pdf',
        ),
        'UNNAMED LAKE ("Redstart Lake") located approx. 200 m southwest of Butterfly Lake': DirectMatch(
            waterbody_keys=["329343983", "329343806"],
            note='Unnamed lake locally known as "Redstart Lake", approximately 200m southwest of Butterfly Lake in MU 7-15. Two polygons identified via BC stocking records. Reference: https://www.env.gov.bc.ca/omineca/esd/faw/stocking/kinglet/kinglet_redstart_lakes2003.pdf',
        ),
        "WITCH LAKE": DirectMatch(
            waterbody_keys=["329424460"],
            note="Lake in Region 7 MU 7-28; FWA name is 'Onjo Lake' (gazette name). Found via BC Lakes Database (Report ID 34828: 'A Reconnaissance Survey of Witch Lake, 1977 01386NATR', WBID: 01386NATR). Survey date: Jul 1, 1977.",
        ),
        "DINA LAKE #1": DirectMatch(
            waterbody_keys=["329465434"],
            note="Found via BC Lakes Database ('Dina Lake #1 Pygmy Whitefish Study', WBID: 00357PARA, located north of Mackenzie). Regulation MU 7-30.",
        ),
        "DINA LAKE #2": DirectMatch(
            waterbody_keys=["329465455"],
            note="Found via BC Lakes Database ('A Fisheries Evaluation of Dina Lake #2', WBID: 00346PARA). Regulation MU 7-30.",
        ),
        # --- Converted from NameVariation ---
        "MORFEE LAKE (south)": DirectMatch(
            gnis_ids=["14906"],
            note="GNIS name: Morfee Lakes (plural). Plural variation.",
        ),
        "HAUTETE LAKE": DirectMatch(
            gnis_ids=["16126"],
            note="GNIS name: Haut\u00eate Lake. Accent correction.",
        ),
        "EAST HAUTETE LAKE": DirectMatch(
            gnis_ids=["37135"],
            note="GNIS name: East Haut\u00eate Lake. Accent correction.",
        ),
        "KLWALI LAKE": DirectMatch(
            gnis_ids=["13370"],
            note="GNIS name: Klawli Lake. Spelling correction.",
        ),
        "THORN CREEK": DirectMatch(
            gnis_ids=["32114"],
            note="GNIS name: Thorne Creek. Spelling correction.",
        ),
        "BOBTAIL (Naltesby) LAKE": DirectMatch(
            gnis_ids=["16498"],
            note="GNIS name: Naltesby Lake. Alternate name in gazetteer.",
        ),
        "FRASER RIVER": DirectMatch(
            gnis_ids=["39325"],
            note="Fraser River in Region 7A. Channels and sloughs are in Zone 2 only.",
        ),
    },
    "Region 7B": {
        "TUPPER RIVER": DirectMatch(
            fwa_watershed_codes=[
                "200-948755-780133-471255-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
            ],
            note="FWA has MU 7-33, regulation has MU 7-20 (boundary issue - regulation refers to outlet weir at Swan Lake which is in 7-33)",
        ),
        "CHUNAMUN LAKE": DirectMatch(
            waterbody_keys=["328995585"],
            note="Lake in Region 7B MU 7-35 (same waterbody as 'CHINAMAN' LAKE - alternate name); found via BC Lakes Database (Report ID 52574: 'Chunamun Lake Gillnet Survey - 1989', WBID: 00552UPCE). Survey date: Oct 1, 1989.",
        ),
        "CAMERON LAKES": DirectMatch(
            gnis_ids=["38712"],
            note="3 polygons with GNIS 38712 - Cameron Lakes",
        ),
        "SUNDANCE LAKE": DirectMatch(
            gnis_ids=["20296"],
            note="2 polygons with GNIS 20296 - Sundance Lakes (plural in FWA)",
        ),
        "RAINBOW LAKES": DirectMatch(
            gnis_ids=["30756"],
            note="2 polygons with GNIS 30756 - Rainbow Lakes",
        ),
        "RADAR LAKE": DirectMatch(
            waterbody_keys=["329328645"],
            note="Lake in Region 7B MU 7-20; found via BC Lakes Database (Report ID 6880: 'Peace Fisheries Field Report: Radar Lake (230-690000-56100-63800-6807, 00680LPCE), 2004', WBID: 00680LPCE). Survey date: Jul 1, 2005.",
        ),
        "SWAN LAKE": DirectMatch(
            gnis_ids=["20911"],
            note="Disambiguate from GNIS 20912; FWA has MU 7-33, regulation has MU 7-20 (boundary issue - lake is about 800m from boundary with MU 7-20)",
        ),
    },
    "Region 8": {
        "FRAZER LAKE": DirectMatch(
            gnis_ids=["12484"],
            note="FWA has MU 8-10, regulation has MU 8-9 (boundary issue - lake is 700m from border)",
        ),
        "ELLISON LAKE": DirectMatch(
            gnis_ids=["16910"],
            note="FWA has MU 8-10, regulation has MU 8-8 (boundary issue - lake likely mislabeled MU)",
        ),
        "FIVE O'CLOCK LAKE (approx. 800 m southeast of Cup Lake)": DirectMatch(
            waterbody_keys=["329216683"],
            note="Lake in Region 8 MU 8-14 (approx. 800 m southeast of Cup Lake); found via BC Lakes Database (Report ID 20691: 'Memo to File - Five O'Clock Lake Fish 00796KETL', WBID: 00796KETL). Survey date: May 1, 1992.",
        ),
        "HALL ROAD (Mission) POND": DirectMatch(
            waterbody_keys=["329460964"],
            ungazetted_waterbody_id="UNGAZ_HALL_ROAD_POND_R8",
            note="Region 8 MU 8-10. Links to both Mission Creek Regional Park Children's Fishing Pond ungazetted waterbody (49.87084°N, 119.42958°W) AND adjacent FWA waterbody 329460964. Both locations provided to ensure comprehensive coverage of the fishing area.",
        ),
        "HEADWATER LAKE #1": DirectMatch(
            waterbody_keys=["329459136"],
            note="Lake in Region 8 MU 8-8; found via BC Lakes Database (survey_id 5824: 'LAKE OVERVIEW DATA - HEADWATER LAKES;LAKE #1', WBID 175465). Survey date: Sep 1, 1972.",
        ),
        "CLIFFORD (Cliff ) LAKE": DirectMatch(
            waterbody_keys=["329520059"],
            note="FWA incorrectly has GNIS_ID 31706 in Region 1 MU 1-14; correct lake is in Region 8 MU 8-5. BC Lakes Database: survey_id 3834 ('CLIFF AND RICK LAKES INVESTIGATION, JULY 24 & 25 1989', WBID 174079), survey date Jul 25, 1989. References: https://www.brmbmaps.com/explore/canada/british-columbia/thompson-nicola/clifford-lake-recreation-site/84451 | https://web.archive.org/web/20200529103953/https://www.sitesandtrailsbc.ca/search/search-result.aspx?site=REC1636&type=Site",
        ),
        "LARRY LAKE (unnamed lake located about 400 m west of Thalia Lake)": DirectMatch(
            waterbody_keys=["329520255"],
            note="Unnamed lake west of Thalia Lake in Region 8 MU 8-5. BC Lakes Database: Report ID 13491 ('Memo to File: Larry Lake Investigation - June 11 & 12 1984 00470SIML', WBID 00470SIML), survey date Apr 1, 1986. Reference: https://www.brmbmaps.com/explore/canada/british-columbia/thompson-nicola/clifford-lake-recreation-site/84451",
        ),
        "TEE PEE LAKES": DirectMatch(
            gnis_ids=["21766"],
            note="3 polygons with GNIS 21766 - Tepee Lakes (spelling variation in FWA)",
        ),
        "MCCULLOCH RESERVOIR": DirectMatch(
            gnis_ids=["15973"],
            note="3 polygons with GNIS 15973 - McCulloch Reservoir",
        ),
        "ARLINGTON LAKES": DirectMatch(
            gnis_ids=["16647"],
            note="6 polygons with GNIS 16647 - Arlington Lakes",
        ),
        "TWIN LAKES": DirectMatch(
            gnis_ids=["3086"],
            note="3 polygons with GNIS 3086 - Twin Lakes span MUs 8-1 and 8-2",
        ),
        "RICKEY LAKE": DirectMatch(
            waterbody_keys=["329520185"],
            note="Lake in Region 8 MU 8-5 (WBID: 00472SIML). Shares BC Lakes Database survey with CLIFFORD (Cliff) LAKE: survey_id 3834 ('CLIFF AND RICK LAKES INVESTIGATION, JULY 24 & 25 1989'), survey date Jul 25, 1989. References: https://adventuregenie.com/rv-campgrounds/british-columbia/merritt/rickey-lake | https://web.archive.org/web/20200529102211/https://www.sitesandtrailsbc.ca/search/search-result.aspx?site=REC1637&type=Site",
        ),
        "ROSE VALLEY RESERVOIR (Lakeview Irrigation District)": DirectMatch(
            waterbody_keys=["329459139"],
            note="Lake in Region 8 MU 8-11 (Lakeview Irrigation District); found via BC Lakes Database (Report ID 43772: 'Survey of Rose Valley (Lake) Reservoir 1977', WBID: 00867OKAN). Survey date: May 1, 1977.",
        ),
        "SWAN LAKE": DirectMatch(
            gnis_ids=["20909"],
            note="FWA has MU 8-22, regulation has MU 8-26 (boundary issue). Confirmed correct lake via stocked lakes map. Other Swan Lake (GNIS 37740) is in MU 8-6.",
        ),
        "GRANBY RIVER'S TRIBUTARIES": DirectMatch(
            gnis_ids=["18775"],
            note="Tributaries of Granby River - links to parent waterbody (GNIS 18775 - Granby River). Regulation MU 8-15.",
        ),
        # TODO: Get the BLK for these unresolved fwa_watershed_codes and the linear_feature_id
        # so they can be added back with proper identifiers.
        "OKANAGAN RIVER OXBOWS": DirectMatch(
            fwa_watershed_codes=[
                "300-432687-461418-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                # "300-432687-461418-400917-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",  # unresolved — need BLK
                "300-432687-463105-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                # "300-432687-463105-427876-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",  # unresolved — need BLK
                "300-432687-459615-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-466472-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                # "300-432687-461418-565942-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",  # unresolved — need BLK
                # "300-432687-466472-328926-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",  # unresolved — need BLK
                "300-432687-469486-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-476281-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                # "300-432687-476281-770576-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",  # unresolved — need BLK
                "300-432687-476812-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-478730-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-480401-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-482026-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-483280-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-485563-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-487483-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-494632-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-500162-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-499499-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-503666-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-505976-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-516449-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-517221-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-518583-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-521786-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-558447-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-555555-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-560023-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-564690-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-565796-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            ],
            waterbody_keys=[
                "328948249",
                "328947490",
                "328944522",
                "328957729",
                "329459219",
                "329459234",
                "329460279",
                "329459157",
                "329462114",
                "329462438",
                "329462220",
                "329461070",
                "329462410",
                "329462406",
                "329460316",
                "329462179",
                "329461320",
                "329461445",
                "329462452",
                "329461584",
                "329462319",
                "329462064",
                "329461918",
                "329462395",
                "329460748",
                "329460457",
                "328985785",
                "329461800",
                "329461866",
                "328985783",
                "328985789",
                "329461388",
                "329460329",
                "329460050",
                "329460670",
                "329462470",
                "329461983",
                "329462424",
                "329462306",
                "329462043",
            ],
            # linear_feature_ids=["707367498"],  # unresolved — need BLK
            note="Okanagan River Oxbows in MU 8-1. Multiple oxbow waterbodies and stream segments along the Okanagan River. Includes 32 watershed codes, 40 waterbody keys, and 1 linear feature ID.",
        ),
        "TREPANIER RIVER": DirectMatch(
            gnis_ids=["27648"],
            note="Trepanier River in MU 8-8. FWA has name 'Trépanier Creek' (GNIS 27648).",
        ),
        "KETTLE RIVER'S TRIBUTARIES": DirectMatch(
            gnis_ids=["11984"],
            note="Tributaries of Kettle River - links to parent waterbody (GNIS 11984 - Kettle River). Regulation MU 8-14.",
        ),
        "WEST KETTLE RIVER'S tributaries": DirectMatch(
            gnis_ids=["6358"],
            note="Tributaries of West Kettle River - links to parent waterbody (GNIS 6358 - West Kettle River). Regulation MU 8-12.",
        ),
        '"BLUEY LAKE POTHOLES"': DirectMatch(
            waterbody_poly_ids=[
                "700171140",
                "705026999",
                "705026685",
                "705028515",
                "705028606",
                "705026434",
                "705026561",
                "705026580",
                "705027285",
                "705026103",
            ],
            note="Bluey Lake Potholes in Region 8 MU 8-6. 10 specific waterbody polygons.",
        ),
        "UNNAMED LAKES (located immediately north and south of Bluey Lake)": DirectMatch(
            waterbody_keys=[
                "329521064",
                "329520774",
                "329522486",
                "329522575",
                "329520563",
                "329520659",
                "329520676",
                "329521324",
                "329520282",
            ],
            note="9 unnamed lakes located immediately north and south of Bluey Lake in Region 8 MU 8-6. All waterbodies matching the location criteria are included.",
        ),
        # NOTE: The following three lakes are part of the Tepee Lakes group (TEE PEE LAKES in regulations).
        # With GNIS_NAME_2 support implemented in metadata_gazetteer, these should now be found automatically
        # via natural name search. Direct matches kept here for reference but commented out.
        # "FRIDAY LAKE": DirectMatch(
        #     waterbody_keys=["329520049"],
        #     note="Part of Tepee Lakes group in Region 8 MU 8-6. TEE PEE LAKES regulation says 'See Friday, Saturday, and Sunday Lakes'. Has GNIS_NAME_2 attribute in FWA.",
        # ),
        # "SATURDAY LAKE": DirectMatch(
        #     waterbody_keys=["329520046"],
        #     note="Part of Tepee Lakes group in Region 8 MU 8-6. TEE PEE LAKES regulation says 'See Friday, Saturday, and Sunday Lakes'. Has GNIS_NAME_2 attribute in FWA. FWA incorrectly shows GNIS_ID 23494 in Region 6 MU 6-8.",
        # ),
        # "SUNDAY LAKE": DirectMatch(
        #     waterbody_keys=["329520017"],
        #     note="Part of Tepee Lakes group in Region 8 MU 8-6. TEE PEE LAKES regulation says 'See Friday, Saturday, and Sunday Lakes'. Has GNIS_NAME_2 attribute in FWA.",
        # ),
        # --- Converted from NameVariation ---
        "TUC-EL-NUIT LAKE": DirectMatch(
            gnis_ids=["3035"],
            note="GNIS name: Tugulnuit Lake. Spelling mismatch.",
        ),
        "BIGHORN RESERVOIR (Lakeview Irrigation District)": DirectMatch(
            gnis_ids=["39570"],
            note="GNIS name: Big Horn Reservoir. Spacing correction.",
        ),
    },
}


# Skip entries - waterbodies that should not be linked
# For alternate/historical names, use NAME_VARIATION_LINKS instead.
# Format: {"Region X": {"WATERBODY NAME": SkipEntry(note="...", not_found=True|ignored=True)}}
SKIP_ENTRIES: Dict[str, Dict[str, SkipEntry]] = {
    "Region 2": {
        "LITTLE STAWAMUS CREEK": SkipEntry(
            note="Known location in MU 2-8 near Squamish but stream does not exist in FWA mapping data. Would require custom stream segment creation. Reference: DFO Stream Summary Catalogue 'Little Stawamus Creek' https://publications.gc.ca/collections/collection_2014/mpo-dfo/Fs97-6-2282-eng.pdf and https://squamish.ca/assets/Uploads/928c09348e/Camping-Bylaw-Map.pdf",
            not_found=True,
        ),
    },
    "Region 4": {
        "ARROW LAKES": SkipEntry(
            note="Regulation refers to Upper/Lower Arrow Lake details",
            ignored=True,
        ),
        "ARROW LAKES' TRIBUTARIES": SkipEntry(
            note="Likely covered by Upper/Lower tributaries",
            ignored=True,
        ),
        "SEVEN MILE RESERVOIR": SkipEntry(
            note="Dammed portion of Pend d'Oreille River - uses same regulations as Pend d'Oreille River. This may change in future. Polygons are in unnamed manmade lakes.",
            ignored=True,
        ),
        "SEVEN MILE RESERVOIR'S TRIBUTARIES": SkipEntry(
            note="Covered by Pend d'Oreille River tributary regulations",
            ignored=True,
        ),
        "WANETA RESERVOIR": SkipEntry(
            note="Dammed portion of Pend d'Oreille River - uses same regulations as Pend d'Oreille River. This may change in future. Polygons are in unnamed manmade lakes.",
            ignored=True,
        ),
        "WANETA RESERVOIR'S TRIBUTARIES": SkipEntry(
            note="Covered by Pend d'Oreille River tributary regulations",
            ignored=True,
        ),
    },
    "Region 5": {
        "TOMS LAKE": SkipEntry(
            note="Cross-listed entry - already covered in Region 6 (MU 6-1)",
            ignored=True,
        ),
        "SQUIRREL LAKE": SkipEntry(
            note="Cross-listed entry - already covered in Region 6 (MU 6-1)",
            ignored=True,
        ),
        "BASALT LAKE": SkipEntry(
            note="Duplicate entry - already covered in Region 6 (MU 6-1, GNIS 18842)",
            ignored=True,
        ),
        "GATCHO LAKE": SkipEntry(
            note="Duplicate entry - already covered in Region 6 (MU 6-1, GNIS 13318)",
            ignored=True,
        ),
        "NAGLICO LAKE": SkipEntry(
            note="Duplicate entry - already covered in Region 6 (MU 6-1, GNIS 16467)",
            ignored=True,
        ),
        "PETTRY LAKE": SkipEntry(
            note="Duplicate entry - already covered in Region 6 (MU 6-1, GNIS 20981)",
            ignored=True,
        ),
        "REDFERN LAKE": SkipEntry(
            note="Searched but only found in Region 7 (MU 7-42, GNIS 38145, waterbody_key 329423854), not Region 5 (regulation MU 5-15). May be typo in regulations or require duplicate feature creation. Reference: BC Freshwater Fishing Regulations Synopsis 2024-2026, Region 5, MU 5-15.",
            not_found=True,
        ),
        "SECRET LAKE": SkipEntry(
            note="Searched but only found in Region 8 (MU 8-7, GNIS 37609, waterbody_key 329220075) and Region 3 (MU 3-30, GNIS 38166, waterbody_key 331154867), not Region 5 (regulation MU 5-6). May be typo in regulations or require duplicate feature creation. Reference: BC Freshwater Fishing Regulations Synopsis 2024-2026, Region 5, MU 5-6.",
            not_found=True,
        ),
        "FROG LAKE": SkipEntry(
            note="Searched but only found in Region 3 (MU 3-29, GNIS 12556, waterbody_key 330954838), Region 1 (MU 1-10, GNIS 12554, waterbody_key 329173098), and Region 7 (MU 7-30, GNIS 56145, waterbody_key 329383393), not Region 5 (regulation MU 5-6). May be typo in regulations or require duplicate feature creation. Reference: BC Freshwater Fishing Regulations Synopsis 2024-2026, Region 5, MU 5-6.",
            not_found=True,
        ),
        "HIDDEN LAKE": SkipEntry(
            note="Searched but only found in Region 5 MU 5-15 (GNIS 21382, waterbody_key 329635892), not in regulation MU 5-6. May be MU boundary issue or typo in regulations. Reference: BC Freshwater Fishing Regulations Synopsis 2024-2026, Region 5, MU 5-6.",
            not_found=True,
        ),
        "SQUARE LAKE": SkipEntry(
            note="Searched but only found in Region 5 MU 5-3 (GNIS 29739, waterbody_key 329677163), not in regulation MU 5-6. May be MU boundary issue or typo in regulations. Reference: BC Freshwater Fishing Regulations Synopsis 2024-2026, Region 5, MU 5-6.",
            not_found=True,
        ),
        "CHIPMUNK LAKE": SkipEntry(
            note="Cross-listed entry - already covered in Region 6 (MU 6-1)",
            ignored=True,
        ),
    },
    "Region 6": {
        "SQUIRREL LAKE": SkipEntry(
            note="Searched but only found in Region 5 (MU 5-1, GNIS 38786, waterbody_key 329642030). Should be close to border with Region 5 MUs 5-10, 5-12, or 5-13 (neighboring MU 6-1). Lake likely exists near regional boundary but not found in FWA data for Region 6. May need duplicate feature or MU boundary correction. Reference: BC Freshwater Fishing Regulations Synopsis 2024-2026, Region 6, MU 6-1.",
            not_found=True,
        ),
    },
    "Region 7A": {
        "ENDAKO RIVER": SkipEntry(
            note="Cross-listed entry - already covered in Region 6 (MUs 6-4, 6-5)",
            ignored=True,
        ),
        # LIARD RIVER WATERSHED - moved to ADMIN_DIRECT_MATCHES (watershed polygon matching)
        "NATION ARM (Williston Lake)": SkipEntry(
            note="Nation Arm of Williston Lake requires custom polygon subdivision. MU 7-58. Williston Lake GNIS 21990, waterbody_key 329393419. Nation River (GNIS 16593) flows into this arm. Requires custom geometry creation by subdividing Williston Lake polygon to define the Nation Arm portion. Reference: BC Freshwater Fishing Regulations Synopsis 2024-2026, Region 7, MU 7-58.",
            not_found=True,
        ),
        "WILLISTON LAKE (in Zone A) (includes waters 500 m east/upstream of the Causeway Road)": SkipEntry(
            note="Williston Lake Zone A requires custom polygon subdivision based on regulation description. MU 7-58. Williston Lake GNIS 21990, waterbody_key 329393419. Zone A includes waters 500m east/upstream of the Causeway Road. Different regulations apply to Zone A vs Zone B. Requires custom geometry creation by subdividing lake polygon based on Causeway Road location. Reference: BC Freshwater Fishing Regulations Synopsis 2024-2026, Region 7, MU 7-58.",
            not_found=True,
        ),
        "DAVIS BAY (in Finlay Reach of Williston Lake)": SkipEntry(
            note="Davis Bay in Finlay Reach of Williston Lake requires custom polygon subdivision. MU 7-58. Williston Lake GNIS 21990, waterbody_key 329393419. Finlay Reach is the northern arm where Finlay River (GNIS 12355) enters Williston Lake. Davis Bay is a specific bay within this reach. Requires custom geometry creation by identifying and subdividing the bay portion. Reference: BC Freshwater Fishing Regulations Synopsis 2024-2026, Region 7, MU 7-58.",
            not_found=True,
        ),
    },
    "Region 7B": {
        "WILLISTON LAKE (in Zone B)": SkipEntry(
            note="Williston Lake Zone B requires custom polygon subdivision based on regulation description. MU 7-58. Williston Lake GNIS 21990, waterbody_key 329393419. Zone B is the remainder of Williston Lake excluding Zone A (500m east/upstream of Causeway Road). Different regulations apply to Zone B vs Zone A. Requires custom geometry creation by subdividing lake polygon based on Causeway Road location. Reference: BC Freshwater Fishing Regulations Synopsis 2024-2026, Region 7, MU 7-58.",
            not_found=True,
        ),
    },
}


# ──────────────────────────────────────────────────────────────────────────────
# NAME_VARIATION_LINKS - Alternate names for already-linked waterbodies
# ──────────────────────────────────────────────────────────────────────────────
# When the same waterbody appears twice in the synopsis under different names,
# only the primary entry should link to FWA features.  The alternate name is
# recorded here so it skips linking but gets passed downstream as a searchable
# alias (name_variant) in the search index and regulation display names.
#
# primary_name must exactly match the name_verbatim of the primary regulation
# entry (which may itself be a DirectMatch or naturally-linked entry).
#
# Format: {"Region X": {"ALTERNATE NAME": NameVariationLink(primary_name="PRIMARY NAME", note="...")}}
NAME_VARIATION_LINKS: Dict[str, Dict[str, NameVariationLink]] = {
    "Region 1": {
        '"LINK" RIVER': NameVariationLink(
            primary_name='MARBLE ("Link") RIVER (only between Victoria and Alice lakes)',
            note="Listed as 'Marble (Link) River' in gazetteer. 'Link River' is an alternate name.",
        ),
        "BEAR RIVER": NameVariationLink(
            primary_name="AMOR DE COSMOS CREEK",
            note="Regulation says 'See Amor de Cosmos Creek'. Bear River is historical/alternate name for Amor de Cosmos Creek. Evidence: https://www.facebook.com/aboriginal.journeys/videos/819108737814861/",
        ),
    },
    "Region 2": {
        "LITTLE CAMPBELL RIVER": NameVariationLink(
            primary_name="CAMPBELL RIVER",
            note="Alternate name for Campbell River in MU 2-4. Regulations already covered under Campbell River entry.",
        ),
    },
    "Region 4": {
        "CAMERON SLOUGH": NameVariationLink(
            primary_name='LEWIS ("Cameron") SLOUGH',
            note='Alternate name for LEWIS ("Cameron") SLOUGH. Same waterbody in regulation MU 4-21.',
        ),
        "MCNAUGHTON LAKE": NameVariationLink(
            primary_name="KINBASKET (McNaughton) LAKE",
            note="Regulation says 'See Kinbasket Lake'. McNaughton Lake is an alternate name for part of Kinbasket Lake.",
        ),
    },
    "Region 5": {
        '"BLACKWATER" RIVER': NameVariationLink(
            primary_name='WEST ROAD ("Blackwater") RIVER',
            note='Alternate name for WEST ROAD ("Blackwater") RIVER. Same waterbody, regulation MU 5-13. Primary entry is in Region 6.',
        ),
        '"BROWN" LAKE': NameVariationLink(
            primary_name='BISHOP ("Brown") LAKE',
            note='Alternate name for BISHOP ("Brown") LAKE. Same waterbody.',
        ),
    },
    "Region 6": {
        "COPPER RIVER": NameVariationLink(
            primary_name="ZYMOETZ (Copper) RIVER",
            note="Alternate name for ZYMOETZ (Copper) RIVER. Same waterbody, regulation MU 6-9.",
        ),
        "ISHKHEENICKH RIVER": NameVariationLink(
            primary_name="KSI HLGINX RIVER (formerly Ishkheenickh River)",
            note="Regulation says 'See Ksi Hlginx River'. River has been renamed to KSI HLGINX (GNIS 4069). Regulation MU 6-14.",
        ),
        "KWINAMASS RIVER": NameVariationLink(
            primary_name="KSI X'ANMAS RIVER (formerly Kwinamass River)",
            note="Regulation says 'See Ksi X'anmas River'. River has been renamed to KSI X'ANMAS (GNIS 3815). Regulation MU 6-14.",
        ),
        "MCQUEEN CREEK": NameVariationLink(
            primary_name='HEVENOR ("McQueen") CREEK',
            note='Alternate name for HEVENOR ("McQueen") CREEK. Same waterbody.',
        ),
        "SEASKINNISH CREEK": NameVariationLink(
            primary_name="KSI SGASGINIST CREEK (formerly Seaskinnish Creek)",
            note="Regulation says 'See Ksi Sgasginist Creek'. Creek has been renamed to KSI SGASGINIST CREEK. Regulation MU 6-15.",
        ),
        "TSEAX RIVER": NameVariationLink(
            primary_name="KSI SII AKS RIVER (formerly Tseax River)",
            note="River has been renamed to KSI SII AKS RIVER. Regulation MU 6-14.",
        ),
    },
    "Region 7A": {
        "BLACKWATER RIVER": NameVariationLink(
            primary_name='WEST ROAD ("Blackwater") RIVER',
            note="Regulation says 'See West Road River'. Alternate name for WEST ROAD ('Blackwater') RIVER. Regulation MU 7-10.",
        ),
    },
    "Region 7B": {
        '"CHINAMAN" LAKE': NameVariationLink(
            primary_name="CHUNAMUN LAKE",
            note="Same waterbody as CHUNAMUN LAKE (waterbody_key 328995585, WBID: 00552UPCE). Both names appear in BC Lakes Database surveys.",
        ),
    },
    "Region 8": {
        "SAWMILL LAKE": NameVariationLink(
            primary_name="BURNELL (Sawmill) LAKE",
            note="Alternative name for Burnell Lake. Same waterbody.",
        ),
    },
}

# ──────────────────────────────────────────────────────────────────────────────
# FEATURE_NAME_VARIATIONS - Display names assigned to features by BLK/WBK
# ──────────────────────────────────────────────────────────────────────────────
# Assigns a display name to a specific FWA feature identified by blue_line_key
# or waterbody_key.  Used for unnamed features like side channels of a river
# that share regulations with the mainstem but need their own display name.
#
# Adding a FeatureNameVariation causes:
#   - The feature to display with the assigned name
#   - The feature to group SEPARATELY from other features on the same BLK/WBK
#     that lack this name variation (e.g., side channel vs mainstem)
#   - A distinct frontend_group_id so clicking the feature highlights only it
#
# Format: {"Region X": [FeatureNameVariation(name="...", blue_line_keys=["..."], note="...")]}
# At least one of blue_line_keys or waterbody_keys must be provided per entry.
FEATURE_NAME_VARIATIONS: Dict[str, List[FeatureNameVariation]] = {
    "Region 3": [
        FeatureNameVariation(
            name="McArthur Island Slough",
            blue_line_keys=["355994157"],
            note="Unnamed slough near McArthur Island in Region 3 MU 3-12.",
        ),
    ],
    "Region 2": [
        FeatureNameVariation(
            name="Jeperson Side Channel",
            blue_line_keys=[
                "355994571",
                "355994568",
                "355994563",
                "355994564",
                "355994562",
                "355994572",
            ],
            note="Unnamed side channel of Fraser River near Mission, Region 2.",
        ),
        FeatureNameVariation(
            name="Herring Island Side Channel",
            blue_line_keys=[
                "355992188",
                "355992191",
                "355992201",
                "355992200",
                "355992187",
                "355992198",
            ],
            note="Unnamed side channel of Fraser River near Herring Island, Region 2.",
        ),
        FeatureNameVariation(
            name="Seabird Island Side Channel",
            blue_line_keys=["355991780", "355991778", "355991779", "355991777"],
            note="Unnamed side channel of Fraser River near Seabird Island, Region 2.",
        ),
    ],
}

# ──────────────────────────────────────────────────────────────────────────────
# ADMIN_DIRECT_MATCHES - Synopsis regulations targeting administrative areas
# ──────────────────────────────────────────────────────────────────────────────
# These map synopsis regulation names to admin boundary polygons (parks, WMAs,
# watersheds, etc.). At pipeline time the polygon is spatially intersected with
# FWA features to produce the list of linked streams/lakes/etc.
#
# Admin layer keys correspond to fetch_data.py DATASETS / GPKG layer names:
#   parks_bc      → Provincial Parks & Ecological Reserves (TA_PARK_ECORES_PA_SVW)
#   parks_nat     → National Parks (CLAB_NATIONAL_PARKS)
#   wma           → Wildlife Management Areas (WLS_WILDLIFE_MGMT_AREA_SVW)
#   watersheds    → Named Watersheds (FWA_NAMED_WATERSHEDS_POLY)
#   historic_sites → Historic Sites (HIST_HERITAGE_WRECK_SVW)
#
# ID Fields (from ADMIN_LAYER_CONFIG in metadata_builder.py):
#   parks_nat      → NATIONAL_PARK_ID
#   parks_bc       → ADMIN_AREA_SID
#   wma            → ADMIN_AREA_SID
#   watersheds     → NAMED_WATERSHED_ID
#   historic_sites → SITE_ID
#
# Format: {"Region X": {"REGULATION NAME": AdminDirectMatch(...)}}
# Each entry uses AdminTarget(layer, feature_id) pairs.
ADMIN_DIRECT_MATCHES: Dict[str, Dict[str, AdminDirectMatch]] = {
    "Region 1": {
        "STRATHCONA PARK WATERS": AdminDirectMatch(
            admin_targets=[
                AdminTarget("parks_bc", "1125"),
                AdminTarget("parks_bc", "1127"),
            ],
            note=(
                "Synopsis lists 'STRATHCONA PARK WATERS' in Region 1. "
                "Applies to all streams and lakes within Strathcona Provincial Park. "
                "Layer: TA_PARK_ECORES_PA_SVW, ID field: ADMIN_AREA_SID."
            ),
        ),
    },
    "Region 4": {
        "CRESTON VALLEY WILDLIFE MANAGEMENT AREA (CVWMA) WATERS": AdminDirectMatch(
            admin_targets=[AdminTarget("wma", "5364")],
            note=(
                "Synopsis lists 'CRESTON VALLEY WILDLIFE MANAGEMENT AREA (CVWMA) WATERS' in Region 4 MU 4-6. "
                "Applies to all streams and lakes within Creston Valley Wildlife Management Area. "
                "Layer: WLS_WILDLIFE_MGMT_AREA_SVW, ID field: ADMIN_AREA_SID. "
                "Permit requirement is now a zone regulation (zone_r4_creston_valley_wma_permit)."
            ),
        ),
        "KIKOMUN CREEK PARK (all lakes in the park)": AdminDirectMatch(
            admin_targets=[AdminTarget("parks_bc", "793")],
            note=(
                "Synopsis lists 'KIKOMUN CREEK PARK (all lakes in the park)' in Region 4 MU 4-22. "
                "Regulations apply specifically to lakes within Kikomun Creek Provincial Park. "
                "Layer: TA_PARK_ECORES_PA_SVW, ID field: ADMIN_AREA_SID."
            ),
            feature_types=[FeatureType.LAKE],
        ),
    },
    "Region 5": {
        "BOWRON LAKE Park waters other than Bowron Lake": AdminDirectMatch(
            admin_targets=[AdminTarget("parks_bc", "519")],
            note=(
                "Synopsis lists 'BOWRON LAKE Park waters other than Bowron Lake' in Region 5 MU 5-16. "
                "Applies to all streams and lakes within Bowron Lake Provincial Park, excluding Bowron Lake itself. "
                "Layer: TA_PARK_ECORES_PA_SVW, ID field: ADMIN_AREA_SID."
            ),
        ),
    },
    "Region 6": {
        "CHILKOOT TRAIL NATIONAL HISTORIC PARK WATERS": AdminDirectMatch(
            admin_targets=[
                AdminTarget("historic_sites", "4af28ce2-bda0-47bf-8e64-664b1be54922")
            ],
            note=(
                "Synopsis lists 'CHILKOOT TRAIL NATIONAL HISTORIC PARK WATERS' in Region 6 MU 6-28. "
                "Applies to all streams and lakes within the Chilkoot Trail National Historic Site. "
                "Layer: HIST_HERITAGE_WRECK_SVW, ID field: SITE_ID."
            ),
        ),
    },
    "Region 7B": {
        "LIARD RIVER WATERSHED (see map on page 63)": AdminDirectMatch(
            admin_targets=[AdminTarget("watersheds", "5")],
            note=(
                "Regulation specifies 'LIARD RIVER WATERSHED (see map on page 63)' in MU 7-53. "
                "FWA NAMED WATERSHED: Named Watershed ID 5, Object ID 6422089. "
                "Layer: FWA_NAMED_WORKSHEDS_POLY, ID field: NAMED_WATERSHED_ID."
            ),
            feature_types=[FeatureType.STREAM, FeatureType.LAKE, FeatureType.MANMADE],
        ),
    },
}


# Ungazetted Waterbodies — custom waterbodies not in the FWA gazetteer
# Injected into the gazetteer at pipeline startup so they participate in
# linking, merging, and geographic export like any other FWA feature.
# All coordinates are in EPSG:3005 (BC Albers).
# Format: {"UNGAZ_ID": UngazettedWaterbody(...)}
UNGAZETTED_WATERBODIES: Dict[str, UngazettedWaterbody] = {
    "UNGAZ_MARSH_POND_R2": UngazettedWaterbody(
        ungazetted_id="UNGAZ_MARSH_POND_R2",
        name="MARSH POND",
        geometry_type="point",
        coordinates=[1259700.296, 450116.823],
        zones=["2"],
        mgmt_units=["2-4"],
        note="No polygon found in FWA lakes, wetlands, or manmade layers. Coordinates from KML point labeling in Aldergrove Regional Park, converted to EPSG:3005.",
        source_url="https://metrovancouver.org/services/regional-parks/park/aldergrove-regional-park",
    ),
    "UNGAZ_HALL_ROAD_POND_R8": UngazettedWaterbody(
        ungazetted_id="UNGAZ_HALL_ROAD_POND_R8",
        name="HALL ROAD (Mission) POND",
        geometry_type="point",
        coordinates=[1471726.918, 561326.078],
        zones=["8"],
        mgmt_units=["8-10"],
        note="Mission Creek Regional Park Children's Fishing Pond. Regulation name is 'HALL ROAD (Mission) POND'. Location coordinates identify the fishing pond; an adjacent FWA waterbody (329460964) also exists in the area. Converted to EPSG:3005.",
        source_url=None,
    ),
    "UNGAZ_SKEENA_KISPIOX_CONFLUENCE_R6": UngazettedWaterbody(
        ungazetted_id="UNGAZ_SKEENA_KISPIOX_CONFLUENCE_R6",
        name="SKEENA RIVER/KISPIOX RIVER CONFLUENCE",
        geometry_type="point",
        coordinates=[892981.484, 1150750.525],
        zones=["6"],
        mgmt_units=["6-8"],
        note=(
            "Confluence of Skeena River and Kispiox River. "
            "Coordinates: X=892981.48371, Y=1150750.52467 in EPSG:3005 (BC Albers)."
        ),
        source_url=None,
    ),
}
