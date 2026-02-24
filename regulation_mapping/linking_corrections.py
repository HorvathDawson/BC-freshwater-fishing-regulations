"""
Manual name variations and direct matches for waterbody linking.

This module contains three types of manual corrections:

1. NAME_VARIATIONS: Search hints for fuzzy matching
   - Spelling mismatches between synopsis and gazetteer
   - Name order differences (e.g., "Maxwell Lake" -> "Lake Maxwell")
   - Plural/singular variations
   - Combined entries that need splitting
   - Renamed waterbodies

2. DIRECT_MATCHES: Explicit FWA feature mappings
   - Manual labels that directly specify which FWA feature(s) to link
   - Bypasses MU filtering (for boundary issues or incorrect MU data)
   - Takes priority over name variations and natural search
   - Keys use regulation name_verbatim (exact name from regulation text)

3. SKIP_ENTRIES: Waterbodies that should not be linked
   - Not found: Searched extensively but couldn't locate in FWA data
   - Ignored: Intentionally skipped (duplicates, cross-listings, redirects)

Format:
- Use "ALL REGIONS" for wildcard patterns that apply everywhere
- Wildcard patterns use * as placeholder, e.g., "* LAKE'S TRIBUTARIES"
"""

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class NameVariation:
    """
    Maps a regulation name to alternative gazetteer name(s).

    Simple name correction for fuzzy matching:
    - Spelling variations ("TOQUART" → "toquaht")
    - Name order ("MAXWELL LAKE" → "lake maxwell")
    - Plural/singular ("LAKES" → "lake")
    - 1:many splits ("RIVER1 AND RIVER2" → ["river1", "river2"])

    Attributes:
        target_names: FWA gazetteer name(s) to search for
        note: Explanation of the variation
    """

    target_names: List[str]
    note: str


@dataclass
class DirectMatch:
    """
    Maps a regulation name directly to exact FWA identifier(s).

    Pure ID-based lookup - no name searching needed.
    Use any FWA system identifier:

    - gnis_id: Best for lakes - matches ALL polygons with this GNIS ID
    - gnis_ids: List of GNIS identifiers (for multiple lakes/features)
    - fwa_watershed_code: Best for streams - matches ALL segments of a single stream (e.g., "700-123456-...")
    - fwa_watershed_codes: For multiple distinct streams - matches ALL segments from each watershed code
    - waterbody_poly_id: For a specific polygon by WATERBODY_POLY_ID (most precise)
    - waterbody_poly_ids: List of specific WATERBODY_POLY_IDs (for multiple specific polygons)
    - waterbody_key: For all polygons sharing a WATERBODY_KEY (e.g., all 3 Williston Lake polygons)
    - waterbody_keys: List of WATERBODY_KEYs (matches all polygons for each key)
    - linear_feature_ids: List of specific stream segment IDs (for individual stream segments)
    - blue_line_key: Blue Line Key - matches ALL stream segments AND polygons with this BLK
    - blue_line_keys: List of Blue Line Keys (matches all features from each BLK)

    Can combine multiple ID types to match both polygons and streams (e.g., slough polygon + tributary streams).

    Priority: Use gnis_id/gnis_ids for lakes, fwa_watershed_code/fwa_watershed_codes for streams when available.
    Use waterbody_poly_id/waterbody_poly_ids for specific polygons, waterbody_key for all polygons sharing a key.

    Attributes:
        gnis_id: GNIS identifier (matches all polygons of a lake)
        gnis_ids: List of GNIS identifiers (for multiple lakes/features)
        fwa_watershed_code: FWA watershed code for a single stream (matches all segments)
        fwa_watershed_codes: List of FWA watershed codes for multiple streams (matches all segments from each)
        waterbody_poly_id: Specific WATERBODY_POLY_ID (most precise polygon lookup)
        waterbody_poly_ids: List of WATERBODY_POLY_IDs (for multiple specific polygons)
        waterbody_key: WATERBODY_KEY (matches all polygons sharing this key)
        waterbody_keys: List of WATERBODY_KEYs (for multiple waterbody keys)
        linear_feature_ids: List of stream segment IDs (for specific stream segments)
        blue_line_key: Blue Line Key (matches all stream segments and polygons with this BLK)
        blue_line_keys: List of Blue Line Keys (matches all features from each BLK)
        note: Explanation of why this mapping exists
        ignored: If True, prevent all matching for this entry (intentional)
        not_found: If True, searched but couldn't locate in FWA data
    """

    note: str
    gnis_id: Optional[str] = None
    gnis_ids: Optional[List[str]] = None
    fwa_watershed_code: Optional[str] = None
    fwa_watershed_codes: Optional[List[str]] = None
    waterbody_poly_id: Optional[str] = None
    waterbody_poly_ids: Optional[List[str]] = None
    waterbody_key: Optional[str] = None
    waterbody_keys: Optional[List[str]] = None
    linear_feature_ids: Optional[List[str]] = None
    blue_line_key: Optional[str] = None
    blue_line_keys: Optional[List[str]] = None
    unmarked_waterbody_id: Optional[str] = None  # Links to custom UnmarkedWaterbody


@dataclass
class AdminDirectMatch:
    """
    Maps a regulation name to administrative boundary polygon(s).

    Used for synopsis regulations that apply to all FWA features within
    a specific administrative area (park, WMA, watershed, etc.).
    The matched polygon(s) are spatially intersected with FWA features
    to assign regulations to all streams/lakes/etc. within the boundary.

    Matching strategies (checked in order):
    1. feature_ids: Exact feature ID lookup in the admin layer
    2. feature_names: Case-insensitive partial name search

    Admin Layer Types (matching fetch_data.py / GPKG layer names):
        - "parks_bc"        → Provincial Parks & Ecological Reserves
        - "parks_nat"       → National Parks
        - "wma"             → Wildlife Management Areas
        - "watersheds"      → Named Watersheds
        - "historic_sites"  → Historic Sites

    Attributes:
        admin_layer: Which admin boundary layer to query
        note: Explanation of the match
        feature_ids: Specific feature IDs within the admin layer
        feature_names: Name(s) to search for in the admin layer (case-insensitive partial match)
        code_filter: Classification codes to pre-filter the layer by (e.g., ["PP"]
                     for provincial parks in parks_bc). Only effective when the
                     layer defines a code_field in ADMIN_LAYER_CONFIG.
        include_streams: Include stream features in spatial intersection
        include_lakes: Include lake features in spatial intersection
        include_wetlands: Include wetland features in spatial intersection
        include_manmade: Include manmade waterbody features in spatial intersection
    """

    admin_layer: str
    note: str
    feature_ids: Optional[List[int]] = None
    feature_names: Optional[List[str]] = None
    code_filter: Optional[List[str]] = None
    include_streams: bool = True
    include_lakes: bool = True
    include_wetlands: bool = False
    include_manmade: bool = False


@dataclass
class UnmarkedWaterbody:
    """
    Represents a custom waterbody not in the FWA database.

    Used for waterbodies that appear in regulations but don't exist in FWA.
    These are added to the gazetteer as searchable features with custom geometry.

    Geometry Types:
    - point: Single coordinate [longitude, latitude]
    - linestring: List of coordinates [[lon1, lat1], [lon2, lat2], ...]
    - polygon: List of coordinate rings [[[lon1, lat1], [lon2, lat2], [lon1, lat1]]]

    Attributes:
        unmarked_waterbody_id: Unique identifier (e.g., "UNMARKED_MARSH_POND_R2")
        name: Display name of the waterbody
        geometry_type: Type of geometry - "point", "linestring", or "polygon"
        coordinates: Coordinates in GeoJSON format (WGS84):
                     - Point: [longitude, latitude]
                     - LineString: [[lon1, lat1], [lon2, lat2], ...]
                     - Polygon: [[[lon1, lat1], [lon2, lat2], [lon1, lat1]]] (first ring is exterior)
        zones: List of zone numbers this waterbody appears in (e.g., ["2"])
        mgmt_units: List of management unit codes (e.g., ["2-4"])
        note: Explanation of where coordinates came from and why unmarked waterbody was created
        source_url: Optional URL reference for location/documentation
    """

    unmarked_waterbody_id: str
    name: str
    geometry_type: str  # "point", "linestring", or "polygon"
    coordinates: any  # Point: [lon, lat], LineString: [[lon, lat], ...], Polygon: [[[lon, lat], ...]]
    zones: List[str]
    mgmt_units: List[str]
    note: str
    source_url: Optional[str] = None


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
       - Duplicate entries (already covered elsewhere)
       - Cross-listed entries between regions
       - Alternative names for same waterbody
       - Reservoirs using parent river regulations
       - Historical names redirecting to current names

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
    Manages manual name variations, direct feature matches, admin direct matches,
    skip entries, and unmarked waterbodies.

    Provides lookup methods to check for corrections by region and name.
    """

    def __init__(
        self,
        name_variations: Dict[str, Dict[str, NameVariation]],
        direct_matches: Dict[str, Dict[str, DirectMatch]],
        skip_entries: Dict[str, Dict[str, SkipEntry]],
        unmarked_waterbodies: Dict[str, UnmarkedWaterbody],
        admin_direct_matches: Optional[Dict[str, Dict[str, AdminDirectMatch]]] = None,
    ):
        self.name_variations = name_variations
        self.direct_matches = direct_matches
        self.skip_entries = skip_entries
        self.unmarked_waterbodies = unmarked_waterbodies
        self.admin_direct_matches = admin_direct_matches or {}

    def get_skip_entry(self, region: str, name_verbatim: str) -> Optional[SkipEntry]:
        """Get skip entry for a regulation name in a region."""
        if region not in self.skip_entries:
            return None
        return self.skip_entries[region].get(name_verbatim)

    def get_direct_match(
        self, region: str, name_verbatim: str
    ) -> Optional[DirectMatch]:
        """Get direct match for a regulation name in a region."""
        if region not in self.direct_matches:
            return None
        return self.direct_matches[region].get(name_verbatim)

    def get_admin_direct_match(
        self, region: str, name_verbatim: str
    ) -> Optional[AdminDirectMatch]:
        """Get admin direct match for a regulation name in a region."""
        if region not in self.admin_direct_matches:
            return None
        return self.admin_direct_matches[region].get(name_verbatim)

    def get_name_variation(self, region: str, name: str) -> Optional[NameVariation]:
        """Get name variation for a regulation name in a region."""
        if region not in self.name_variations:
            return None
        return self.name_variations[region].get(name)

    def get_unmarked_waterbody(
        self, unmarked_waterbody_id: str
    ) -> Optional[UnmarkedWaterbody]:
        """Get unmarked waterbody by ID."""
        return self.unmarked_waterbodies.get(unmarked_waterbody_id)

    def has_skip_entry(self, region: str, name_verbatim: str) -> bool:
        """Check if a skip entry exists."""
        return self.get_skip_entry(region, name_verbatim) is not None

    def has_direct_match(self, region: str, name_verbatim: str) -> bool:
        """Check if a direct match exists."""
        return self.get_direct_match(region, name_verbatim) is not None

    def has_admin_direct_match(self, region: str, name_verbatim: str) -> bool:
        """Check if an admin direct match exists."""
        return self.get_admin_direct_match(region, name_verbatim) is not None

    def has_name_variation(self, region: str, name: str) -> bool:
        """Check if a name variation exists."""
        return self.get_name_variation(region, name) is not None


# Region-specific name variations
# Format: {"Region X": {"WATERBODY NAME": NameVariation(target_names=[...], note="...")}}
NAME_VARIATIONS: Dict[str, Dict[str, NameVariation]] = {
    "Region 1": {
        "TOQUART LAKE": NameVariation(
            target_names=["toquaht lake"], note="Spelling mismatch"
        ),
        "TOQUART RIVER": NameVariation(
            target_names=["toquaht river"], note="Spelling mismatch"
        ),
        "MAGGIE LAKE": NameVariation(
            target_names=["makii lake"],
            note="renamed in gazette (https://apps.gov.bc.ca/pub/bcgnws/names/62541.html)",
        ),
        "MAHATTA RIVER": NameVariation(
            target_names=["mahatta creek"], note="gazetteer lists as creek"
        ),
        '"BIG QUALICUM" RIVER': NameVariation(
            target_names=["qualicum river"], note="i think this is just qualicum river"
        ),  # TODO: add to ignore because duplicate entry with "QUALICUM RIVER" in same region
        '"MAXWELL LAKE" (Lake Maxwell)': NameVariation(
            target_names=["lake maxwell"], note="it is 'lake maxwell'"
        ),
        '"STOWELL LAKE" (Lake Stowell)': NameVariation(
            target_names=["lake stowell"], note="it is 'lake stowell' in gazetteer"
        ),
        '"WESTON LAKE"': NameVariation(
            target_names=["lake weston"], note="Name order correction"
        ),
    },
    "Region 2": {
        '"ERROCK" ("Squakum") LAKE': NameVariation(
            target_names=["lake errock"], note="Name order correction"
        ),
        "MCKAY CREEK": NameVariation(
            target_names=["mackay creek"], note="Spelling correction"
        ),
        "SARDIS PARK POND": NameVariation(
            target_names=["sardis pond"], note="Name simplification"
        ),
        '"JONES" LAKE': NameVariation(
            target_names=["wahleach lake"],
            note="Labelled as Wahleach Lake in GIS (https://www.bchydro.com/community/recreation_areas/jones_lake.html)",
        ),
        '"PAQ" LAKE': NameVariation(
            target_names=["lily lake"],
            note="Known locally as Lily Lake",
        ),
        "SWELTZER CREEK": NameVariation(
            target_names=["sweltzer river"],
            note="Labelled as Sweltzer River in GIS",
        ),
        "BEAR (Mahood) CREEK": NameVariation(
            target_names=["mahood creek"],
            note="Known as Mahood Creek in FWA",
        ),
        '"MARSHALL" CREEK': NameVariation(
            target_names=["marshall creek"],
            note="Remove quotes",
        ),
    },
    "Region 3": {
        '"MORGAN" LAKE': NameVariation(
            target_names=["morgan lake"],
            note="Remove quotes",
        ),
        "KWOTLENEMO (Fountain) LAKE": NameVariation(
            target_names=["kwotlenemo (fountain) lake"],
            note="Ensure parenthetical is included in search (fallback should handle this but being explicit)",
        ),
    },
    "Region 4": {
        "CARIBOU LAKES": NameVariation(
            target_names=["north caribou lake", "south caribou lake"],
            note="Split into North Caribou Lake and South Caribou Lake",
        ),
        "ARROW PARK (Mosquito) CREEK": NameVariation(
            target_names=["mosquito creek"], note="gazetteer lists as 'mosquito creek'"
        ),
        "EDWARDS LAKE": NameVariation(
            target_names=["edwards lakes"], note="Plural variation"
        ),
        "QUINN CREEK": NameVariation(
            target_names=["quinn (queen) creek"],
            note="Full name is 'Quinn (Queen) Creek' in gazetteer",
        ),
        "PEND D'OREILLE RIVER (Includes the reservoirs behind Waneta Dam and Seven Mile Dam)": NameVariation(
            target_names=["pend-d'oreille river"], note="Hyphenation correction"
        ),
        "GARBUTT LAKE": NameVariation(
            target_names=["norbury lake"],
            note="Official name is Norbury (Garbutt) Lake",
        ),
        "KOOCANUSA RESERVOIR": NameVariation(
            target_names=["lake koocanusa"], note="Name variation"
        ),
        "BURTON CREEK": NameVariation(
            target_names=["burton (trout) creek"],
            note="Full name is 'Burton (Trout) Creek' in gazetteer",
        ),
        "LAKE REVELSTOKE'S TRIBUTARIES": NameVariation(
            target_names=["revelstoke lake"],
            note="Tributary entry - link to parent waterbody (Lake Revelstoke → revelstoke lake); global_scope indicates tributaries only",
        ),
        "LITTLE SLOCAN LAKE'S TRIBUTARIES": NameVariation(
            target_names=[
                "upper little slocan lake",
                "lower little slocan lake",
            ],
            note="GIS has separate upper and lower tributary entrys; link to both",
        ),
        "PEND D'OREILLE RIVER'S TRIBUTARIES (except Salmo River)": NameVariation(
            target_names=["pend-d'oreille river"],
            note="Tributary entry - link to parent waterbody (uses hyphenated form); global_scope indicates tributaries only",
        ),
    },
    "Region 5": {
        "BALLON LAKE": NameVariation(
            target_names=["baillon lake"], note="Spelling correction"
        ),
        '"AGNUS" LAKE': NameVariation(
            target_names=["agnus lake"],
            note="Remove quotes",
        ),
        "WHALE LAKE (Canim Lake area)": NameVariation(
            target_names=["whale lake"],
            note="Remove quotes",
        ),
    },
    "Region 6": {
        "SUSTUT LAKES": NameVariation(
            target_names=["sustut lake"], note="Singular variation"
        ),
        "KSI HLGINX RIVER (formerly Ishkheenickh River)": NameVariation(
            target_names=["ksi hlginx"], note="Drops 'river' suffix"
        ),
        "KSI SGASGINIST CREEK (formerly Seaskinnish Creek)": NameVariation(
            target_names=["ksi sgasginist"], note="Drops 'creek' suffix"
        ),
        "KSI SII AKS RIVER (formerly Tseax River)": NameVariation(
            target_names=["ksi sii aks"], note="Drops 'river' suffix"
        ),
        "KSI X'ANMAS RIVER (formerly Kwinamass River)": NameVariation(
            target_names=["ksi x'anmas"], note="Drops 'river' suffix"
        ),
        'HEVENOR ("McQueen") CREEK': NameVariation(
            target_names=["hevenor creek"],
            note="Search for primary name without parenthetical",
        ),
        "MCDONNEL LAKE": NameVariation(
            target_names=["mcdonell lake"], note="Spelling correction"
        ),
    },
    "Region 7": {
        "MORFEE LAKE (south)": NameVariation(
            target_names=["morfee lakes"], note="Plural variation"
        ),
        "HAUTETE LAKE": NameVariation(
            target_names=["hautête lake"], note="Accent correction"
        ),
        "EAST HAUTETE LAKE": NameVariation(
            target_names=["east hautête lake"], note="Accent correction"
        ),
        "KLWALI LAKE": NameVariation(
            target_names=["klawli lake"], note="Spelling correction"
        ),
        "THORN CREEK": NameVariation(
            target_names=["thorne creek"], note="Spelling correction"
        ),
        "BOBTAIL (Naltesby) LAKE": NameVariation(
            target_names=["naltesby lake"],
            note="Search for alternate name Naltesby Lake",
        ),
    },
    "Region 8": {
        "TUC-EL-NUIT LAKE": NameVariation(
            target_names=["tugulnuit lake"], note="spelling mismatch"
        ),
        "BIGHORN RESERVOIR (Lakeview Irrigation District)": NameVariation(
            target_names=["big horn reservoir"], note="Spacing correction"
        ),
    },
}


# Direct feature matches - explicit FWA feature mappings by ID
# These take priority over name variations and natural search
# Keys use exact regulation name_verbatim
# Format: {"Region X": {"EXACT REGULATION NAME": DirectMatch(...)}}
DIRECT_MATCHES: Dict[str, Dict[str, DirectMatch]] = {
    "Region 1": {
        "LONG LAKE (Nanaimo)": DirectMatch(
            gnis_id="17501",
            note="Disambiguate using GNIS ID",
        ),
        "LANGFORD LAKE": DirectMatch(
            gnis_id="16325",
            note="FWA has MU 1-2, regulation has MU 1-21 (boundary issue)",
        ),
        "NOLA LAKE": DirectMatch(
            gnis_id="1816",
            note="FWA has MU 1-10, regulation has MU 1-9 (boundary issue)",
        ),
        "PANTHER LAKE": DirectMatch(
            gnis_id="21965",
            note="FWA has MU 1-6, regulation has MU 1-5 (boundary issue)",
        ),
        "PRIOR LAKE": DirectMatch(
            gnis_id="26972",
            note="Direct GNIS ID match",
        ),
        "PROVOST DAM": DirectMatch(
            waterbody_key="329101355",
            note="Unnamed lake/reservoir in FWA. Federal Fisheries Act Schedule: https://laws-lois.justice.gc.ca/eng/regulations/SOR-2008-120/section-sched743254-20220221.html",
        ),
        "PROSPECT LAKE": DirectMatch(
            gnis_id="26995",
            note="Direct GNIS ID match",
        ),
        # "HALL LAKE": DirectMatch(
        #     gnis_id="36138",
        #     note="FWA name is 'Hall Lakes' (plural); correct MU but polygon may be smaller than expected",
        # ),
        "HEALY LAKE'S OUTLET STREAM": DirectMatch(
            gnis_id="26468",
            note="Healy Lake's outlet stream is the South Englishman River (GNIS 26468) in Region 1 MU 1-5.",
        ),
        "MUCHALAT RIVER": DirectMatch(
            fwa_watershed_code="930-508366-413291-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Direct watershed code match",
        ),
        "THETIS LAKE": DirectMatch(
            gnis_id="21695",
            note="2 polygons with GNIS 21695 - Thetis Lake",
        ),
        "MAIN LAKE (Quadra Island)": DirectMatch(
            gnis_id="12126",
            note="2 polygons with GNIS 12126 - Main Lake on Quadra Island",
        ),
        "ILLUSION LAKES": DirectMatch(
            gnis_id="9801",
            note="7 polygons with GNIS 9801 - Illusion Lakes",
        ),
        '"ANDERSON" LAKE': DirectMatch(
            gnis_id="1657",
            note="Lake in MU 1-3. FWA has MU 1-6, regulation has MU 1-3 (boundary issue). Piscivorous rainbow trout and kokanee population. BC Lakes Database: survey_id 1129, WBID 00105SANJ, watershed code 930-063200-41400. ACAT 9030. Location found from map.",
        ),
        # Haida Gwaii waterbodies (MUs 6-12, 6-13 now managed as Region 1)
        "COPPER CREEK": DirectMatch(
            fwa_watershed_code="950-012855-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice",
        ),
        "DATLAMEN CREEK": DirectMatch(
            fwa_watershed_code="940-862496-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice",
        ),
        "DEENA CREEK": DirectMatch(
            fwa_watershed_code="950-976286-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice",
        ),
        "HONNA RIVER": DirectMatch(
            fwa_watershed_code="940-098825-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice",
        ),
        "PALLANT CREEK": DirectMatch(
            fwa_watershed_code="950-069770-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice",
        ),
        "TLELL RIVER": DirectMatch(
            fwa_watershed_code="940-051976-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice",
        ),
        "YAKOUN RIVER": DirectMatch(
            fwa_watershed_code="940-906664-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Haida Gwaii - MUs 6-12, 6-13 now managed as Region 1 per regulations notice",
        ),
        "BUTTLE LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="16747",
            note="Tributaries of Buttle Lake - links to parent waterbody (GNIS 16747)",
        ),
        "(Lower) CAMPBELL LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="17768",
            note="Tributaries of Campbell Lake - links to parent waterbody (GNIS 17768 - Campbell Lake)",
        ),
        "JOHN HART LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="18905",
            note="Tributaries of John Hart Lake - links to parent waterbody (GNIS 18905)",
        ),
        '"PETE\'S POND" Unnamed lake at the head of San Juan River': DirectMatch(
            waterbody_key="329504349",
            note="Unnamed lake at the head of San Juan River. BC Lakes Database survey_id 1155: 'A RECONNAISSANCE SURVEY OF PETE'S POND'. Regulation MU 1-3.",
        ),
        'UNNAMED LAKE "A" - MAP A (below)': DirectMatch(
            waterbody_key="329500453",
            note="Unnamed lake in MU 1-10. Location found from map in regulations.",
        ),
        'UNNAMED LAKE "B" - MAP A (below)': DirectMatch(
            waterbody_key="329500505",
            note="Unnamed lake in MU 1-10. Location found from map in regulations.",
        ),
        'UNNAMED LAKE "C" - MAP B (below)': DirectMatch(
            waterbody_key="329500435",
            note="Unnamed lake in MU 1-10. Location found from map in regulations.",
        ),
        'UNNAMED LAKE "D" - MAP B (below)': DirectMatch(
            waterbody_key="329500416",
            note="Unnamed lake in MU 1-10. Location found from map in regulations.",
        ),
        'UNNAMED LAKE "E" - MAP B (below)': DirectMatch(
            waterbody_key="329500498",
            note="Unnamed lake in MU 1-10. Location found from map in regulations.",
        ),
        'UNNAMED LAKE "F" - MAP B (below)': DirectMatch(
            waterbody_key="329500447",
            note="Unnamed lake in MU 1-10. Location found from map in regulations.",
        ),
        'UNNAMED LAKE "G" - MAP B (below)': DirectMatch(
            waterbody_key="329500561",
            note="Unnamed lake in MU 1-10. Location found from map in regulations.",
        ),
        'UNNAMED LAKE "H" - MAP B (below)': DirectMatch(
            waterbody_key="328988117",
            note="Unnamed lake in MU 1-10. Location found from map in regulations.",
        ),
        'UNNAMED LAKE "I" - MAP B (below)': DirectMatch(
            waterbody_key="329500475",
            note='Unnamed lake in MU 1-10 ("Elmer Lake" on Google Maps). Location found from map in regulations.',
        ),
        "MINE LAKE": DirectMatch(
            waterbody_key="329095512",
            note="Lake in MU 1-15. Also labelled as 'Main Lake' in gazette (https://www.canoevancouverisland.com/canoe-kayak-vancouver-island-directory/main-lake-canoe-chain-quadra-island/). Location found from map.",
        ),
    },
    "Region 2": {
        "VEDDER RIVER": DirectMatch(
            fwa_watershed_code="100-064535-057628-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            gnis_id="29662",
            note="Links to both Vedder River (stream) and Vedder Canal (polygon with GNIS_NAME_2: Vedder River). Canal is irrigation diversion from the river system.",
        ),
        "STAWAMUS RIVER": DirectMatch(
            fwa_watershed_code="900-102882-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="FWA has MU 2-8, regulation has MU 2-9 (boundary issue)",
        ),
        "MIAMI CREEK": DirectMatch(
            fwa_watershed_code="100-077501-243752-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="FWA has MU 2-18, regulation has MU 2-19 (boundary issue)",
        ),
        "HOPE SLOUGH": DirectMatch(
            fwa_watershed_code="100-072260-716322-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="FWA has MUs 2-3, 2-4, regulation has MU 2-8 (boundary issue)",
        ),
        "LUCILLE LAKE": DirectMatch(
            gnis_id="37388",
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
            gnis_id="1860",
            note="FWA name is Marshall Creek (GNIS 1860) in Region 2 MU 2-4. Lonzo is alternate name.",
        ),
        "CHILLIWACK LAKE": DirectMatch(
            gnis_id="13745",
            note="FWA has MU 2-3, regulation has MU 2-4 (boundary issue)",
        ),
        "MCLENNAN CREEK": DirectMatch(
            fwa_watershed_code="100-052188-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
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
            gnis_id="30212",
            note="2 polygons with GNIS 30212 - Twin Lakes",
        ),
        "SOUTH ALOUETTE RIVER": DirectMatch(
            fwa_watershed_code="100-025956-057184-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="South Alouette River. Regulation MU 2-8.",
        ),
        "BRUNETTE RIVER'S TRIBUTARIES": DirectMatch(
            gnis_id="10070",
            # fwa_watershed_code="100-019698-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Tributaries of Brunette River - links to parent waterbody (GNIS 10070 - Brunette River). Regulation MU 2-8.",
        ),
        "CHEHALIS LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="13012",
            note="Tributaries of Chehalis Lake - links to parent waterbody (GNIS 13012 - Chehalis Lake). Regulation MU 2-19.",
        ),
        "SQUAMISH RIVER'S TRIBUTARIES": DirectMatch(
            gnis_id="25671",
            # fwa_watershed_code="900-105574-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
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
            gnis_id="22474",
            note='Tributaries of Wahleach Lake - links to parent waterbody (GNIS 22474 - Wahleach Lake). Alternate name: "Jones" Lake. Regulation MU 2-3.',
        ),
        "BURNABY LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="10633",
            note="Tributaries of Burnaby Lake - links to parent waterbody (GNIS 10633)",
        ),
        "MINNEKHADA MARSH": DirectMatch(
            waterbody_key="329291857",
            note="Wetland in Minnekhada Regional Park. BC Lakes Database surveys: 'Minnekhada Regional Park Inventory - 2017; SU17-270318' and 'Minnekhada Regional Park Invasives - 2016; SU16-235849'. Regulation MU 2-8.",
        ),
        "CEDAR LAKE": DirectMatch(
            waterbody_key="329522925",
            note="Lake on boundary with MU 2-2. FWA has MU 2-17, regulation has MU 2-2 (boundary issue). Location found from map.",
        ),
        "CAP SHEAF LAKES": DirectMatch(
            waterbody_key="329197919",
            note="Lake in MU 2-16. Location found from map (https://www.alltrails.com/explore/recording/afternoon-hike-at-placer-mountain-0d770c4?p=-1&sh=li9ufv).",
        ),
        "CHEAM LAKE": DirectMatch(
            waterbody_key="329177896",
            note="Wetland in MU 2-3. Polygon found in FWA wetlands layer. Location found from map.",
        ),
        "GREEN TIMBERS LAKE": DirectMatch(
            waterbody_key="360887617",
            note="Lake in MU 2-4. Polygon found in FWA manmade waterbodies layer. Location found from map.",
        ),
        "JERRY SULINA PARK POND": DirectMatch(
            waterbody_key="329292590",
            note="Unnamed pond in MU 2-8. Location found from map.",
        ),
        "MACLEAN PONDS": DirectMatch(
            waterbody_key="329292212",
            note="Unnamed in FWA lakes layer. ID 070111626. Location found from map.",
        ),
        "MARSH POND": DirectMatch(
            unmarked_waterbody_id="UNMARKED_MARSH_POND_R2",
            note="No polygon in FWA. Using custom unmarked waterbody with coordinates from KML point in Aldergrove Regional Park.",
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
    },
    "Region 3": {
        "COLDWATER RIVER'S TRIBUTARIES": DirectMatch(
            gnis_id="18066",
            # fwa_watershed_code="100-190442-244975-337574-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Tributaries of Coldwater River - links to parent waterbody (GNIS 18066 - Coldwater River). Regulation MU 3-13.",
        ),
        "RAINBOW LAKE": DirectMatch(
            waterbody_key="329452040",
            note="Unnamed lake in Region 3 MU 3-12; found via BC Lakes Database (survey_id 4782: 'A RECONNAISSANCE SURVEY OF RAINBOW LAKE'). FWA has no GNIS ID.",
        ),
        "LLOYD LAKE": DirectMatch(
            gnis_id="33438",
            note="FWA has MU 3-29, regulation has MU 3-30 (boundary issue)",
        ),
        "MCARTHUR ISLAND SLOUGH": DirectMatch(
            # waterbody_key="329564232",
            # linear_feature_ids=[
            #     "703312800",
            #     "703312651",
            #     "703313162",
            #     "703312745",
            #     "703312290",
            # ],
            blue_line_key="355994157",
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
            gnis_id="16470",
            note="2 polygons with GNIS 16470 - Nahatlatch Lake (east and west)",
        ),
        "DEEP LAKE": DirectMatch(
            waterbody_key="329321237",
            note="Unnamed lake in MU 3-28. Location found from stocked lake map.",
        ),
        "JACKPINE LAKE": DirectMatch(
            waterbody_key="329320955",
            note="Unnamed lake in MU 3-28. Location found from map. Other Jackpine Lakes exist in Region 5 (GNIS 16938, MU 5-2) and Region 8 (GNIS 16941, MU 8-11).",
        ),
        '"NORMAN" LAKE (unnamed lake approximately 600 m southeast of Durand Lake)': DirectMatch(
            waterbody_key="329563878",
            note="Unnamed lake in MU 3-19, approximately 600 m southeast of Durand Lake. BC Lakes Database: WBID 00719THOM. ACAT 15541. Other Norman Lakes exist in Region 7 and Region 8. Location found from map.",
        ),
        "ROSE LAKE": DirectMatch(
            waterbody_key="329534365",
            note="Lake in MU 3-20. BC Lakes Database: WBID 00776STHM. ACAT 15585. Other Rose Lakes exist in Region 2, 5, and 6. Location found from map.",
        ),
        "CLANWILLIAM LAKE": DirectMatch(
            waterbody_key="329518210",
            note="Unnamed in FWA lakes layer. MU 3-34. BC Lakes Database: WBID 523394. ACAT ObjectID 1122331271. Gazetted name: CLANWILLIAM LAKE. Location found from map.",
        ),
        "LITTLE DUM LAKE": DirectMatch(
            waterbody_key="329321212",
            note="Lake in MU 3-28. BC Lakes Database: WBID 00618LNTH. ACAT ObjectID 15306 ('A Reconnaissance Survey Of Dum 2'). Found in stocked lakes map. Part of Dum Lake group (gazette: https://apps.gov.bc.ca/pub/bcgnws/names/27972.html).",
        ),
        "LITTLE LAC DES ROCHES (at west end of Lac Des Roches)": DirectMatch(
            waterbody_key="329320894",
            note="Lake in MU 3-30, at west end of Lac Des Roches. Location found from map.",
        ),
        '"LITTLE PETER HOPE" LAKE (unnamed lake approximately 200 m southwest of Peter Hope Lake)': DirectMatch(
            waterbody_key="329316229",
            note="Unnamed lake in MU 3-20, approximately 200 m southwest of Peter Hope Lake. BC Lakes Database: WBID 00497LNIC. ACAT ObjectID 10044. FWA Watershed Code: 120-246600-53700-23700-5090-0000-000-000-000-000-000-000. Location found from map.",
        ),
        "LORENZO LAKE": DirectMatch(
            waterbody_key="329354793",
            note="Lake in MU 3-39. BC Lakes Database: WBID 02102MAHD. ACAT ObjectID 7147. FWA Watershed Code: 129-360400-23900-98400-4800-9150-000-000-000-000-000-000. Location found from map.",
        ),
        "LOWER KANE LAKE": DirectMatch(
            waterbody_key="329316143",
            note="Lake in MU 3-13. BC Lakes Database: WBID 01088LNIC. ACAT ObjectID 31446. FWA Watershed Code: 120-246600-33700-41300-7100-0000-000-000-000-000-000-000. Same regulations as Upper Kane Lake. Location found from map.",
        ),
        "UPPER KANE LAKE": DirectMatch(
            waterbody_key="329316130",
            note="Lake in MU 3-13. BC Lakes Database: WBID 01083LNIC. ACAT ObjectID 4123. FWA Watershed Code: 120-246600-33700-41300-7100-0000-000-000-000-000-000-000. Same regulations as Lower Kane Lake. Location found from map.",
        ),
        "SICAMOUS NARROWS": DirectMatch(
            linear_feature_ids=["703030326"],
            # waterbody_poly_id="700189163",  # Future: river polygon when river polygons are added to matching
            note="Specific segment of Shuswap River in MU 3-26. Linear feature ID 703030326 represents the Sicamous Narrows portion. Also associated with river polygon 700189163 (will match in future when river polygons are added to linking system).",
        ),
        "TULIP LAKE": DirectMatch(
            waterbody_key="329534212",
            note="Lake in MU 3-20. BC Lakes Database: WBID 00762STHM. ACAT ObjectID 49860. FWA Watershed Code: 128-123700-73700-41000-0000-0000-000-000-000-000-000-000. Location found from map.",
        ),
    },
    "Region 4": {
        '"ALTA" LAKE': DirectMatch(
            waterbody_key="328965040",
            note="Unnamed lake in MU 4-3. Location found from map. Low confidence - Alta Lake GNIS 7915 is in Region 2 MUs 2-11, 2-9.",
        ),
        '"ALCES" LAKE': DirectMatch(
            waterbody_key="329247797",
            note="Lake in MU 4-24. BC Lakes Database: WBID 00374KOTR, ACAT 280743. Gazetted name: MOOSE LAKE. Location found from map.",
        ),
        "BRIDAL LAKE": DirectMatch(
            gnis_id="37990",
            note="FWA has MU 4-8, regulation has MU 4-7 (boundary issue)",
        ),
        "HALL LAKE": DirectMatch(
            gnis_id="36134",
            note="FWA has MU 4-20, regulation has MU 4-34 (boundary issue)",
        ),
        "HIAWATHA LAKE": DirectMatch(
            gnis_id="21359",
            note="FWA has MU 4-4, regulation has MU 4-3 (boundary issue)",
        ),
        "ROCK ISLAND LAKE": DirectMatch(
            gnis_id="26621",
            note="Named 'Rock Isle Lake' in FWA; has both polygon and KML point with same name/MU",
        ),
        "CONNOR LAKE": DirectMatch(
            gnis_id="19304",
            note="3 polygons with GNIS 19304 - Connor Lakes (plural in FWA)",
        ),
        "CONNOR LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="19304",
            note="Tributaries of Connor Lakes - links to parent waterbody (3 polygons with GNIS 19304)",
        ),
        "COLUMBIA LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="18123",
            note="Tributaries of Columbia Lake - links to parent waterbody (GNIS 18123 - Columbia Lake). Regulation MU 4-25.",
        ),
        "DUNCAN LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="8109",
            note="Tributaries of Duncan Lake - links to parent waterbody (GNIS 8109 - Duncan Lake). Regulation MU 4-27.",
        ),
        "ELK RIVER'S TRIBUTARIES (see exceptions)": DirectMatch(
            gnis_id="16880",
            # fwa_watershed_code="300-625474-584724-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Tributaries of Elk River - links to parent waterbody (GNIS 16880 - Elk River). Regulation MUs 4-2, 4-23.",
        ),
        "FLATHEAD RIVER'S TRIBUTARIES": DirectMatch(
            gnis_id="19843",
            # fwa_watershed_code="300-602565-854327-993941-902282-132363-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Tributaries of Flathead River - links to parent waterbody (GNIS 19843 - Flathead River). Regulation MU 4-1.",
        ),
        "KOOTENAY LAKE, ALL PARTS (Main Body, Upper West Arm and Lower West Arm)": DirectMatch(
            gnis_id="14091",
            note="Kootenay Lake - Main Body, Upper West Arm and Lower West Arm. GNIS 14091 - Kootenay Lake. Regulation MU 4-19.",
        ),
        "KOOTENAY LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="14091",
            note="Tributaries of Kootenay Lake - links to parent waterbody (GNIS 14091 - Kootenay Lake). Regulation MUs 4-7, 4-19.",
        ),
        "KINBASKET (McNaughton) LAKE": DirectMatch(
            gnis_id="3133",
            note="Kinbasket Lake (alternate name: McNaughton Lake). GNIS 3133 - 2 polygons spanning MUs 4-34, 4-36, 4-37, 4-38, 4-39, 4-40, 7-2. Regulation MU 4-36.",
        ),
        "KINBASKET (McNaughton) LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="3133",
            note="Tributaries of Kinbasket Lake - links to parent waterbody (GNIS 3133 - Kinbasket Lake). Regulation MU 4-36.",
        ),
        "LOWER ARROW LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="18644",
            note="Tributaries of Lower Arrow Lake - links to parent waterbody (GNIS 18644 - Lower Arrow Lake). Regulation MU 4-14.",
        ),
        "UPPER ARROW LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="8405",
            note="Tributaries of Upper Arrow Lake - links to parent waterbody (GNIS 8405 - Upper Arrow Lake).",
        ),
        "LAKE REVELSTOKE": DirectMatch(
            gnis_id="39145",
            note="GNIS 39145 - Revelstoke Lake. Regulation uses 'Lake Revelstoke' name order.",
        ),
        "REVELSTOKE LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="39145",
            note="Tributaries of Revelstoke Lake - links to parent waterbody (GNIS 39145 - Revelstoke Lake). Regulation MU 4-38.",
        ),
        "PREMIER LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="25274",
            note="Tributaries of Premier Lake - links to parent waterbody (GNIS 25274 - Premier Lake). Regulation MU 4-21.",
        ),
        "SLOCAN LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="27954",
            note="Tributaries of Slocan Lake - links to parent waterbody (GNIS 27954 - Slocan Lake). Regulation MU 4-17.",
        ),
        "TROUT LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="28481",
            note="Tributaries of Trout Lake - links to parent waterbody (GNIS 28481 - Trout Lake). Regulation MU 4-30.",
        ),
        "CHAMPION LAKE NO. 3": DirectMatch(
            waterbody_key="329262654",
            note="Lake in MU 4-8. BC Lakes Database: WBID 00352LARL, ACAT 4926. Location found from map.",
        ),
        "IDLEWILD LAKE (old Cranbrook Reservoir)": DirectMatch(
            waterbody_key="329524502",
            note="Lake in MU 4-3. BC Lakes Database: WBID 01249SMAR, ACAT 52677. Alternate name: old Cranbrook Reservoir. Location found from map.",
        ),
        '"MCCLAIN" LAKE': DirectMatch(
            waterbody_key="329220417",
            note="Lake in MU 4-34, approximately 750 m south of Mitten Lake. BC Lakes Database: WBID 00799KHOR, ACAT 22239. Spelling variations: McClain/McLain/McLean. Location found from map.",
        ),
        "SALMO RIVER'S TRIBUTARIES": DirectMatch(
            gnis_id="20528",
            note="Tributaries of Salmo River - links to parent waterbody (GNIS 20528 - Salmo River). Regulation MU 4-8.",
        ),
        "SLEWISKIN (Macdonald) CREEK": DirectMatch(
            gnis_id="6159",
            note="FWA name is McDonald Creek (GNIS 6159). Regulation uses 'SLEWISKIN (Macdonald) CREEK' with alternate name 'Macdonald'. Note spelling variation: Macdonald vs McDonald. Left tributary. MU 4-15.",
        ),
        "ECHOES LAKE (near Kimberley)": DirectMatch(
            gnis_id="7369",
            note="2 polygons with GNIS 7369 - Echoes Lakes (plural in FWA)",
        ),
        "MOYIE LAKE": DirectMatch(
            gnis_id="15779",
            note="2 polygons with GNIS 15779 - Moyie Lake spans MUs 4-4 and 4-5",
        ),
        "MOSES CREEK": DirectMatch(
            fwa_watershed_code="300-751058-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Moses Creek in MU 4-39. No GNIS name in FWA. Reference: Fish Collection Permit CB12-80431 Moses Creek Hydro Project Fisheries Impact Assessment (ACAT Report ID 37080) - proposed hydroelectric project utilizing flows of Moses and Beattie Creeks near Revelstoke.",
        ),
        'LEWIS ("Cameron") SLOUGH': DirectMatch(
            waterbody_key="329524002",
            note="Found in EAUBC Lakes dataset",
        ),
        '"LOST" LAKE': DirectMatch(
            waterbody_key="329123527",
            note="Near Elkford; found on AllTrails: https://www.alltrails.com/poi/canada/british-columbia/elkford/lost-lake; Using specific waterbody_key (GNIS 18009 is a different Lost Lake)",
        ),
        "TWIN LAKES": DirectMatch(
            waterbody_keys=["329524100", "328989162"],
            note="2 polygons near Premier Lake (accessed via 15min hike from overflow camping area); first lake encountered is Twin Lakes (Yankee); FWA MU 4-21, regulation MU 4-34 (boundary issue); Found via stocked lakes map",
        ),
        '"SPRING" LAKE': DirectMatch(
            waterbody_key="329069329",
            note="Unnamed lake approximately 1.5 km west/northwest of the west end of Tie Lake in MU 4-22. Medium confidence.",
        ),
        '"LITTLE MITTEN" LAKE (approx. 400 m west of Mitten Lake)': DirectMatch(
            waterbody_key="329220422",
            note="Unnamed lake approximately 400m west of Mitten Lake in MU 4-34; found via BC Lakes Database ('Kootenay Fisheries Field Report Little Mitten Lake 00753KHOR', located approx. 13 km SW of Parsons). FWA has no GNIS ID.",
        ),
        "FISHER MAIDEN LAKE": DirectMatch(
            waterbody_key="329524272",
            note="Found via fish stocking records and ACAT reports (https://a100.gov.bc.ca/pub/acat/public/viewReport.do?reportId=51502). FWA has MU 4-17, regulation has MU 4-26 (boundary issue).",
        ),
        "CHAMPION LAKES NO. 1 & 2": DirectMatch(
            waterbody_keys=["328974978", "329262641"],
            note="MU 4-8. BC Lakes Database: WBID 00339LARL (Champion Lake #1 Lower, survey_id 2030) and WBID 00346LARL (Champion Lake #2 Middle, survey_id). ACAT ObjectID 4923. Gazetted name: CHAMPION LAKES. Map reference: https://nrs.objectstore.gov.bc.ca/kuwyyf/champion_lakes_map_230d86f8ad.pdf. Location found from map.",
        ),
        "SAM'S FOLLY LAKE": DirectMatch(
            waterbody_key="328966176",
            note="Lake in MU 4-34. BC Lakes Database: WBID 00398COLR, ACAT 2716. Location found from map.",
        ),
        "WIGWAM RIVER (downstream of the access road adjacent to km 42 on the Bighorn (Ram) Forest Service Road)": DirectMatch(
            gnis_id="27703",
            note="Wigwam River in MU 4-2. Regulation specifies specific reach: 'downstream of the access road adjacent to km 42 on the Bighorn (Ram) Forest Service Road'. Links to entire river (GNIS 2311 - Wigwam River) as specific reach boundaries not in FWA data. NOTE: Divide is approximately at Linear Feature ID 706869683 for future reference.",
        ),
        "WIGWAM RIVER (upstream of the Forest Service recreation site adjacent to km 42 on the Bighorn (Ram) Forest Service Road)": DirectMatch(
            gnis_id="27703",
            note="Wigwam River in MU 4-2. Regulation specifies specific reach: 'upstream of the Forest Service recreation site adjacent to km 42 on the Bighorn (Ram) Forest Service Road'. Links to entire river (GNIS 2311 - Wigwam River) as specific reach boundaries not in FWA data. NOTE: Divide is approximately at Linear Feature ID 706869683 for future reference.",
        ),
    },
    "Region 5": {
        "ATNARKO/BELLA COOLA RIVERS EXCEPT: Burnt Bridge Creek upstream of Sitkatapa Creek, Hunlen Creek upstream of Hunlen Falls, and Young Creek upstream of Hwy 20 (see separate entries for these three waters)": DirectMatch(
            gnis_ids=["11611", "17209"],
            note="Links to both Bella Coola River (GNIS 11611) and Atnarko River (GNIS 17209). Bella Coola River flows from the coast (ocean) upstream to confluence of Atnarko and Talchako Rivers. Regulation MUs 5-6, 5-8, 5-11. IMPORTANT - Exceptions handling: Three tributary streams have separate regulation entries with different rules: (1) Burnt Bridge Creek upstream of Sitkatapa Creek, (2) Hunlen Creek upstream of Hunlen Falls, and (3) Young Creek upstream of Hwy 20. These exceptions will be handled as separate waterbody entries in the regulations with their own specific restrictions. When matching, the main Atnarko/Bella Coola rivers entry will link to the entire river systems, and the exception entries will link to specific upstream portions of the tributaries. Users querying these locations will see both the main river regulations AND the specific exception regulations if they apply.",
        ),
        "BABY CHARLOTTE LAKE": DirectMatch(
            waterbody_key="329021828",
            note="Lake in MU 5-6. Location found from map.",
        ),
        '"CRUISE" LAKE': DirectMatch(
            waterbody_key="329021826",
            note="Unnamed lake in MU 5-6, approximately 500 m south of Stewart Lake. BC Lakes Database: WBID 01304ATNA, ACAT 32593. Location found from map.",
        ),
        '"GEESE" LAKE (2 km northeast of Eliguk Lake)': DirectMatch(
            waterbody_key="329126819",
            note="Lake in MU 5-12, 2 km northeast of Eliguk Lake. Medium confidence. Location found from map.",
        ),
        '"KESTREL" LAKE': DirectMatch(
            waterbody_key="329586239",
            note="Lake in MU 5-2. BC Lakes Database: WBID 00238TWAC. Gazetted name: KESTREL LAKE. Location found from map.",
        ),
        "CHILKO LAKE": DirectMatch(
            gnis_id="33890",
            note="GNIS 33890 - Tŝilhqox Biny (current name) / Chilko Lake (former name). FWA has MU 5-4, regulation has MU 5-4 (correct MU).",
        ),
        "ALEXIS LAKE": DirectMatch(
            gnis_id="9356",
            note="GNIS 9356 - Tigulhdzin (current name) / Alexis Lake (former name). Name was officially changed.",
        ),
        "STUM LAKE": DirectMatch(
            gnis_id="16247",
            note="GNIS 16247 - Tegunlin (current name) / Stum Lake (former name). Name was officially changed.",
        ),
        "CHILKO LAKE'S tributary streams": DirectMatch(
            gnis_id="33890",
            note="Tributaries of Chilko Lake - links to parent waterbody (GNIS 33890 - Tŝilhqox Biny / Chilko Lake). FWA has MU 5-4, regulation has MU 5-4.",
        ),
        "TANYA LAKE'S TRIBUTARIES": DirectMatch(
            gnis_id="23919",
            note="Tributaries of Tanya Lakes - links to parent waterbody (GNIS 23919 - Tanya Lakes). FWA has MU 5-10, regulation has MU 5-10.",
        ),
        "BIG LAKE (approx. 30 km west of Likely)": DirectMatch(
            gnis_id="13037",
            note="Disambiguate using GNIS ID",
        ),
        "BIG LAKE (approx. 10 km west of 100 Mile House)": DirectMatch(
            gnis_id="33210",
            note="Disambiguate using GNIS ID",
        ),
        "BLUE LAKE (near Alexandria)": DirectMatch(
            gnis_id="14474",
            note="Disambiguate using GNIS ID",
        ),
        "BLUE LAKE (Soda Creek area)": DirectMatch(
            gnis_id="38199",
            note="Disambiguate using GNIS ID",
        ),
        "BRIDGE LAKE": DirectMatch(
            gnis_id="33293",
            note="FWA has MU 5-1, regulation has MU 5-2 (boundary issue)",
        ),
        "NIMPO LAKE": DirectMatch(
            gnis_id="21146",
            note="FWA has MU 5-6, regulation has MU 5-12 (boundary issue)",
        ),
        "KATHERINE LAKE": DirectMatch(
            gnis_id="11358",
            note="FWA has MU 5-2, regulation has MU 5-15 (boundary issue - lake is close to border)",
        ),
        "JACK OF CLUBS LAKE": DirectMatch(
            gnis_id="16935",
            note="FWA has MU 5-15, regulation has MU 5-2 (boundary issue)",
        ),
        "HUSH LAKE": DirectMatch(
            gnis_id="22322",
            note="FWA has MU 5-2, regulation has MU 5-15 (boundary issue - lake is on the border)",
        ),
        '"SANDY" LAKE': DirectMatch(
            waterbody_key="329481525",
            note="Unnamed lake approximately 3.2 km south of Le Bourdais Lake in MU 5-2. Confirmed via FDIS fish observation (Waterbody ID 48340, Project: Inventory, Rudy, Maud Creek, Sandy Lake; 2019). Reference: http://a100.gov.bc.ca/pub/acat/public/viewReport.do?reportId=58961. Note: There is a different Sandy Lake (GNIS 21196) in MU 5-16 which is NOT this one.",
        ),
        '"BLUFF" LAKE': DirectMatch(
            waterbody_key="329354763",
            note="Unnamed lake approximately 25 km NE of Lac La Hache in Region 5; found via BC Lakes Database (survey_id 20677: 'A Reconnaissance Survey of Bluff Lake'). FWA has no GNIS ID.",
        ),
        "FISH LAKE (unnamed lake approx. 2 km northwest of McClinchy Lake)": DirectMatch(
            waterbody_key="329238149",
            note="Unnamed lake in Region 5; found via BC Lakes Database (survey_id 315: 'UNTITLED REPORT: WINTER LIMNOLOGY DATA FOR FISH LAKE'). FWA has no GNIS ID.",
        ),
        '"SLIM" LAKE': DirectMatch(
            waterbody_key="329554444",
            note="Unnamed lake in Taseko River drainage approximately 4 km north of Cone Hill in MU 5-4; found via BC Lakes Database (survey_id 5210: 'Lake Survey: Slim Lake 00811TASR', associated with Taseko Mines Limited Fish Lake Project). FWA has no GNIS ID.",
        ),
        '"WHALE" LAKE (Gustafsen Lake area)': DirectMatch(
            waterbody_key="329116791",
            note="Whale Lake in Gustafsen Lake area; found via BC Lakes Database (survey_id from 1981: 'A Reconnaissance Survey of Whale Lake 00389DOGC, 1981'). FWA has MU 5-15, regulation has MU 5-2 (boundary issue).",
        ),
        '"RYE" LAKE': DirectMatch(
            waterbody_key="329480817",
            note="Unnamed lake approximately 1.6 km downstream of Joan Lake in MU 5-2; found via ACAT report (https://a100.gov.bc.ca/pub/acat/public/viewReport.do?reportId=23024). FWA has no GNIS ID.",
        ),
        '"PIGEON LAKE #1"': DirectMatch(
            waterbody_key="329116844",
            note="Unnamed lake adjacent to Dog Creek Road, approximately 9 km west of Gustafsen Lake and 19 km north of Meadow Lake Road in MU 5-2; found via BC Lakes Database ('A Reconnaissance Survey of Pigeon #1 (alias) Lake, WBID: 00525DOGC'). FWA has no GNIS ID.",
        ),
        '"SINKHOLE" LAKE': DirectMatch(
            waterbody_key="329495189",
            note="Unnamed lake approximately 100 m east of Sneezie Lake in MU 5-2; found via BC Lakes Database ('A Reconnaissance Survey of Unnamed Lake 5964', WSC 100-385000-98600-98900-5160-5530, located approx. 19.5 km east of Lac La Hache). FWA has no GNIS ID.",
        ),
        "SARDINE LAKE": DirectMatch(
            waterbody_key="329480905",
            note="Found via BC Lakes Database ('Fish Tissue Sample for Sardine lake', WBID: 00444QUES, December 1992). Regulation MU 5-2.",
        ),
        "MAYDOE LAKE": DirectMatch(
            waterbody_keys=["329021823", "329021799"],
            note="Known as Cowboy (Maydoe) Lakes in FWA; 2 polygons found via ACAT report ('Reconnaissance Survey of Cowboy (Maydoe) Lakes - 1997', WBIDs: 01344ATNA and 01372ATNA). Regulation MU 5-6.",
        ),
        'SUNSHINE ("Ant") LAKE': DirectMatch(
            gnis_id="10522",
            note="GNIS 10522 - Ant Lake. Regulation uses alternate name 'Sunshine' with 'Ant' in parentheses. Regulation MU 5-11.",
        ),
        "WENTWORTH LAKES": DirectMatch(
            waterbody_keys=["329430971", "329430952"],
            note="2 polygons found via BC Lakes Database ('A Reconnaissance Survey of Unnamed Lake (Upper Wentworth)', WBID: 00628NAZR and 'A Reconnaissance Survey of Wentworth Lake'). Regulation MU 5-13.",
        ),
        '"SNAG" LAKE': DirectMatch(
            waterbody_key="329060886",
            note="Unnamed lake approximately 60 km ESE of 100 Mile House (West King Area) in MU 5-1; found via BC Lakes Database (survey_id 20655: 'A Reconnaissance Survey of Snag Lake'). FWA has no GNIS ID.",
        ),
        "BEAVER CREEK chain of lakes": DirectMatch(
            gnis_id="11119",
            note="Beaver Creek chain of lakes in MU 5-2. FWA has name 'Beaver Creek' (GNIS 11119).",
        ),
        '"DOG" LAKE': DirectMatch(
            waterbody_key="329116771",
            note="Unnamed lake in MU 5-2, approximately 6 km south/southwest of the confluence of Dog and Pigeon Creeks. BC Lakes Database: WBID 00289DOGC. ACAT 6700. Other Dog Lakes exist in Region 4 (GNIS 24393, MU 4-25) and Region 6 (GNIS 24396, MU 6-16). Location found from map. Medium confidence.",
        ),
        '"GRIZZLY" LAKE (unnamed lake approx. 4.5 km upstream of Maeford Lake)': DirectMatch(
            waterbody_key="329074474",
            note="Lake in MU 5-15. BC Lakes Database: WBID 00514CARR, watershed code 160-466100-28200-84200. ACAT 54245. Location found from map. Low confidence.",
        ),
        "PADDY LAKE": DirectMatch(
            waterbody_key="329354753",
            note="Lake in MU 5-1, also known as Squirrel Lake. Paddy Lake Recreation Site located here (REC5960). Referred to as 'Paddy Squirrel Lake' in BC Lakes bathymetric maps. Another Paddy Lake (GNIS 21895) exists in Region 6 MU 6-26. Another Squirrel Lake listed in Region 6 MU 6-1. Location found from map.",
        ),
        '"GRASSY" LAKE (unnamed lake approx. 1 km southwest of West King Lake)': DirectMatch(
            waterbody_key="329061097",
            note="Unnamed lake in MU 5-1, approximately 1 km southwest of West King Lake. BC Lakes Database: WBID 00707BRID, watershed code 129-360400-23900-98400-9950-9850-283. ACAT 54219. Other Grassy Lakes exist in Region 4 (GNIS 19444, MU 4-16) and Region 8 (GNIS 19443, MU 8-25). Location found from map.",
        ),
        '"HIGH" LAKE (unnamed lake approx. 4 km north of Bridge Lake)': DirectMatch(
            waterbody_key="329060853",
            note="Unnamed lake in MU 5-1, approximately 4 km north of Bridge Lake. BC Lakes Database: WBID 00697BRID. ACAT 24310. Other High Lakes exist in Region 4 (GNIS 21399, MU 4-35) and Region 8 (GNIS 21400, MU 8-22). Location found from map.",
        ),
        '"LITTLE BISHOP" LAKE (approx. 1.7 km northeast of Bishop Lake)': DirectMatch(
            waterbody_key="329430970",
            note="Lake in MU 5-13, approximately 1.7 km northeast of Bishop Lake. Location found from map.",
        ),
        '"LITTLE JONES" LAKE': DirectMatch(
            waterbody_key="328987837",
            note="Unnamed lake in MU 5-2, approximately 13 km east/southeast of 150 Mile House on the north side of Jones Creek. Confirmed via stocked lakes map. Location found from map.",
        ),
        "MERIDIAN LAKE": DirectMatch(
            waterbody_key="329354797",
            note="Unnamed lake in MU 5-1, in Jim Creek system (North Thompson River watershed), approximately 55 km east of 100 Mile House. BC Lakes Database: ACAT 54214. FWA watershed code: 129-360400-23900-98400-4800. Location found from map.",
        ),
        'WEST ROAD ("Blackwater") RIVER': DirectMatch(
            fwa_watershed_code="100-500560-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="West Road River (Blackwater River). Regulation MUs 5-12, 5-13.",
        ),
    },
    "Region 6": {
        "UNNAMED LAKE (approx. 500 m south of Natalkuz Lake)": DirectMatch(
            waterbody_key="329318485",
            note="Unnamed lake in MU 6-1, approximately 500 m south of Natalkuz Lake. Location found from map.",
        ),
        "GLACIER (Redslide) CREEK (unnamed tributary to Nanika River)": DirectMatch(
            fwa_watershed_code="400-431358-585806-708951-288577-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Medium confidence. Historic Alcan water diversion boundary (1950): diverted Nanika watershed waters upstream of Glacier Creek, ~4km below Kidprice Lake",
        ),
        "LAKELSE RIVER": DirectMatch(
            fwa_watershed_code="400-174068-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="FWA has MU 6-11, regulation has MU 6-10 (boundary issue)",
        ),
        "WAHLA LAKE": DirectMatch(
            gnis_id="22471",
            note="FWA has MU 6-1, regulation has MU 6-2 (boundary issue - neighboring MUs but lake is 7km from boundary)",
        ),
        "DUNALTER LAKE (Irrigation Lake)": DirectMatch(
            gnis_id="2687",
            note="FWA has MU 6-8, regulation has MU 6-9 (boundary issue). Alternate name 'Irrigation Lake' confirmed by Irrigation Lake Park near the lake.",
        ),
        "OWEEGEE LAKE": DirectMatch(
            gnis_id="21851",
            note="FWA has MU 6-17, regulation has MU 6-16 (boundary issue - lake is less than 1.5km from border)",
        ),
        "LOST LAKE": DirectMatch(
            waterbody_key="329242402",
            note="FWA has MU 6-21, regulation has MU 6-15 (boundary issue). Lake near Terrace; incorrect GNIS_ID 39158 candidate found in MU 6-21. Using specific waterbody_key for correct lake. Location: https://www.google.com/maps/place/Lost+Lake/@54.6002328,-128.6552761,4390m | Reference: https://www.cbc.ca/news/canada/british-columbia/goldfish-invasion-closes-b-c-fishing-lake-1.5184045",
        ),
        "TOMS LAKE": DirectMatch(
            waterbody_key="329126712",
            note="Unnamed lake in Region 6 MU 6-1; found via BC Lakes Database (survey_id 73: 'UNTITLED REPORT: LIMNOLOGY DATA FOR TOMS LAKE'). FWA has no GNIS ID.",
        ),
        "CHIPMUNK LAKE": DirectMatch(
            waterbody_key="329126718",
            note="Unnamed lake in Region 6; found via BC Lakes Database (survey_id 65: 'UNTITLED REPORT: LIMNOLOGY DATA FOR CHIPMUNK LAKE'). FWA has no GNIS ID.",
        ),
        "TAGISH LAKE": DirectMatch(
            gnis_id="23158",
            note="2 polygons with GNIS 23158 - Tagish Lake",
        ),
        "RANCHERIA RIVER'S TRIBUTARIES": DirectMatch(
            fwa_watershed_code="200-692231-770914-177748-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
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
            fwa_watershed_code="915-679587-924307-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="East Gribbell Creek in MU 6-3, on Ursula Channel. Ungazetted stream. Waterbody ID 00000KHTZ. Cutthroat Trout observed 1994-01-01. Source: FISS Database survey_id 77237 '01-JAN-94 Fisheries Assessment of East Gribbell Creek on Ursula Channel'.",
        ),
        '"OLDFIELD" CREEK': DirectMatch(
            fwa_watershed_code="915-755598-329884-406409-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Oldfield Creek in MU 6-14, tributary of Hays Creek. Oldfield Creek Fish Hatchery located here.",
        ),
        '"SEELEY" CREEK (outlet of Seeley Lake)': DirectMatch(
            fwa_watershed_code="400-426207-245962-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Seeley Creek (outlet of Seeley Lake) in MU 6-9.",
        ),
        'WEST ROAD ("Blackwater") RIVER': DirectMatch(
            fwa_watershed_code="100-500560-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="West Road River (Blackwater River). Regulation MU 6-1.",
        ),
        'WEST ROAD ("Blackwater") RIVER\'S TRIBUTARIES': DirectMatch(
            fwa_watershed_code="100-500560-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Tributaries of West Road River - links to parent waterbody. Regulation MU 6-1. NOTE: Different regulations apply to Region 6 vs Region 7 - regulations must be applied separately for each region.",
        ),
    },
    "Region 7": {
        "EYE LAKE": DirectMatch(
            waterbody_key="329376649",
            note="Lake in MU 7-26. BC Lakes Database: WBID 00041MIDR, ACAT 3257. Location found from map.",
        ),
        "TSITNIZ LAKE": DirectMatch(
            gnis_id="29262",
            note="FWA has MU 7-8, regulation has MU 7-9 (boundary issue - lake is about 3km from border)",
        ),
        "SQUARE LAKE (located in Crooked River Provincial Park)": DirectMatch(
            waterbody_key="329102648",
            note="FWA has MU 7-12, regulation has MU 7-16 (boundary issue). Crooked River Provincial Park is partly in both 7-24 and 7-16; the lake is in 7-24 which is very close to 7-16.",
        ),
        "STELLAKO RIVER": DirectMatch(
            fwa_watershed_code="100-567134-374775-948201-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="FWA has MU 7-13, regulation has MU 7-12 (boundary issue)",
        ),
        "NATION RIVER": DirectMatch(
            fwa_watershed_code="200-948755-937012-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="FWA has MUs 7-28, 7-29, regulation has MU 7-30 (boundary issue)",
        ),
        "NAUTLEY RIVER": DirectMatch(
            fwa_watershed_code="100-567134-374775-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Nautley River in MU 7-13.",
        ),
        "TUPPER RIVER": DirectMatch(
            fwa_watershed_code="200-948755-780133-471255-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="FWA has MU 7-33, regulation has MU 7-20 (boundary issue - regulation refers to outlet weir at Swan Lake which is in 7-33)",
        ),
        "LA SALLE LAKES": DirectMatch(
            gnis_id="2482",
            note="FWA has MU 7-5, regulation has MU 7-3 (boundary issue). GNIS ID matches both polygons.",
        ),
        "LITTLE LOST LAKE": DirectMatch(
            waterbody_key="328993555",
            note="Unnamed lake in Region 7 MU 7-3; found via BC Lakes Database (survey_id 6447: 'A RECONNAISSANCE SURVEY OF UNNAMED (LITTLE LOST) LAKE'). FWA has no GNIS ID. Note: GNIS 10869 exists for 'Little Lost Lake' but is in Region 1 MU 1-6 (different lake).",
        ),
        '"LITTLE TOMAS" LAKE': DirectMatch(
            waterbody_key="329537273",
            note="Unnamed lake in Region 7 MU 7-25; found via BC Lakes Database (Report ID 3676: 'FORT ST. JAMES LAKE INVENTORY 1996 RECONNAISSANCE SURVEY OF UNNAMED LAKE 73 K113 (Little Tomas)', WBID: 01199STUL).",
        ),
        '"LOWER BEAVERPOND" LAKE (lowermost of the two Beaverpond lakes)': DirectMatch(
            waterbody_key="329654564",
            note="Unnamed lake in Region 7 MU 7-38 (lowermost of the two Beaverpond lakes); found via BC Lakes Database (Report ID 6399: 'A Reconnaissance Survey of Lower Beaver Pond Lake 00849UOMI'). Survey date: Mar 1, 1995.",
        ),
        "EMERALD LAKE": DirectMatch(
            gnis_id="31097",
            note="FWA has MU 7-16, regulation has MU 7-15 (boundary issue). Confirmed correct location via stocked lake maps - lake is in MU 7-16.",
        ),
        "DEM LAKE": DirectMatch(
            gnis_id="38071",
            note="FWA has MU 7-26, regulation has MU 7-25 (boundary issue - lake is approximately 2.5km from border)",
        ),
        "CHUBB LAKE": DirectMatch(
            gnis_id="13840",
            note="FWA has MU 7-8, regulation has MU 7-10 (boundary issue - neighboring MUs but lake not close to border, likely mislabeled MU)",
        ),
        "CHUNAMUN LAKE": DirectMatch(
            waterbody_key="328995585",
            note="Lake in Region 7 MU 7-35 (same waterbody as 'CHINAMAN' LAKE - alternate name); found via BC Lakes Database (Report ID 52574: 'Chunamun Lake Gillnet Survey - 1989', WBID: 00552UPCE). Survey date: Oct 1, 1989.",
        ),
        "DINA CREEK": DirectMatch(
            fwa_watershed_code="200-948755-936810-110196-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Dina Creek in MU 7-30. Unnamed creek that flows to Dina Lakes. Medium confidence.",
        ),
        "BEAR LAKE (Crooked River Provincial Park)": DirectMatch(
            gnis_id="11062",
            note="FWA has MU 7-24, regulation has MU 7-16 (boundary issue). Crooked River Provincial Park is mostly in 7-24 but extends into 7-16; the lake is in 7-24.",
        ),
        'WEST ROAD ("Blackwater") RIVER\'S TRIBUTARIES': DirectMatch(
            fwa_watershed_code="100-500560-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Tributaries of West Road River - links to parent waterbody. Regulation MU 7-10. NOTE: Different regulations apply to Region 6 vs Region 7 - regulations must be applied separately for each region.",
        ),
        "LYNX LAKE": DirectMatch(
            gnis_id="19238",
            note="FWA has MU 7-10, regulation has MU 7-15 (boundary issue - neighboring MUs but lake is 21km from border, likely mislabeled MU in regulations)",
        ),
        '"MT. MILLIGAN" LAKE': DirectMatch(
            waterbody_key="329424582",
            note="Unnamed lake in Region 7 MU 7-28 (located approximately 7.5 km south/southeast of Mt. Milligan); found via BC Lakes Database (Report ID 4121: 'Mount Milligan Lake 2004 Fish Stocking Assessment 01479NATR', WBID: 01479NATR).",
        ),
        "CAMERON LAKES": DirectMatch(
            gnis_id="38712",
            note="3 polygons with GNIS 38712 - Cameron Lakes",
        ),
        "SUNDANCE LAKE": DirectMatch(
            gnis_id="20296",
            note="2 polygons with GNIS 20296 - Sundance Lakes (plural in FWA)",
        ),
        "RAINBOW LAKES": DirectMatch(
            gnis_id="30756",
            note="2 polygons with GNIS 30756 - Rainbow Lakes",
        ),
        "RADAR LAKE": DirectMatch(
            waterbody_key="329328645",
            note="Lake in Region 7 MU 7-20; found via BC Lakes Database (Report ID 6880: 'Peace Fisheries Field Report: Radar Lake (230-690000-56100-63800-6807, 00680LPCE), 2004', WBID: 00680LPCE). Survey date: Jul 1, 2005.",
        ),
        "SHANDY LAKE": DirectMatch(
            waterbody_key="329382061",
            note="Lake in Region 7 MU 7-5; found via BC Lakes Database (survey_id 6484: 'BATHYMETRIC OF SHANDY LAKES', WBID 18411). Survey date: Aug 1, 1974.",
        ),
        "SWAN LAKE": DirectMatch(
            gnis_id="20911",
            note="Disambiguate from GNIS 20912; FWA has MU 7-33, regulation has MU 7-20 (boundary issue - lake is about 800m from boundary with MU 7-20)",
        ),
        "HART LAKE (Fort St. James)": DirectMatch(
            waterbody_key="329353216",
            note="Unnamed lake near Fort St. James; found via BC Lakes Database (survey_id 5255: 'A RECONNAISSANCE SURVEY OF UNNAMED \"HART\" LAKE'). FWA has no GNIS ID, regulation has MU 7-25.",
        ),
        "YELLOWHEAD LAKE": DirectMatch(
            gnis_id="30397",
            note="2 polygons with GNIS 30397 - Yellowhead Lake",
        ),
        "TATLATUI LAKE": DirectMatch(
            gnis_id="25404",
            note="2 polygons with GNIS 25404 - Tatlatui Lake",
        ),
        "TEBBUTT LAKE": DirectMatch(
            waterbody_key="329537362",
            note="Lake in Region 7 MU 7-13; found via BC Lakes Database (Report ID 9703: 'Tebbutt Lake - Reconnaissance Survey 1987 02283STUL', WBID: 02283STUL). Survey date: Jul 1, 1987.",
        ),
        "TACHEEDA LAKES (north and south)": DirectMatch(
            gnis_id="3956",
            note="2 polygons with GNIS 3956 - Tacheeda Lakes (north and south)",
        ),
        'UNNAMED LAKE ("Kinglet Lake") located 100 m west of Butterfly Lake': DirectMatch(
            waterbody_key="329343551",
            note='Unnamed lake locally known as "Kinglet Lake", 100m west of Butterfly Lake in MU 7-15. Identified via BC stocking records. Reference: https://www.env.gov.bc.ca/omineca/esd/faw/stocking/kinglet/kinglet_redstart_lakes2003.pdf',
        ),
        'UNNAMED LAKE ("Redstart Lake") located approx. 200 m southwest of Butterfly Lake': DirectMatch(
            waterbody_keys=["329343983", "329343806"],
            note='Unnamed lake locally known as "Redstart Lake", approximately 200m southwest of Butterfly Lake in MU 7-15. Two polygons identified via BC stocking records. Reference: https://www.env.gov.bc.ca/omineca/esd/faw/stocking/kinglet/kinglet_redstart_lakes2003.pdf',
        ),
        "WITCH LAKE": DirectMatch(
            waterbody_key="329424460",
            note="Lake in Region 7 MU 7-28; FWA name is 'Onjo Lake' (gazette name). Found via BC Lakes Database (Report ID 34828: 'A Reconnaissance Survey of Witch Lake, 1977 01386NATR', WBID: 01386NATR). Survey date: Jul 1, 1977.",
        ),
        "DINA LAKE #1": DirectMatch(
            waterbody_key="329465434",
            note="Found via BC Lakes Database ('Dina Lake #1 Pygmy Whitefish Study', WBID: 00357PARA, located north of Mackenzie). Regulation MU 7-30.",
        ),
        "DINA LAKE #2": DirectMatch(
            waterbody_key="329465455",
            note="Found via BC Lakes Database ('A Fisheries Evaluation of Dina Lake #2', WBID: 00346PARA). Regulation MU 7-30.",
        ),
    },
    "Region 8": {
        "FRAZER LAKE": DirectMatch(
            gnis_id="12484",
            note="FWA has MU 8-10, regulation has MU 8-9 (boundary issue - lake is 700m from border)",
        ),
        "ELLISON LAKE": DirectMatch(
            gnis_id="16910",
            note="FWA has MU 8-10, regulation has MU 8-8 (boundary issue - lake likely mislabeled MU)",
        ),
        "FIVE O'CLOCK LAKE (approx. 800 m southeast of Cup Lake)": DirectMatch(
            waterbody_key="329216683",
            note="Lake in Region 8 MU 8-14 (approx. 800 m southeast of Cup Lake); found via BC Lakes Database (Report ID 20691: 'Memo to File - Five O'Clock Lake Fish 00796KETL', WBID: 00796KETL). Survey date: May 1, 1992.",
        ),
        "HALL ROAD (Mission) POND": DirectMatch(
            waterbody_key="329460964",
            unmarked_waterbody_id="UNMARKED_HALL_ROAD_POND_R8",
            note="Region 8 MU 8-10. Links to both Mission Creek Regional Park Children's Fishing Pond unmarked waterbody (49.87084°N, 119.42958°W) AND adjacent FWA waterbody 329460964. Both locations provided to ensure comprehensive coverage of the fishing area.",
        ),
        "HEADWATER LAKE #1": DirectMatch(
            waterbody_key="329459136",
            note="Lake in Region 8 MU 8-8; found via BC Lakes Database (survey_id 5824: 'LAKE OVERVIEW DATA - HEADWATER LAKES;LAKE #1', WBID 175465). Survey date: Sep 1, 1972.",
        ),
        "CLIFFORD (Cliff ) LAKE": DirectMatch(
            waterbody_key="329520059",
            note="FWA incorrectly has GNIS_ID 31706 in Region 1 MU 1-14; correct lake is in Region 8 MU 8-5. BC Lakes Database: survey_id 3834 ('CLIFF AND RICK LAKES INVESTIGATION, JULY 24 & 25 1989', WBID 174079), survey date Jul 25, 1989. References: https://www.brmbmaps.com/explore/canada/british-columbia/thompson-nicola/clifford-lake-recreation-site/84451 | https://web.archive.org/web/20200529103953/https://www.sitesandtrailsbc.ca/search/search-result.aspx?site=REC1636&type=Site",
        ),
        "LARRY LAKE (unnamed lake located about 400 m west of Thalia Lake)": DirectMatch(
            waterbody_key="329520255",
            note="Unnamed lake west of Thalia Lake in Region 8 MU 8-5. BC Lakes Database: Report ID 13491 ('Memo to File: Larry Lake Investigation - June 11 & 12 1984 00470SIML', WBID 00470SIML), survey date Apr 1, 1986. Reference: https://www.brmbmaps.com/explore/canada/british-columbia/thompson-nicola/clifford-lake-recreation-site/84451",
        ),
        "TEE PEE LAKES": DirectMatch(
            gnis_id="21766",
            note="3 polygons with GNIS 21766 - Tepee Lakes (spelling variation in FWA)",
        ),
        "MCCULLOCH RESERVOIR": DirectMatch(
            gnis_id="15973",
            note="3 polygons with GNIS 15973 - McCulloch Reservoir",
        ),
        "ARLINGTON LAKES": DirectMatch(
            gnis_id="16647",
            note="6 polygons with GNIS 16647 - Arlington Lakes",
        ),
        "TWIN LAKES": DirectMatch(
            gnis_id="3086",
            note="3 polygons with GNIS 3086 - Twin Lakes span MUs 8-1 and 8-2",
        ),
        "RICKEY LAKE": DirectMatch(
            waterbody_key="329520185",
            note="Lake in Region 8 MU 8-5 (WBID: 00472SIML). Shares BC Lakes Database survey with CLIFFORD (Cliff) LAKE: survey_id 3834 ('CLIFF AND RICK LAKES INVESTIGATION, JULY 24 & 25 1989'), survey date Jul 25, 1989. References: https://adventuregenie.com/rv-campgrounds/british-columbia/merritt/rickey-lake | https://web.archive.org/web/20200529102211/https://www.sitesandtrailsbc.ca/search/search-result.aspx?site=REC1637&type=Site",
        ),
        "ROSE VALLEY RESERVOIR (Lakeview Irrigation District)": DirectMatch(
            waterbody_key="329459139",
            note="Lake in Region 8 MU 8-11 (Lakeview Irrigation District); found via BC Lakes Database (Report ID 43772: 'Survey of Rose Valley (Lake) Reservoir 1977', WBID: 00867OKAN). Survey date: May 1, 1977.",
        ),
        "SWAN LAKE": DirectMatch(
            gnis_id="20909",
            note="FWA has MU 8-22, regulation has MU 8-26 (boundary issue). Confirmed correct lake via stocked lakes map. Other Swan Lake (GNIS 37740) is in MU 8-6.",
        ),
        "GRANBY RIVER'S TRIBUTARIES": DirectMatch(
            gnis_id="18775",
            note="Tributaries of Granby River - links to parent waterbody (GNIS 18775 - Granby River). Regulation MU 8-15.",
        ),
        "OKANAGAN RIVER OXBOWS": DirectMatch(
            fwa_watershed_codes=[
                "300-432687-461418-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-461418-400917-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-463105-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-463105-427876-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-459615-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-466472-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-461418-565942-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-466472-328926-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-469486-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-476281-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "300-432687-476281-770576-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
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
            linear_feature_ids=["707367498"],
            note="Okanagan River Oxbows in MU 8-1. Multiple oxbow waterbodies and stream segments along the Okanagan River. Includes 32 watershed codes, 40 waterbody keys, and 1 linear feature ID.",
        ),
        "TREPANIER RIVER": DirectMatch(
            gnis_id="27648",
            note="Trepanier River in MU 8-8. FWA has name 'Trépanier Creek' (GNIS 27648).",
        ),
        "KETTLE RIVER'S TRIBUTARIES": DirectMatch(
            gnis_id="11984",
            note="Tributaries of Kettle River - links to parent waterbody (GNIS 11984 - Kettle River). Regulation MU 8-14.",
        ),
        "WEST KETTLE RIVER'S tributaries": DirectMatch(
            gnis_id="6358",
            note="Tributaries of West Kettle River - links to parent waterbody (GNIS 6358 - West Kettle River). Regulation MU 8-12.",
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
        #     waterbody_key="329520049",
        #     note="Part of Tepee Lakes group in Region 8 MU 8-6. TEE PEE LAKES regulation says 'See Friday, Saturday, and Sunday Lakes'. Has GNIS_NAME_2 attribute in FWA.",
        # ),
        # "SATURDAY LAKE": DirectMatch(
        #     waterbody_key="329520046",
        #     note="Part of Tepee Lakes group in Region 8 MU 8-6. TEE PEE LAKES regulation says 'See Friday, Saturday, and Sunday Lakes'. Has GNIS_NAME_2 attribute in FWA. FWA incorrectly shows GNIS_ID 23494 in Region 6 MU 6-8.",
        # ),
        # "SUNDAY LAKE": DirectMatch(
        #     waterbody_key="329520017",
        #     note="Part of Tepee Lakes group in Region 8 MU 8-6. TEE PEE LAKES regulation says 'See Friday, Saturday, and Sunday Lakes'. Has GNIS_NAME_2 attribute in FWA.",
        # ),
    },
}


# Skip entries - waterbodies that should not be linked
# Format: {"Region X": {"WATERBODY NAME": SkipEntry(note="...", not_found=True|ignored=True)}}
SKIP_ENTRIES: Dict[str, Dict[str, SkipEntry]] = {
    "Region 1": {
        '"LINK" RIVER': SkipEntry(
            note="Listed as 'Marble (Link) River' in gazzetteer",
            ignored=True,
        ),
        "BEAR RIVER": SkipEntry(
            note="Regulation says 'See Amor de Cosmos Creek'. Bear River is historical/alternate name for Amor de Cosmos Creek; regulations covered under that entry. Evidence: https://www.facebook.com/aboriginal.journeys/videos/819108737814861/ and https://www.flickr.com/photos/23057174@N02/16259837080",
            ignored=True,
        ),
    },
    "Region 2": {
        "LITTLE CAMPBELL RIVER": SkipEntry(
            note="Alternate name for Campbell River in MU 2-4. Regulations already covered under Campbell River entry.",
            ignored=True,
        ),
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
        "CAMERON SLOUGH": SkipEntry(
            note='Already covered by LEWIS ("Cameron") SLOUGH entry in Region 4. Same waterbody, regulation MU 4-21.',
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
        "KOOTENAY LAKE - MAIN BODY (for location see map on page 34)": SkipEntry(
            note="Main body of Kootenay Lake (excluding West Arm zones) requires custom polygon subdivision based on regulation map on page 34. MU 4-4. Kootenay Lake GNIS 18851, waterbody_key 331076875. Different regulations apply to Main Body vs Upper/Lower West Arms. Requires custom geometry creation by subdividing lake polygon. Reference: BC Freshwater Fishing Regulations Synopsis 2024-2026, Region 4, page 34.",
            not_found=True,
        ),
        "KOOTENAY LAKE - UPPER WEST ARM (for location see map on page 34)": SkipEntry(
            note="Upper West Arm of Kootenay Lake requires custom polygon subdivision based on regulation map on page 34. MU 4-4. Kootenay Lake GNIS 18851, waterbody_key 331076875. Different regulations apply to Upper West Arm vs Main Body and Lower West Arm. Requires custom geometry creation by subdividing lake polygon. Reference: BC Freshwater Fishing Regulations Synopsis 2024-2026, Region 4, page 34.",
            not_found=True,
        ),
        "KOOTENAY LAKE - LOWER WEST ARM (for location see map on page 34)": SkipEntry(
            note="Lower West Arm of Kootenay Lake requires custom polygon subdivision based on regulation map on page 34. MU 4-4. Kootenay Lake GNIS 18851, waterbody_key 331076875. Different regulations apply to Lower West Arm vs Main Body and Upper West Arm. Requires custom geometry creation by subdividing lake polygon. Reference: BC Freshwater Fishing Regulations Synopsis 2024-2026, Region 4, page 34.",
            not_found=True,
        ),
        "WANETA RESERVOIR'S TRIBUTARIES": SkipEntry(
            note="Covered by Pend d'Oreille River tributary regulations",
            ignored=True,
        ),
        "MCNAUGHTON LAKE": SkipEntry(
            note="Regulation says 'See Kinbasket Lake' - covered by Kinbasket Lake regulations",
            ignored=True,
        ),
    },
    "Region 5": {
        '"BLACKWATER" RIVER': SkipEntry(
            note='Already covered by WEST ROAD ("Blackwater") RIVER entry in Region 6. Same waterbody, regulation MU 5-13.',
            ignored=True,
        ),
        '"BROWN" LAKE': SkipEntry(
            note='Already covered by BISHOP ("Brown") LAKE entry',
            ignored=True,
        ),
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
        "COPPER RIVER": SkipEntry(
            note="Already covered by ZYMOETZ (Copper) RIVER entry. Same waterbody, regulation MU 6-9.",
            ignored=True,
        ),
        "ISHKHEENICKH RIVER": SkipEntry(
            note="Regulation says 'See Ksi Hlginx River'. River has been renamed to KSI HLGINX (GNIS 4069). Regulation MU 6-14.",
            ignored=True,
        ),
        "KWINAMASS RIVER": SkipEntry(
            note="Regulation says 'See Ksi X'anmas River'. River has been renamed to KSI X'ANMAS (GNIS 3815). Regulation MU 6-14.",
            ignored=True,
        ),
        "MCQUEEN CREEK": SkipEntry(
            note='Already covered by HEVENOR ("McQueen") CREEK entry',
            ignored=True,
        ),
        "SEASKINNISH CREEK": SkipEntry(
            note="Regulation says 'See Ksi Sgasginist Creek'. Creek has been renamed to KSI SGASGINIST CREEK. Regulation MU 6-15.",
            ignored=True,
        ),
        "SQUIRREL LAKE": SkipEntry(
            note="Searched but only found in Region 5 (MU 5-1, GNIS 38786, waterbody_key 329642030). Should be close to border with Region 5 MUs 5-10, 5-12, or 5-13 (neighboring MU 6-1). Lake likely exists near regional boundary but not found in FWA data for Region 6. May need duplicate feature or MU boundary correction. Reference: BC Freshwater Fishing Regulations Synopsis 2024-2026, Region 6, MU 6-1.",
            not_found=True,
        ),
        "TSEAX RIVER": SkipEntry(
            note="River has been renamed to KSI SII AKS RIVER. Regulation MU 6-14. Regulations apply to KSI SII AKS RIVER (formerly Tseax River).",
            ignored=True,
        ),
    },
    "Region 7": {
        "BLACKWATER RIVER": SkipEntry(
            note="Regulation says 'See West Road River'. Already covered by WEST ROAD ('Blackwater') RIVER entry. Regulation MU 7-10.",
            ignored=True,
        ),
        '"CHINAMAN" LAKE': SkipEntry(
            note="Same waterbody as CHUNAMUN LAKE (waterbody_key 328995585, WBID: 00552UPCE). Regulations refer to CHUNAMUN LAKE. Both names appear in BC Lakes Database surveys (Report ID 4631: 'A Reconnaissance Survey of Chinaman Lake' from 1984, and Report ID 52574: 'Chunamun Lake Gillnet Survey' from 1989).",
            ignored=True,
        ),
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
        "WILLISTON LAKE (in Zone B)": SkipEntry(
            note="Williston Lake Zone B requires custom polygon subdivision based on regulation description. MU 7-58. Williston Lake GNIS 21990, waterbody_key 329393419. Zone B is the remainder of Williston Lake excluding Zone A (500m east/upstream of Causeway Road). Different regulations apply to Zone B vs Zone A. Requires custom geometry creation by subdividing lake polygon based on Causeway Road location. Reference: BC Freshwater Fishing Regulations Synopsis 2024-2026, Region 7, MU 7-58.",
            not_found=True,
        ),
        "DAVIS BAY (in Finlay Reach of Williston Lake)": SkipEntry(
            note="Davis Bay in Finlay Reach of Williston Lake requires custom polygon subdivision. MU 7-58. Williston Lake GNIS 21990, waterbody_key 329393419. Finlay Reach is the northern arm where Finlay River (GNIS 12355) enters Williston Lake. Davis Bay is a specific bay within this reach. Requires custom geometry creation by identifying and subdividing the bay portion. Reference: BC Freshwater Fishing Regulations Synopsis 2024-2026, Region 7, MU 7-58.",
            not_found=True,
        ),
    },
    "Region 8": {
        "SAWMILL LAKE": SkipEntry(
            note="Alternative name for Burnell Lake - already covered by that entry",
            ignored=True,
        ),
    },
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
# IMPORTANT: Must use feature_ids with exact IDs from the admin layer.
#            Name-based matching (feature_names) is not allowed.
ADMIN_DIRECT_MATCHES: Dict[str, Dict[str, AdminDirectMatch]] = {
    "Region 1": {
        "STRATHCONA PARK WATERS": AdminDirectMatch(
            admin_layer="parks_bc",
            feature_ids=["1125", "1127"],
            note=(
                "Synopsis lists 'STRATHCONA PARK WATERS' in Region 1. "
                "Applies to all streams and lakes within Strathcona Provincial Park. "
                "Layer: TA_PARK_ECORES_PA_SVW, ID field: ADMIN_AREA_SID."
            ),
            include_streams=True,
            include_lakes=True,
            include_wetlands=True,
            include_manmade=True,
        ),
    },
    "Region 4": {
        "CRESTON VALLEY WILDLIFE MANAGEMENT AREA (CVWMA) WATERS": AdminDirectMatch(
            admin_layer="wma",
            feature_ids=["5364"],
            note=(
                "Synopsis lists 'CRESTON VALLEY WILDLIFE MANAGEMENT AREA (CVWMA) WATERS' in Region 4 MU 4-6. "
                "Applies to all streams and lakes within Creston Valley Wildlife Management Area. "
                "Layer: WLS_WILDLIFE_MGMT_AREA_SVW, ID field: ADMIN_AREA_SID."
            ),
            include_streams=True,
            include_lakes=True,
            include_wetlands=True,
            include_manmade=True,
        ),
        "KIKOMUN CREEK PARK (all lakes in the park)": AdminDirectMatch(
            admin_layer="parks_bc",
            feature_ids=["793"],
            note=(
                "Synopsis lists 'KIKOMUN CREEK PARK (all lakes in the park)' in Region 4 MU 4-22. "
                "Regulations apply specifically to lakes within Kikomun Creek Provincial Park. "
                "Layer: TA_PARK_ECORES_PA_SVW, ID field: ADMIN_AREA_SID."
            ),
            include_streams=False,
            include_lakes=True,
            include_wetlands=False,
            include_manmade=False,
        ),
    },
    "Region 5": {
        "BOWRON LAKE Park waters other than Bowron Lake": AdminDirectMatch(
            admin_layer="parks_bc",
            feature_ids=["519"],
            note=(
                "Synopsis lists 'BOWRON LAKE Park waters other than Bowron Lake' in Region 5 MU 5-16. "
                "Applies to all streams and lakes within Bowron Lake Provincial Park, excluding Bowron Lake itself. "
                "Layer: TA_PARK_ECORES_PA_SVW, ID field: ADMIN_AREA_SID."
            ),
            include_streams=True,
            include_lakes=True,
            include_wetlands=True,
            include_manmade=True,
        ),
    },
    "Region 6": {
        "CHILKOOT TRAIL NATIONAL HISTORIC PARK WATERS": AdminDirectMatch(
            admin_layer="historic_sites",
            feature_ids=["4af28ce2-bda0-47bf-8e64-664b1be54922"],
            note=(
                "Synopsis lists 'CHILKOOT TRAIL NATIONAL HISTORIC PARK WATERS' in Region 6 MU 6-28. "
                "Applies to all streams and lakes within the Chilkoot Trail National Historic Site. "
                "Layer: HIST_HERITAGE_WRECK_SVW, ID field: SITE_ID."
            ),
            include_streams=True,
            include_lakes=True,
            include_wetlands=True,
            include_manmade=True,
        ),
    },
    "Region 7": {
        "LIARD RIVER WATERSHED (see map on page 63)": AdminDirectMatch(
            admin_layer="watersheds",
            feature_ids=["5"],  # Named Watershed ID 5 (Object ID 6422089)
            note=(
                "Regulation specifies 'LIARD RIVER WATERSHED (see map on page 63)' in MU 7-53. "
                "FWA NAMED WATERSHED: Named Watershed ID 5, Object ID 6422089. "
                "Layer: FWA_NAMED_WATERSHEDS_POLY, ID field: NAMED_WATERSHED_ID."
            ),
            include_streams=True,
            include_lakes=True,
            include_wetlands=False,
            include_manmade=True,
        ),
    },
}


# Unmarked Waterbodies - custom waterbodies not in FWA database
# These are added to the gazetteer as searchable features with point geometry
# Format: {"UNMARKED_ID": UnmarkedWaterbody(...)}
UNMARKED_WATERBODIES: Dict[str, UnmarkedWaterbody] = {
    "UNMARKED_MARSH_POND_R2": UnmarkedWaterbody(
        unmarked_waterbody_id="UNMARKED_MARSH_POND_R2",
        name="MARSH POND",
        geometry_type="point",
        coordinates=[-122.4532345486399, 49.00878676964613],
        zones=["2"],
        mgmt_units=["2-4"],
        note="No polygon found in FWA lakes, wetlands, or manmade layers. Coordinates from KML point labeling in Aldergrove Regional Park.",
        source_url="https://metrovancouver.org/services/regional-parks/park/aldergrove-regional-park",
    ),
    "UNMARKED_HALL_ROAD_POND_R8": UnmarkedWaterbody(
        unmarked_waterbody_id="UNMARKED_HALL_ROAD_POND_R8",
        name="HALL ROAD (Mission) POND",
        geometry_type="point",
        coordinates=[-119.42958, 49.87084],
        zones=["8"],
        mgmt_units=["8-10"],
        note="Mission Creek Regional Park Children's Fishing Pond. Regulation name is 'HALL ROAD (Mission) POND'. Location coordinates identify the fishing pond; an adjacent FWA waterbody (329460964) also exists in the area.",
        source_url=None,
    ),
}
