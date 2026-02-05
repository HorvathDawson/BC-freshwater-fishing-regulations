"""
Manual name variations and direct matches for waterbody linking.

This module contains two types of manual corrections:

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
        ignored: If True, skip linking entirely (intentional)
        not_found: If True, searched but couldn't locate in FWA data
    """

    target_names: List[str]
    note: str
    ignored: bool = False
    not_found: bool = False


@dataclass
class DirectMatch:
    """
    Maps a regulation name directly to exact FWA identifier(s).

    Pure ID-based lookup - no name searching needed.
    Use any FWA system identifier:

    - gnis_id: Best for lakes - matches ALL polygons with this GNIS ID
    - fwa_watershed_code: Best for streams - matches ALL segments of a single stream (e.g., "700-123456-...")
    - fwa_watershed_codes: For multiple distinct streams - matches ALL segments from each watershed code
    - waterbody_key: For individual lake/wetland/manmade polygons (e.g., "12345")
    - waterbody_keys: List of specific polygon IDs (for multiple distinct polygons)
    - linear_feature_ids: List of specific stream segment IDs (for individual stream segments)

    Can combine multiple ID types to match both polygons and streams (e.g., slough polygon + tributary streams).

    Priority: Use gnis_id for lakes, fwa_watershed_code/fwa_watershed_codes for streams when available.
    Only use waterbody_key/waterbody_keys if you need to match specific polygons.

    Attributes:
        gnis_id: GNIS identifier (matches all polygons of a lake)
        fwa_watershed_code: FWA watershed code for a single stream (matches all segments)
        fwa_watershed_codes: List of FWA watershed codes for multiple streams (matches all segments from each)
        waterbody_key: Specific polygon ID (use if gnis_id unavailable)
        waterbody_keys: List of specific polygon IDs (for multiple polygons)
        linear_feature_ids: List of stream segment IDs (for specific stream segments)
        note: Explanation of why this mapping exists
        ignored: If True, prevent all matching for this entry (intentional)
        not_found: If True, searched but couldn't locate in FWA data
    """

    note: str
    gnis_id: Optional[str] = None
    fwa_watershed_code: Optional[str] = None
    fwa_watershed_codes: Optional[List[str]] = None
    waterbody_key: Optional[str] = None
    waterbody_keys: Optional[List[str]] = None
    linear_feature_ids: Optional[List[str]] = None
    ignored: bool = False
    not_found: bool = False


class ManualCorrections:
    """
    Manages manual name variations and direct feature matches.

    Provides lookup methods to check for corrections by region and name.
    """

    def __init__(
        self,
        name_variations: Dict[str, Dict[str, NameVariation]],
        direct_matches: Dict[str, Dict[str, DirectMatch]],
    ):
        self.name_variations = name_variations
        self.direct_matches = direct_matches

    def get_direct_match(
        self, region: str, name_verbatim: str
    ) -> Optional[DirectMatch]:
        """Get direct match for a regulation name in a region."""
        if region not in self.direct_matches:
            return None
        return self.direct_matches[region].get(name_verbatim)

    def get_name_variation(self, region: str, name: str) -> Optional[NameVariation]:
        """Get name variation for a regulation name in a region."""
        if region not in self.name_variations:
            return None
        return self.name_variations[region].get(name)

    def has_direct_match(self, region: str, name_verbatim: str) -> bool:
        """Check if a direct match exists."""
        return self.get_direct_match(region, name_verbatim) is not None

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
        ),
        '"MAXWELL LAKE" (Lake Maxwell)': NameVariation(
            target_names=["lake maxwell"], note="it is 'lake maxwell'"
        ),
        '"LINK" RIVER': NameVariation(
            target_names=[],
            note="Listed as 'Marble (Link) River' in gazzetteer",
            ignored=True,
        ),
        '"STOWELL LAKE" (Lake Stowell)': NameVariation(
            target_names=["lake stowell"], note="it is 'lake stowell' in gazetteer"
        ),
        '"WESTON LAKE"': NameVariation(
            target_names=["lake weston"], note="Name order correction"
        ),
        # Unnamed lakes from KML
        'UNNAMED LAKE "A" - MAP A (below)': NameVariation(
            target_names=["unnamed lake a - map a"], note="KML point match"
        ),
        'UNNAMED LAKE "B" - MAP A (below)': NameVariation(
            target_names=["unnamed lake b - map a"], note="KML point match"
        ),
        'UNNAMED LAKE "C" - MAP B (below)': NameVariation(
            target_names=["unnamed lake c - map b"], note="KML point match"
        ),
        'UNNAMED LAKE "D" - MAP B (below)': NameVariation(
            target_names=["unnamed lake d - map b"], note="KML point match"
        ),
        'UNNAMED LAKE "E" - MAP B (below)': NameVariation(
            target_names=["unnamed lake e - map b"], note="KML point match"
        ),
        'UNNAMED LAKE "F" - MAP B (below)': NameVariation(
            target_names=["unnamed lake f - map b"], note="KML point match"
        ),
        'UNNAMED LAKE "G" - MAP B (below)': NameVariation(
            target_names=["unnamed lake g - map b"], note="KML point match"
        ),
        'UNNAMED LAKE "H" - MAP B (below)': NameVariation(
            target_names=["unnamed lake h - map b"], note="KML point match"
        ),
        'UNNAMED LAKE "I" - MAP B (below)': NameVariation(
            target_names=['unnamed lake i - map b ("Elmer Lake" on Google Maps)'],
            note="KML point match with Google Maps annotation",
        ),
        '"PETE\'S POND" Unnamed lake at the head of San Juan River': NameVariation(
            target_names=["pete's pond", "petes pond"],
            note="Remove quotes and location descriptor, handle apostrophe variations",
        ),
        "BEAR RIVER": NameVariation(
            target_names=[],
            note="Regulation says 'See Amor de Cosmos Creek'. Bear River is historical/alternate name for Amor de Cosmos Creek; regulations covered under that entry. Evidence: https://www.facebook.com/aboriginal.journeys/videos/819108737814861/ and https://www.flickr.com/photos/23057174@N02/16259837080",
            ignored=True,
        ),
    },
    "Region 2": {
        "CHILLIWACK / VEDDER RIVERS (does not include Sumas River) (see map on page 24)": NameVariation(
            target_names=["chilliwack river", "vedder river"],
            note="Split combined entry into distinct rivers",
        ),
        "LILLOOET LAKE, LILLOOET RIVER": NameVariation(
            target_names=["lillooet lake", "lillooet river"],
            note="Split combined entry into distinct waterbodies.",
        ),
        "CAP SHEAF LAKES": NameVariation(
            target_names=["cap sheaf lake"],
            note="Plural to singular for KML point match (https://www.alltrails.com/explore/recording/afternoon-hike-at-placer-mountain-0d770c4?p=-1&sh=li9ufv)",
        ),
        '"ERROCK" ("Squakum") LAKE': NameVariation(
            target_names=["lake errock"], note="Name order correction"
        ),
        "CEDAR LAKE": NameVariation(
            target_names=["cedar lake"],
            note="KML point match; FWA has MU 2-17, regulation has MU 2-2 (boundary issue or human label override)",
        ),
        "MCKAY CREEK": NameVariation(
            target_names=["mackay creek"], note="Spelling correction"
        ),
        "SARDIS PARK POND": NameVariation(
            target_names=["sardis pond"], note="Name simplification"
        ),
        "HATZIC LAKE AND SLOUGH": NameVariation(
            target_names=["hatzic lake", "hatzic slough"],
            note="Split combined entry into distinct waterbodies",
        ),
        '"JONES" LAKE': NameVariation(
            target_names=["wahleach lake"],
            note="Labelled as Wahleach Lake in GIS (https://www.bchydro.com/community/recreation_areas/jones_lake.html)",
        ),
        "WEAVER LAKE and WEAVER CREEK": NameVariation(
            target_names=["weaver lake", "weaver creek"],
            note="Split combined entry into distinct waterbodies",
        ),
        '"PAQ" LAKE': NameVariation(
            target_names=["lily lake"],
            note="Known locally as Lily Lake",
        ),
        "SWELTZER CREEK": NameVariation(
            target_names=["sweltzer river"],
            note="Labelled as Sweltzer River in GIS",
        ),
        '"MOSS POTHOLE" LAKES': NameVariation(
            target_names=["moss pothole lakes"],
            note="KML point match",
        ),
        "MARSH POND": NameVariation(
            target_names=["marsh pond"],
            note="KML point match",
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
        "LITTLE DUM LAKE": NameVariation(
            target_names=["little dum lake", "little dum lake 2"],
            note="Split numbered lakes for KML point matches",
        ),
        '"LITTLE PETER HOPE" LAKE (unnamed lake approximately 200 m southwest of Peter Hope Lake)': NameVariation(
            target_names=["little peter hope lake"],
            note="KML point match",
        ),
        '"NORMAN" LAKE (unnamed lake approximately 600 m southeast of Durand Lake)': NameVariation(
            target_names=["norman lake"],
            note="KML point match",
        ),
        '"MORGAN" LAKE': NameVariation(
            target_names=["morgan lake"],
            note="Remove quotes",
        ),
        "KWOTLENEMO (Fountain) LAKE": NameVariation(
            target_names=["kwotlenemo (fountain) lake"],
            note="Ensure parenthetical is included in search (fallback should handle this but being explicit)",
        ),
        "RAINBOW LAKE": NameVariation(
            target_names=[],
            note="Searched but only found in Regions 2, 5, 6, 7, and 8, not Region 3 (regulation MU 3-12)",
            not_found=True,
        ),
    },
    "Region 4": {
        "CARIBOU LAKES": NameVariation(
            target_names=["north caribou lake", "south caribou lake"],
            note="Split into North Caribou Lake and South Caribou Lake",
        ),
        '"ALTA" LAKE': NameVariation(
            target_names=["alta lake"],
            note="KML point match",
        ),
        '"MCCLAIN" LAKE': NameVariation(
            target_names=["mcclain lake"],
            note="KML point match",
        ),
        "CHAMPION LAKES NO. 1 & 2": NameVariation(
            target_names=["champion lake no. 1", "champion lake no. 2"],
            note="Split numbered lakes for KML point matches",
        ),
        "ARROW PARK (Mosquito) CREEK": NameVariation(
            target_names=["mosquito creek"], note="gazetteer lists as 'mosquito creek'"
        ),
        "LAKE REVELSTOKE": NameVariation(
            target_names=["revelstoke lake"],
            note="Name order correction to match gazetteer",
        ),
        "ARROW LAKES": NameVariation(
            target_names=[],
            note="Regulation refers to Upper/Lower Arrow Lake details",
            ignored=True,
        ),
        "ARROW LAKES' TRIBUTARIES": NameVariation(
            target_names=[],
            note="Likely covered by Upper/Lower tributaries",
            ignored=True,
        ),
        "CONNOR LAKE": NameVariation(
            target_names=["connor lakes"], note="Plural variation"
        ),
        "ECHOES LAKE (near Kimberley)": NameVariation(
            target_names=["echoes lakes"], note="Plural variation"
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
        "SEVEN MILE RESERVOIR": NameVariation(
            target_names=[],
            note="Dammed portion of Pend d'Oreille River - uses same regulations as Pend d'Oreille River. This may change in future. Polygons are in unnamed manmade lakes.",
            ignored=True,
        ),
        "SEVEN MILE RESERVOIR'S TRIBUTARIES": NameVariation(
            target_names=[],
            note="Covered by Pend d'Oreille River tributary regulations",
            ignored=True,
        ),
        "WANETA RESERVOIR": NameVariation(
            target_names=[],
            note="Dammed portion of Pend d'Oreille River - uses same regulations as Pend d'Oreille River. This may change in future. Polygons are in unnamed manmade lakes.",
            ignored=True,
        ),
        "WANETA RESERVOIR'S TRIBUTARIES": NameVariation(
            target_names=[],
            note="Covered by Pend d'Oreille River tributary regulations",
            ignored=True,
        ),
        "ROCK ISLAND LAKE": NameVariation(
            target_names=["rock isle lake"],
            note="Name variation; rock isle lake (in 4-25, rock island lake in 3-39)",
        ),
        "GARBUTT LAKE": NameVariation(
            target_names=["norbury lake"],
            note="Official name is Norbury (Garbutt) Lake",
        ),
        "KOOCANUSA RESERVOIR": NameVariation(
            target_names=["lake koocanusa"], note="Name variation"
        ),
        "MCNAUGHTON LAKE": NameVariation(
            target_names=["kinbasket lake"],
            note="McNaughton Lake is part of Kinbasket Lake reservoir",
        ),
        '"ALCES" LAKE': NameVariation(
            target_names=["alces lake"],
            note="Remove quotes, KML point match",
        ),
        '"LITTLE MITTEN" LAKE (approx. 400 m west of Mitten Lake)': NameVariation(
            target_names=["little mitten lake"],
            note="Remove quotes and location descriptor",
        ),
        '"LOST" LAKE': NameVariation(
            target_names=["lost lake"],
            note="Remove quotes",
        ),
        '"SPRING" LAKE': NameVariation(
            target_names=["spring lake", '"spring" lake'],
            note="Remove quotes",
        ),
        "BURTON CREEK": NameVariation(
            target_names=["burton (trout) creek"],
            note="Full name is 'Burton (Trout) Creek' in gazetteer",
        ),
        "CONNOR LAKE'S TRIBUTARIES": NameVariation(
            target_names=["connor lakes"],
            note="Tributary entry - link to parent waterbody (Connor Lake → connor lakes); global_scope indicates tributaries only",
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
        '"GRASSY" LAKE (unnamed lake approx. 1 km southwest of West King Lake)': NameVariation(
            target_names=["grassy lake", "grassy lake 2"],
            note="Split numbered lakes for KML point matches",
        ),
        '"HIGH" LAKE (unnamed lake approx. 4 km north of Bridge Lake)': NameVariation(
            target_names=["high lake", "high lake 2", "high lake 3"],
            note="Split numbered lakes for KML point matches",
        ),
        '"GRIZZLY" LAKE (unnamed lake approx. 4.5 km upstream of Maeford Lake)': NameVariation(
            target_names=["grizzly lake"],
            note="KML point match",
        ),
        '"LITTLE JONES" LAKE': NameVariation(
            target_names=["little jones lake"],
            note="KML point match",
        ),
        '"CRUISE" LAKE': NameVariation(
            target_names=["cruise lake"],
            note="KML point match",
        ),
        '"KESTREL" LAKE': NameVariation(
            target_names=["kestrel lake"],
            note="KML point match",
        ),
        '"GEESE" LAKE (2 km northeast of Eliguk Lake)': NameVariation(
            target_names=["geese lake"],
            note="KML point match",
        ),
        '"LITTLE BISHOP" LAKE (approx. 1.7 km northeast of Bishop Lake)': NameVariation(
            target_names=["little bishop lake"],
            note="KML point match",
        ),
        '"AGNUS" LAKE': NameVariation(
            target_names=["agnus lake"],
            note="Remove quotes",
        ),
        '"BLACKWATER" RIVER': NameVariation(
            target_names=["blackwater river", '"blackwater" river'],
            note="Remove quotes",
        ),
        '"BLUFF" LAKE': NameVariation(
            target_names=["bluff lake"],
            note="Remove quotes",
        ),
        '"BROWN" LAKE': NameVariation(
            target_names=[],
            note='Already covered by BISHOP ("Brown") LAKE entry',
            ignored=True,
        ),
        '"DOG" LAKE': NameVariation(
            target_names=["dog lake"],
            note="Remove quotes, KML point match",
        ),
        '"PIGEON LAKE #1"': NameVariation(
            target_names=["pigeon lake #1", "pigeon lake no. 1", '"pigeon lake #1"'],
            note="Remove quotes, handle number variations",
        ),
        '"RYE" LAKE': NameVariation(
            target_names=["rye lake", '"rye" lake'],
            note="Remove quotes",
        ),
        '"SINKHOLE" LAKE': NameVariation(
            target_names=["sinkhole lake", '"sinkhole" lake'],
            note="Remove quotes",
        ),
        '"SLIM" LAKE': NameVariation(
            target_names=["slim lake", '"slim" lake'],
            note="Remove quotes",
        ),
        '"SNAG" LAKE': NameVariation(
            target_names=["snag lake"],
            note="Remove quotes",
        ),
        "WHALE LAKE (Canim Lake area)": NameVariation(
            target_names=["whale lake"],
            note="Remove quotes",
        ),
        "CHILKO LAKE'S tributary streams": NameVariation(
            target_names=["chilko lake"],
            note="Tributary entry - link to parent waterbody; global_scope indicates tributaries only",
        ),
        "REDFERN LAKE": NameVariation(
            target_names=[],
            note="Searched but only found in Region 7 (MU 7-42), not Region 5 (regulation MU 5-15)",
            not_found=True,
        ),
        "SECRET LAKE": NameVariation(
            target_names=[],
            note="Searched but only found in Region 8 (MU 8-7, GNIS 37609) and Region 3 (MU 3-30, GNIS 38166), not Region 5 (regulation MU 5-6)",
            not_found=True,
        ),
        "FROG LAKE": NameVariation(
            target_names=[],
            note="Searched but only found in Region 3 (MU 3-29, GNIS 12556), Region 1 (MU 1-10, GNIS 12554), and Region 7 (MU 7-30, GNIS 56145), not Region 5 (regulation MU 5-6)",
            not_found=True,
        ),
        "HIDDEN LAKE": NameVariation(
            target_names=[],
            note="Searched but only found in Region 5 MU 5-15 (GNIS 21382), not in regulation MU 5-6",
            not_found=True,
        ),
        "TOMS LAKE": NameVariation(
            target_names=[],
            note="Cross-listed entry - already covered in Region 6 (MU 6-1)",
            ignored=True,
        ),
        "SQUIRREL LAKE": NameVariation(
            target_names=[],
            note="Cross-listed entry - already covered in Region 6 (MU 6-1)",
            ignored=True,
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
        '"DIANA" CREEK': NameVariation(
            target_names=["diana creek", '"diana" creek'],
            note="Remove quotes",
        ),
        '"EAST GRIBBELL" CREEK': NameVariation(
            target_names=["east gribbell creek", '"east gribbell" creek'],
            note="Remove quotes",
        ),
        '"OLDFIELD" CREEK': NameVariation(
            target_names=["oldfield creek", '"oldfield" creek'],
            note="Remove quotes",
        ),
        '"SEELEY" CREEK (outlet of Seeley Lake)': NameVariation(
            target_names=["seeley creek"],
            note="Remove quotes and location descriptor",
        ),
        "MCQUEEN CREEK": NameVariation(
            target_names=[],
            note='Already covered by HEVENOR ("McQueen") CREEK entry',
            ignored=True,
        ),
        "TOMS LAKE": NameVariation(
            target_names=[],
            note="Searched but only found in Region 7 (MU 7-33, GNIS 26726). Should be close to border with Region 5 MUs 5-10, 5-12, or 5-13 (neighboring MU 6-1). Lake likely exists near regional boundary but not found in FWA data for Region 6.",
            not_found=True,
        ),
        "SQUIRREL LAKE": NameVariation(
            target_names=[],
            note="Searched but only found in Region 5 (MU 5-1, GNIS 38786). Should be close to border with Region 5 MUs 5-10, 5-12, or 5-13 (neighboring MU 6-1). Lake likely exists near regional boundary but not found in FWA data for Region 6.",
            not_found=True,
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
        "JOHN'S LAKE": NameVariation(
            target_names=["johns lake", "john's lake"],
            note="Possessive apostrophe removal",
        ),
        '"LITTLE TOMAS" LAKE': NameVariation(
            target_names=["little tomas lake", '"little tomas" lake'],
            note="Remove quotes",
        ),
        '"LOWER BEAVERPOND" LAKE (lowermost of the two Beaverpond lakes)': NameVariation(
            target_names=["lower beaverpond lake"],
            note="Remove quotes and location descriptor",
        ),
        '"MT. MILLIGAN" LAKE': NameVariation(
            target_names=[
                "mt. milligan lake",
                "mount milligan lake",
                '"mt. milligan" lake',
            ],
            note="Remove quotes, handle Mt./Mount variations",
        ),
        '"CHINAMAN" LAKE': NameVariation(
            target_names=["chinaman lake", '"chinaman" lake'],
            note="Remove quotes",
        ),
        "KLWALI LAKE": NameVariation(
            target_names=["klawli lake"], note="Spelling correction"
        ),
        "THORN CREEK": NameVariation(
            target_names=["thorne creek"], note="Spelling correction"
        ),
        "SUNDANCE LAKE": NameVariation(
            target_names=["sundance lakes"], note="Plural variation"
        ),
        "ENDAKO RIVER": NameVariation(
            target_names=[],
            note="Cross-listed entry - already covered in Region 6 (MUs 6-4, 6-5)",
            ignored=True,
        ),
        "HART LAKE (Fort St. James)": NameVariation(
            target_names=[],
            note="Searched but only found Hart Lake in Region 7 MU 7-16 (GNIS 15347), not in regulation MU 7-25. The only information about a Hart Lake near Fort St. James is a mention in scenic areas listing (1999/2005): https://www2.gov.bc.ca/assets/gov/farming-natural-resources-and-industry/natural-resource-use/land-water-use/crown-land/land-use-plans-and-objectives/omineca-region/fortstjames-lrmp/fortstjames-srmp/fortstjames_forest_district_listing_scenic_areas_1999_versus_2005.pdf - requires further investigation.",
            not_found=True,
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
        "TEE PEE LAKES": NameVariation(
            target_names=["tepee lakes"], note="Spelling correction"
        ),
        '"BLUEY LAKE POTHOLES"': NameVariation(
            target_names=["bluey lake potholes", '"bluey lake potholes"'],
            note="Remove quotes",
        ),
        "FIVE O'CLOCK LAKE (approx. 800 m southeast of Cup Lake)": NameVariation(
            target_names=["five o'clock lake", "5 o'clock lake"],
            note="Remove location descriptor, handle apostrophe and number variations",
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
        "PROSPECT LAKE": DirectMatch(
            gnis_id="26995",
            note="Direct GNIS ID match",
        ),
        "HALL LAKE": DirectMatch(
            gnis_id="36138",
            note="FWA name is 'Hall Lakes' (plural); correct MU but polygon may be smaller than expected",
        ),
        "MUCHALAT RIVER": DirectMatch(
            fwa_watershed_code="930-508366-413291-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Direct watershed code match",
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
    },
    "Region 3": {
        "LLOYD LAKE": DirectMatch(
            gnis_id="33438",
            note="FWA has MU 3-29, regulation has MU 3-30 (boundary issue)",
        ),
        "MCARTHUR ISLAND SLOUGH": DirectMatch(
            waterbody_key="329564232",
            linear_feature_ids=[
                "703312800",
                "703312651",
                "703313162",
                "703312745",
                "703312290",
            ],
            note="Slough polygon + tributary stream segments",
        ),
        "MAKA CREEK": DirectMatch(
            fwa_watershed_codes=[
                "100-190442-244975-232973-504304-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
                "100-190442-244975-119256-796149-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            ],
            note="Two candidates in MU 3-13: (1) WSC 100-190442-244975-232973-504304 (103 segments) - MOST LIKELY match as it is found inside fisheries sensitive wildlife area, or (2) WSC 100-190442-244975-119256-796149 (9 segments). Linked to both for completeness.",
        ),
    },
    "Region 4": {
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
        'LEWIS ("Cameron") SLOUGH': DirectMatch(
            waterbody_key="329524002",
            note="Found in EAUBC Lakes dataset",
        ),
        '"LOST" LAKE': DirectMatch(
            waterbody_key="329123527",
            note="Near Elkford; found on AllTrails: https://www.alltrails.com/poi/canada/british-columbia/elkford/lost-lake; Using specific waterbody_key (GNIS 18009 is a different Lost Lake)",
        ),
    },
    "Region 5": {
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
            note="Unnamed lake approximately 3.2 km south of Le Bourdais Lake in MU 5-2 (correct region). Note: There is a different Sandy Lake (GNIS 21196) in MU 5-16 which is NOT this one. Medium confidence - could alternatively be waterbody_key 329482112 but difficult to confirm.",
        ),
        '"BLUFF" LAKE': DirectMatch(
            waterbody_key="329354763",
            note="Unnamed lake approximately 2 km east/northeast of Spout Lake; medium confidence match based on location",
        ),
        "FISH LAKE (unnamed lake approx. 2 km northwest of McClinchy Lake)": DirectMatch(
            waterbody_keys=["329238149", "328973937", "329238322"],
            note="Low confidence: unnamed lake approx. 2 km northwest of McClinchy Lake; no info on lake, match based only on location descriptor in regulations",
        ),
        '"SNAG" LAKE': DirectMatch(
            waterbody_key="329060886",
            note="FWA has MU 5-2, regulation has MU 5-1 (boundary issue). Unnamed lake approximately 500 m south/southeast of West King Lake; medium confidence match based on location.",
        ),
    },
    "Region 6": {
        "GLACIER (Redslide) CREEK (unnamed tributary to Nanika River)": DirectMatch(
            fwa_watershed_code="400-431358-585806-708951-288577-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="Historic Alcan water diversion boundary (1950): diverted Nanika watershed waters upstream of Glacier Creek, ~4km below Kidprice Lake",
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
    },
    "Region 7": {
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
        "TUPPER RIVER": DirectMatch(
            fwa_watershed_code="200-948755-780133-471255-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000",
            note="FWA has MU 7-33, regulation has MU 7-20 (boundary issue - regulation refers to outlet weir at Swan Lake which is in 7-33)",
        ),
        "LA SALLE LAKES": DirectMatch(
            gnis_id="2482",
            note="FWA has MU 7-5, regulation has MU 7-3 (boundary issue). GNIS ID matches both polygons.",
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
        "BEAR LAKE (Crooked River Provincial Park)": DirectMatch(
            gnis_id="11062",
            note="FWA has MU 7-24, regulation has MU 7-16 (boundary issue). Crooked River Provincial Park is mostly in 7-24 but extends into 7-16; the lake is in 7-24.",
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
        "CLIFFORD (Cliff ) LAKE": DirectMatch(
            waterbody_key="329520059",
            note="FWA incorrectly has GNIS_ID 31706 in Region 1 MU 1-14; correct lake is in Region 8 MU 8-5. References: https://www.brmbmaps.com/explore/canada/british-columbia/thompson-nicola/clifford-lake-recreation-site/84451 | https://web.archive.org/web/20200529103953/https://www.sitesandtrailsbc.ca/search/search-result.aspx?site=REC1636&type=Site",
        ),
        "LARRY LAKE (unnamed lake located about 400 m west of Thalia Lake)": DirectMatch(
            waterbody_key="329520255",
            note="Unnamed lake west of Thalia Lake in Region 8 MU 8-5. Reference: https://www.brmbmaps.com/explore/canada/british-columbia/thompson-nicola/clifford-lake-recreation-site/84451",
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
