import pytest
import pdfplumber
import os
import re

# Import the main class from your script file
from extract_synopsis import FishingSynopsisParser

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PDF_PATH = os.path.join(SCRIPT_DIR, "output", "fishing_synopsis.pdf")

# --- HELPER ---
def raw_regs_contain(row, text):
    """
    Helper to search for text within the raw regulation text.
    Returns True if 'text' is found in the raw_regs string.
    Works with both dict and dataclass row objects.
    """
    if hasattr(row, 'raw_regs'):
        return text in row.raw_regs
    elif isinstance(row, dict) and 'raw_regs' in row:
        return text in row['raw_regs']
    return False

@pytest.fixture(scope="module")
def parser():
    """Fixture to provide a class instance."""
    return FishingSynopsisParser()

@pytest.fixture(scope="module")
def pdf():
    if not os.path.exists(PDF_PATH):
        pytest.fail(f"PDF not found at {PDF_PATH}")
    with pdfplumber.open(PDF_PATH) as p:
        yield p

# ==========================================
#      PART 1: PDF EXTRACTION TESTS
# ==========================================

EXPECTED_FIRST_ROWS = {
    17: {"water": "COMOX LAKE", "mu": "1-6", "regs_contains": "No wild rainbow trout"},
    19: {"water": "LITTLE QUALICUM RIVER", "mu": "1-6", "regs_contains": "Fly fishing only"},
    24: {"water": "ALICE LAKE", "mu": "2-7", "regs_contains": "No powered boats"},
    25: {"water": "COQUITLAM RIVER", "mu": "2-8", "regs_contains": "Hatchery trout"},
    26: {"water": "KOKOMO LAKE", "mu": "2-5", "regs_contains": "Electric motor only"},
    28: {"water": "SQUAMISH RIVER", "regs_contains": "Bait ban"},
    60: {"water": "TATSATUA CREEK", "regs_contains": "No Fishing"},
}

EXPECTED_LAST_ROWS = {
    17: {"water": "FICKLE LAKE", "regs_contains": "No trout over 50 cm"},
    19: {"water": "PROVOST DAM", "mu": "1-5", "regs_contains": "No powered boats"},
    24: {"water": "COQUIHALLA RIVER", "mu": "2-17", "regs_contains": "steelhead"},
    25: {"water": "KLEIN LAKE", "mu": "2-5", "regs_contains": "Electric motor only"},
    26: {"water": "NANTON LAKE", "mu": "2-12", "regs_contains": "Wild trout/char"},
    28: {"water": "WOLF LAKE", "mu": "2-19", "regs_contains": "No powered boats"},
    60: {"water": "ZYMOETZ (Copper) RIVER", "mu": "6-9", "regs_contains": "McDonell Lake"},
}

EXPECTED_SYMBOLS = {
    19: [
        {"water": "OYSTER RIVER", "has": ["Stocked"]},
        {"water": "PALLANT CREEK", "has": ["Incl. Tribs", "Classified"]},
        {"water": "LIZARD LAKE", "has": ["Stocked"]},
    ],
    24: [
        {"water": "ALICE LAKE", "has": ["Stocked"]},
        {"water": "ALOUETTE RIVER", "has": ["Incl. Tribs"]},
        {"water": "ALPHA LAKE", "has": ["Stocked"]},
        {"water": "COQUIHALLA RIVER", "has": ["Incl. Tribs"]},
    ],
    28: [
        {"water": "SQUAMISH RIVER", "has": ["Incl. Tribs"]},
    ],
    60: [
        {"water": "ZYMOETZ (Copper) RIVER", "has": ["Incl. Tribs", "Classified"]},
    ],
}

# Build expected regions for all pages in each range
EXPECTED_REGIONS = []
for page in range(16, 22):
    EXPECTED_REGIONS.append((page, "REGION 1 - Vancouver Island"))
for page in range(24, 29):
    EXPECTED_REGIONS.append((page, "REGION 2 - Lower Mainland"))
for page in range(31, 35):
    EXPECTED_REGIONS.append((page, "REGION 3 - Thompson-Nicola"))
for page in range(37, 43):
    EXPECTED_REGIONS.append((page, "REGION 4 - Kootenay"))
for page in range(49, 54):
    EXPECTED_REGIONS.append((page, "REGION 5 - Cariboo"))
for page in range(56, 61):
    EXPECTED_REGIONS.append((page, "REGION 6 - Skeena"))
for page in range(65, 68):
    EXPECTED_REGIONS.append((page, "REGION 7A - Omineca"))
for page in range(71, 73):
    EXPECTED_REGIONS.append((page, "REGION 7B - Peace"))
for page in range(75, 78):
    EXPECTED_REGIONS.append((page, "REGION 8 - Okanagan"))

# Derive list of pages with tables from EXPECTED_REGIONS
PAGES_WITH_TABLES = [page_num for page_num, _ in EXPECTED_REGIONS]

@pytest.mark.parametrize("page_num, expected_region", EXPECTED_REGIONS)
def test_region_metadata_extraction(parser, pdf, page_num, expected_region):
    """Verify that region metadata is correctly extracted for each region."""
    result = parser.extract_rows(pdf.pages[page_num - 1])
    metadata = result.metadata
    
    assert metadata.region is not None, f"Page {page_num}: Region is None"
    # Use exact equality to ensure we're not capturing extra text
    assert metadata.region == expected_region, \
        f"Page {page_num}: Expected '{expected_region}' but got '{metadata.region}'"

@pytest.mark.parametrize("page_num", PAGES_WITH_TABLES)
def test_row_extraction_integrity(parser, pdf, page_num):
    """Verify each page returns data and filters out headers."""
    raw_page = pdf.pages[page_num - 1]
    result = parser.extract_rows(raw_page)
    rows = result.rows
    
    assert len(rows) > 0, f"Page {page_num}: No rows extracted."
    
    for row in rows:
        assert "WATER BODY" not in row.water.upper()
        # Raw data has: water, mu, raw_regs, symbols, page, image
        assert hasattr(row, 'water') and hasattr(row, 'mu') and hasattr(row, 'raw_regs')
        assert hasattr(row, 'symbols') and hasattr(row, 'page') and hasattr(row, 'image')
        assert isinstance(row.symbols, list)
        # raw_regs should be a string (not parsed yet)
        assert isinstance(row.raw_regs, str), f"raw_regs for {row.water} is not a string"

@pytest.mark.parametrize("page_num, expected", EXPECTED_FIRST_ROWS.items())
def test_first_row_content(parser, pdf, page_num, expected):
    """Verify the very first data row matches expectations (No-skip logic)."""
    result = parser.extract_rows(pdf.pages[page_num - 1])
    rows = result.rows
    actual = rows[0]
    
    assert expected["water"] in actual.water, f"Page {page_num} first row mismatch."
    if "mu" in expected:
        assert expected["mu"] in actual.mu
    # Use helper to search raw regulation text
    assert raw_regs_contain(actual, expected["regs_contains"]), \
        f"Expected '{expected['regs_contains']}' in {actual.raw_regs}"

@pytest.mark.parametrize("page_num, expected", EXPECTED_LAST_ROWS.items())
def test_last_row_content(parser, pdf, page_num, expected):
    """Verify the last row of the page is captured correctly."""
    result = parser.extract_rows(pdf.pages[page_num - 1])
    rows = result.rows
    actual = rows[-1]
    
    assert expected["water"] in actual.water, f"Page {page_num} last row mismatch."
    if "mu" in expected:
        assert expected["mu"] in actual.mu
    # Use helper to search raw regulation text
    assert raw_regs_contain(actual, expected["regs_contains"]), \
        f"Expected '{expected['regs_contains']}' in {actual.raw_regs}"

@pytest.mark.parametrize("page_num, symbol_checks", EXPECTED_SYMBOLS.items())
def test_symbol_logic(parser, pdf, page_num, symbol_checks):
    """Test Stocked, Classified, and Incl. Tribs detection."""
    result = parser.extract_rows(pdf.pages[page_num - 1])
    rows = result.rows
    
    for check in symbol_checks:
        matching_rows = [r for r in rows if check["water"] in r.water]
        assert len(matching_rows) > 0, f"Could not find {check['water']} on page {page_num}"
        
        actual_symbols = matching_rows[0].symbols
        for sym in check["has"]:
            assert sym in actual_symbols, f"{check['water']} missing symbol {sym}. Got: {actual_symbols}"
            

def test_page_metadata_extraction(parser, pdf):
    """Test that page metadata (Region/Number) is extracted correctly."""
    # Test Page 37 (Columbia River)
    page_37 = parser.extract_rows(pdf.pages[36])
    assert page_37.metadata.page_number == 37
    assert "REGION 4 - Kootenay" in page_37.metadata.region

    # Test Page 17 (Vancouver Island)
    page_17 = parser.extract_rows(pdf.pages[16])
    assert page_17.metadata.page_number == 17
    assert "REGION 1 - Vancouver Island" in page_17.metadata.region

def test_columbia_river_multiple_mus(parser, pdf):
    """
    Test specifically for COLUMBIA RIVER on Page 37.
    It lists multiple MUs: 4-8, 4-15, 4-26, 4-34, 4-38.
    The parser must return these as a list.
    """
    # Page 37 is index 36
    page = pdf.pages[36]
    result = parser.extract_rows(page)
    rows = result.rows
    
    # Find the Columbia River row
    columbia = next((r for r in rows if "COLUMBIA RIVER" in r.water), None)
    
    assert columbia is not None, "Could not find COLUMBIA RIVER on page 37"
    
    # Verify 'mu' is a list
    assert isinstance(columbia.mu, list), f"Expected list for 'mu', got {type(columbia.mu)}"
    
    # Verify exact MUs are present
    expected_mus = ["4-8", "4-15", "4-26", "4-34", "4-38"]
    
    # Check that all expected MUs are in the extracted list
    for mu in expected_mus:
        assert mu in columbia.mu, f"Missing MU {mu} in extracted list: {columbia.mu}"
        
    # Optional: Verify it didn't grab extra garbage
    assert len(columbia.mu) == len(expected_mus), \
        f"Extracted unexpected MUs. Got: {columbia.mu}, Expected: {expected_mus}"

def test_management_unit_lookbehind(parser, pdf):
    """Verify that MUs in notes like '(also in M.U. 5-15)' are NOT in the MU column."""
    page = pdf.pages[16]
    result = parser.extract_rows(page)
    rows = result.rows
    elk = next(r for r in rows if "ELK RIVER" in r.water)
    
    # Check list membership
    assert "1-9" in elk.mu
    assert "5-15" not in elk.mu

def test_parsing_separation(parser, pdf):
    """
    Verify that raw regulation text is captured for waterbodies.
    This test now checks raw data extraction rather than parsing logic.
    """
    page = pdf.pages[16]
    result = parser.extract_rows(page)
    rows = result.rows
    copper = next(r for r in rows if "COPPER CREEK" in r.water)
    
    # Check that we have raw regulation text
    assert copper.raw_regs, "Expected raw regulation text for Copper Creek"
    
    # Check specifically for regulation content in raw text
    assert raw_regs_contain(copper, "Bait ban"), \
        f"Expected 'Bait ban' in raw regulations for Copper Creek"

def test_within_bbox_buffer_check(parser, pdf):
    """Specifically check for CRAIGFLOWER CREEK which fails if buffers are missing."""
    page = pdf.pages[16]
    result = parser.extract_rows(page)
    rows = result.rows
    craig = [r for r in rows if "CRAIGFLOWER" in r.water]
    assert len(craig) > 0, "CRAIGFLOWER CREEK missing. within_bbox might be too strict."
    assert "1-1" in craig[0].mu

def test_canim_river_specific_logic(parser, pdf):
    """Verify Canim River on page 31 retains bracketed notes in water body but excludes from MU."""
    page = pdf.pages[30]
    result = parser.extract_rows(page)
    rows = result.rows
    
    canim = next((r for r in rows if "CANIM RIVER" in r.water), None)
    assert canim is not None, "CANIM RIVER not found on page 31"
    
    assert "3-46" in canim.mu

    assert "5-15" not in canim.mu
    assert "(also in M.U. 5-15)" in canim.water
    assert raw_regs_contain(canim, "catch and release")

TOTAL_PAGES = 88 
EMPTY_PAGES = [
    p for p in range(1, TOTAL_PAGES + 1) 
    if p not in PAGES_WITH_TABLES and p != 6
]

@pytest.mark.parametrize("page_num", EMPTY_PAGES)
def test_no_false_positives_on_non_table_pages(parser, pdf, page_num):
    """Verify that pages not in the known list do not return data."""
    raw_page = pdf.pages[page_num - 1]
    result = parser.extract_rows(raw_page)
    rows = result.rows
    assert len(rows) == 0, f"Page {page_num} detected rows but should be empty."


# ==========================================
#      PART 2: REGULATION PARSER TESTS
# ==========================================

# Each tuple is (description, input_str, expected_list)
# ==========================================
#      PART 2: REGULATION PARSER TESTS
# ==========================================

# Each tuple is (description, input_str, expected_list)
TEST_CASES = [
    (
        "Coquihalla River",
        "No Fishing upstream of the northern entrance to the upper most railway tunnel, Nov 1-June 30 (see map on page 24)\nFly fishing only; bait ban upstream of the northern entrance to the upper most railway tunnel, Jul 1-Oct 31\nNo Fishing downstream of the southern entrance to the lower most railway tunnel, Apr 1-Oct 31\nNo Fishing at Othello Tunnels from the northern entrance to the upper most railway tunnel to the southern entrance of the lower most tunnel;\napproximately 700 m length\nTrout/char (including steelhead) catch and release, bait ban, downstream of the southern entrance to the lower most railway tunnel, Nov\n1-Mar 31",
        [
            "No Fishing upstream of the northern entrance to the upper most railway tunnel, Nov 1-June 30 (see map on page 24)",
            "Fly fishing only",
            "Bait ban upstream of the northern entrance to the upper most railway tunnel, Jul 1-Oct 31",
            "No Fishing downstream of the southern entrance to the lower most railway tunnel, Apr 1-Oct 31",
            "No Fishing at Othello Tunnels from the northern entrance to the upper most railway tunnel to the southern entrance of the lower most tunnel, approximately 700 m length",
            "Trout/char (including steelhead) catch and release, bait ban, downstream of the southern entrance to the lower most railway tunnel, Nov 1-Mar 31"
        ]
    ),
    (
        "Cultus Lake",
        "No wild trout over 50 cm, 1 bull trout over 60 cm",
        [
            "No wild trout over 50 cm", 
            "1 bull trout over 60 cm"
        ]
    ),
    (
        "PEND D’OREILLE RIVER",
        "EXEMPT from single barbless hooks\nEXEMPT from the Apr 1-June 14 closure\nBull trout catch and release, walleye daily quota = unlimited\nNorthern pike daily quota = unlimited, yellow perch daily quota = unlimited, bass daily quota = unlimited",
        [
            "EXEMPT from single barbless hooks",
            "EXEMPT from the Apr 1-June 14 closure",
            "Bull trout catch and release", 
            "Walleye daily quota = unlimited",
            "Northern pike daily quota = unlimited", 
            "Yellow perch daily quota = unlimited", 
            "Bass daily quota = unlimited"
        ]
    ),
    (
        "CLEARWATER RIVER",
        "Downstream of old Clearwater Bridge exempt from spring closure, except No Fishing May 1-June 30\nBait ban (a) from Falls Creek to Mahood River, all year, and (b) from Mahood River to North Thompson River, Sept 1-July 31\nNo angling from powered boats downstream of Falls Creek\nRainbow trout daily quota = 2 (none over 35 cm), char catch and release",
        [
            "Downstream of old Clearwater Bridge exempt from spring closure, except No Fishing May 1-June 30",
            "Bait ban (a) from Falls Creek to Mahood River, all year, and (b) from Mahood River to North Thompson River, Sept 1-July 31",
            "No angling from powered boats downstream of Falls Creek",
            "Rainbow trout daily quota = 2 (none over 35 cm)",
            "Char catch and release"
        ]
    ),
    (
        "Alice Lake",
        "Bait ban, single barbless hook, no trout over 50 cm",
        [
            "Bait ban",
            "Single barbless hook",
            "No trout over 50 cm"
        ]
    ),
    (
        "Anderson Lake",
        "Artificial fly only, bait ban, single barbless hook\nTrout and kokanee catch and release [Includes Tributaries] Unnamed lake in the Walbran Creek Watershed approximately 7 km west/southwest of Mt.\nWalbran",
        [
            "Artificial fly only",
            "Bait ban",
            "Single barbless hook",
            "Trout and kokanee catch and release [Includes Tributaries]",
            "Unnamed lake in the Walbran Creek Watershed approximately 7 km west/southwest of Mt. Walbran"
        ]
    ),
    (
        "Buttle Lake tributaries",
        "Fly fishing only; except Thelwood Creek is closed all year",
        [
            "Fly fishing only",
            "Except Thelwood Creek is closed all year"
        ]
    ),
    (
        "Fuller Lake",
        "Smallmouth bass daily quota = 4; electric motor only - max 7.5 kW; wheelchair accessible fishing platform is located in Fuller Lake Park",
        [
            "Smallmouth bass daily quota = 4",
            "Electric motor only - max 7.5 kW",
            "Wheelchair accessible fishing platform is located in Fuller Lake Park"
        ]
    ),
    (
        "Great Central Lake",
        "No Fishing Jan 1-Apr 30, from the dam to fishing boundary signs approximately 50 m upstream (southwest) of the Ash Main Bridge\n Single barbless hook, no wild rainbow trout over 50 cm",
        [
            "No Fishing Jan 1-Apr 30, from the dam to fishing boundary signs approximately 50 m upstream (southwest) of the Ash Main Bridge",
            "Single barbless hook",
            "No wild rainbow trout over 50 cm"
        ]
    ),
    (
        "Gunflint Lake",
        "Trout catch and release; bait ban, single barbless hook; electric motor only - max 7.5 kW",
        [
            "Trout catch and release",
            "Bait ban",
            "Single barbless hook",
            "Electric motor only - max 7.5 kW"
        ]
    ),
    (
        "Little Qualicum River",
        "No Fishing July 15-Aug 31 [Includes Tributaries] , No Fishing - All tributaries\nNo Fishing from the falls in Little Qualicum Falls Provincial Park downstream to the hatchery fence, Dec 1-May 31\nThe standard 100 m closure around a fish rearing facility has been reduced to a no fishing area from the hatchery fence to signs\napproximately 35 m downstream\nFly fishing only, Sept 1-Nov 30 (where open)",
        [
            "No Fishing July 15-Aug 31 [Includes Tributaries]",
            "No Fishing - All tributaries",
            "No Fishing from the falls in Little Qualicum Falls Provincial Park downstream to the hatchery fence, Dec 1-May 31",
            "The standard 100 m closure around a fish rearing facility has been reduced to a no fishing area from the hatchery fence to signs approximately 35 m downstream",
            "Fly fishing only, Sept 1-Nov 30 (where open)"
        ]
    ),
    (
        "Columbia River",
        "No Fishing from Revelstoke Dam downstream to Hwy 1 bridge in Revelstoke\nNo Fishing from a line between the old Robson Ferry landing and a sign on the south river bank, downstream approximately 950 m to the\nCPR Bridge, Mar 1-June 30\nWhere angling is permitted: EXEMPT from the regional Nov 1-Mar 31 trout/char catch and release and the regional Apr 1-June 14 closure\nBass daily quota = unlimited\nKokanee daily quota = 15 from Keenleyside Dam to a line between the old Robson Ferry landing and a sign on the south river bank\nWalleye daily quota = 16 from Keenleyside Dam to the Washington state border\nFrom Keenleyside Dam downstream to the Washington state border and connected reaches: the Kootenay River (Columbia River\nconfluence to Brilliant Dam) and the Pend d’Oreille River (Columbia River confluence to Waneta Dam): Northern pike daily quota =\nunlimited and bass daily quota = unlimited\nBurbot catch and release\nSpeed restriction (10 km/h) from Mud Lake to Columbia Lake, no power boats in wetlands, no towing and engine power restriction -\n15 kW (20 hp), in main channel from Fairmont to Donald\nSee Upper Arrow Lake for the portion of the Columbia River which may be found downstream of the Hwy 1 bridge in Revelstoke\n(depending on reservoir level)",
        [
            "No Fishing from Revelstoke Dam downstream to Hwy 1 bridge in Revelstoke",
            "No Fishing from a line between the old Robson Ferry landing and a sign on the south river bank, downstream approximately 950 m to the CPR Bridge, Mar 1-June 30",
            "Where angling is permitted: EXEMPT from the regional Nov 1-Mar 31 trout/char catch and release and the regional Apr 1-June 14 closure",
            "Bass daily quota = unlimited",
            "Kokanee daily quota = 15 from Keenleyside Dam to a line between the old Robson Ferry landing and a sign on the south river bank",
            "Walleye daily quota = 16 from Keenleyside Dam to the Washington state border",
            "From Keenleyside Dam downstream to the Washington state border and connected reaches: the Kootenay River (Columbia River confluence to Brilliant Dam) and the Pend d’Oreille River (Columbia River confluence to Waneta Dam): Northern pike daily quota = unlimited and bass daily quota = unlimited",
            "Burbot catch and release",
            "Speed restriction (10 km/h) from Mud Lake to Columbia Lake", 
            "No power boats in wetlands", 
            "No towing and engine power restriction - 15 kW (20 hp), in main channel from Fairmont to Donald",
            "See Upper Arrow Lake for the portion of the Columbia River which may be found downstream of the Hwy 1 bridge in Revelstoke (depending on reservoir level)"
        ]
    ),
    (
        "Elk Lake",
        "No Ice Fishing; trout daily quota = 1 (none under 50 cm); single barbless hook; electric motor only - max 7.5 kW",
        [
            "No Ice Fishing",
            "Trout daily quota = 1 (none under 50 cm)",
            "Single barbless hook",
            "Electric motor only - max 7.5 kW"
        ]
    ),
    (
        "Somass River",
        "No Fishing between the tidal boundary at Papermill Dam to boundary signs approximately 1.0 km upstream (Falls Road Gravel Pit and the\nsouthern most end of Collins Farm/Arrow Vale Campground on Hector Road), Aug 25-Nov 15\nBait ban, June 1-Aug 24\nEngine power restriction 7.5 kW (10 hp)",
        [
            "No Fishing between the tidal boundary at Papermill Dam to boundary signs approximately 1.0 km upstream (Falls Road Gravel Pit and the southern most end of Collins Farm/Arrow Vale Campground on Hector Road), Aug 25-Nov 15",
            "Bait ban, June 1-Aug 24",
            "Engine power restriction 7.5 kW (10 hp)"
        ]
    ),
    (
        "White River",
        "No Fishing between fishing boundary signs at the salmon viewing pool\nNo Fishing upstream of the Sayward Road Bridge crossing, Nov 1-Apr 30",
        [
            "No Fishing between fishing boundary signs at the salmon viewing pool",
            "No Fishing upstream of the Sayward Road Bridge crossing, Nov 1-Apr 30"
        ]
    ),
    (
        "Alouette River",
        "No Fishing upstream of the fishing boundary signs located at 49° 14.790'N and 122° 32.080'W, near the southern boundary (chain-link\nfence) of the Alouette River Management Society\nNo Fishing upstream of 216th Street (including North Alouette River [Includes Tributaries] ), May 1-June 30; no powered boats on mainstem",
        [
            "No Fishing upstream of the fishing boundary signs located at 49° 14.790'N and 122° 32.080'W, near the southern boundary (chain-link fence) of the Alouette River Management Society",
            "No Fishing upstream of 216th Street (including North Alouette River [Includes Tributaries] ), May 1-June 30",
            "No powered boats on mainstem"
        ]
    ),
    (
        "Chilliwack / Vedder Rivers",
        "No Fishing upstream from a line between two fishing boundary signs on either side of the Chilliwack River 100 m downstream of the\nconfluence of the Chilliwack River and Slesse Creek\nNo Fishing downstream of a line between two fishing boundary signs on either side of the Chilliwack River 100m downstream of the\nconfluence of the Chilliwack River and Slesse Creek to Tamihi Rapids Bridge, Apr 1-June 30\nNo Fishing downstream of Tamihi Rapids Bridge to Vedder Crossing Bridge, May 1-June 30\nDownstream of Vedder Crossing Bridge: (a) fly fishing only, bait ban, hatchery rainbow trout catch and release (50 cm or less), and hatch-\nery cutthroat catch and release, May 1-31; (b) No Fishing June 1-30; (c) hatchery rainbow trout of any length 50 cm or less: daily quota =\n4, July 1-Apr 30",
        [
            "No Fishing upstream from a line between two fishing boundary signs on either side of the Chilliwack River 100 m downstream of the confluence of the Chilliwack River and Slesse Creek",
            "No Fishing downstream of a line between two fishing boundary signs on either side of the Chilliwack River 100m downstream of the confluence of the Chilliwack River and Slesse Creek to Tamihi Rapids Bridge, Apr 1-June 30",
            "No Fishing downstream of Tamihi Rapids Bridge to Vedder Crossing Bridge, May 1-June 30",
            "Downstream of Vedder Crossing Bridge: (a) fly fishing only, bait ban, hatchery rainbow trout catch and release (50 cm or less), and hatchery cutthroat catch and release, May 1-31; (b) No Fishing June 1-30; (c) hatchery rainbow trout of any length 50 cm or less: daily quota = 4, July 1-Apr 30"
        ]
    ),
    (
        "Adams River",
        "No Fishing from Sept 1-Oct 31 between fishing boundary signs in the vicinity of the public salmon viewing platforms in Tsutswecew\nProvincial Park\nRainbow trout and char catch and release; bait ban; no powered boats",
        [
            "No Fishing from Sept 1-Oct 31 between fishing boundary signs in the vicinity of the public salmon viewing platforms in Tsutswecew Provincial Park",
            "Rainbow trout and char catch and release",
            "Bait ban",
            "No powered boats"
        ]
    ),
    (
        "Thompson River",
        "No Fishing Oct 1-May 31\nTrout/char daily quota = 2 (none under 35 cm)\nAdditional opening from the CNR Bridge downstream of Deadman River to CNR Bridge upstream of Bonaparte River, May 1-31; trout/char\ncatch and release and artificial fly only, May 1-31\nUpstream of boundary signs 1 km downstream of Martel: bait ban\nNo angling from boats\nDownstream of signs at Kamloops Lake: Class II water Oct 1-Dec 31 and Steelhead Stamp mandatory Oct 1-Dec 31 (when open)",
        [
            "No Fishing Oct 1-May 31",
            "Trout/char daily quota = 2 (none under 35 cm)",
            "Additional opening from the CNR Bridge downstream of Deadman River to CNR Bridge upstream of Bonaparte River, May 1-31; trout/char catch and release and artificial fly only, May 1-31",
            "Upstream of boundary signs 1 km downstream of Martel: bait ban",
            "No angling from boats",
            "Downstream of signs at Kamloops Lake: Class II water Oct 1-Dec 31 and Steelhead Stamp mandatory Oct 1-Dec 31 (when open)"
        ]
    ),
    (
        "Slocan River",
        "No Fishing July 15-Aug 31 from 12 pm to midnight (EXCEPT Koch Creek [Includes Tributaries] upstream of falls located approximately 700 m downstream of\nthe Little Slocan Forest Service Road Koch Creek bridge crossing and Little Slocan Lake’s tributaries; see Lemon Creek)\nBait ban (where open), June 15-Oct 31\nTrout/char catch and release (EXCEPT Koch Creek [Includes Tributaries] upstream of falls and Little Slocan Lake’s tributaries)",
        [
            "No Fishing July 15-Aug 31 from 12 pm to midnight (EXCEPT Koch Creek [Includes Tributaries], upstream of falls located approximately 700 m downstream of the Little Slocan Forest Service Road Koch Creek bridge crossing and Little Slocan Lake’s tributaries; see Lemon Creek)",
            "Bait ban (where open), June 15-Oct 31",
            "Trout/char catch and release (EXCEPT Koch Creek [Includes Tributaries], upstream of falls and Little Slocan Lake’s tributaries)"
        ]
    ),
    (
        "Qualicum River",
        "No Fishing downstream of boundary signs located approximately 100 m downstream of the hatchery counting fence\nNo Fishing from the upper hatchery weir (located 125 m downstream of the E&N Trestle) to boundary sign: located approximately 100 m\ndownstream of the hatchery counting fence, Aug 15-Oct 15. Refer to page 6 for updated regulations related to non-tidal salmon\nNo Fishing tributaries\nSingle barbless hook, no hooks greater than 15 mm from point to shank\nExempt from July 15-Aug 31 summer closure, wheelchair accessible fishing platform is located at the hatchery",
        [
            "No Fishing downstream of boundary signs located approximately 100 m downstream of the hatchery counting fence",
            "No Fishing from the upper hatchery weir (located 125 m downstream of the E&N Trestle) to boundary sign: located approximately 100 m downstream of the hatchery counting fence, Aug 15-Oct 15",
            "Refer to page 6 for updated regulations related to non-tidal salmon",
            "No Fishing tributaries",
            "Single barbless hook", 
            "No hooks greater than 15 mm from point to shank",
            "Exempt from July 15-Aug 31 summer closure", 
            "Wheelchair accessible fishing platform is located at the hatchery"
        ]
    ),
    (
        "atnarko/bella coola rivers",
        "No Fishing upstream of Tweedsmuir Provincial Park plus Tenas Lake, Apr 1-June 30\nNo Fishing from Tenas Lake to fishing boundary signs near Atnarko Park campsite\nTrout/char daily quota = 1 (none under 25 cm and all cutthroat trout catch and release) EXCEPT: on Bella Coola R. MAINSTEM ONLY, trout/\nchar daily quota = 2, of which only one may be a trout (cutthroat or rainbow) and none may be under 25 cm, no cutthroat may be over 33\ncm, and no rainbow may be over 50 cm), Apr 1-May 31 ONLY, EXCEPT: char catch and release (on TRIBUTARIES ONLY), Sept 1-May 31\nBait ban downstream of eastern boundary of Tweedsmuir Provincial Park, Sept 1- May 15\nNo angling from powered boats on mainstems of Atnarko River and Bella Coola River\nNo powered boats on Atnarko River, from Goat Creek to the confluence with Talchako River\nNo Fishing for steelhead\nClass II water downstream of Young Creek, Mar 1-May 31. NOTE: Classified Waters Licence or Steelhead Stamp not required until\nreopened to steelhead fishing",
        [
            "No Fishing upstream of Tweedsmuir Provincial Park plus Tenas Lake, Apr 1-June 30",
            "No Fishing from Tenas Lake to fishing boundary signs near Atnarko Park campsite",
            "Trout/char daily quota = 1 (none under 25 cm and all cutthroat trout catch and release) EXCEPT: on Bella Coola R. MAINSTEM ONLY, trout/char daily quota = 2, of which only one may be a trout (cutthroat or rainbow) and none may be under 25 cm, no cutthroat may be over 33 cm, and no rainbow may be over 50 cm), Apr 1-May 31 ONLY, EXCEPT: char catch and release (on TRIBUTARIES ONLY), Sept 1-May 31",
            "Bait ban downstream of eastern boundary of Tweedsmuir Provincial Park, Sept 1- May 15",
            "No angling from powered boats on mainstems of Atnarko River and Bella Coola River",
            "No powered boats on Atnarko River, from Goat Creek to the confluence with Talchako River",
            "No Fishing for steelhead",
            "Class II water downstream of Young Creek, Mar 1-May 31. NOTE: Classified Waters Licence or Steelhead Stamp not required until reopened to steelhead fishing"
        ],
    ),
    (
        "WESTwood Lake",
        "Smallmouth bass daily quota = 4\nwheelchair accessible fishing platform is located in Westwood Lake Park",
        [
            "Smallmouth bass daily quota = 4",
            "Wheelchair accessible fishing platform is located in Westwood Lake Park"
        ]
    ),
    (
        "Englishman River",
        "No Fishing July 15-Aug 31 [Includes Tributaries]\nNo Fishing from lower falls in Englishman River Park to signs approximately 100 m downstream\nNo Fishing downstream of the lower falls in Englishman River Falls Provincial Park to the Top Bridge crossing at the end of\nAllsbrook Road [Includes Tributaries] , Dec 1-May 31",
        [
            "No Fishing July 15-Aug 31 [Includes Tributaries]",
            "No Fishing from lower falls in Englishman River Park to signs approximately 100 m downstream",
            "No Fishing downstream of the lower falls in Englishman River Falls Provincial Park to the Top Bridge crossing at the end of Allsbrook Road [Includes Tributaries] , Dec 1-May 31"
        ]
    ),
    (
        "Standard Closure (Control Case)",
        "No Fishing upstream of the bridge, Nov 1-June 30",
        [
            "No Fishing upstream of the bridge, Nov 1-June 30"
        ]
    ),
    (
        "Multiple Sentences (Should split)",
        "Fly fishing only; bait ban upstream of the northern entrance",
        [
            "Fly fishing only",
            "Bait ban upstream of the northern entrance"
        ]
    )
        
        
        
]


@pytest.mark.parametrize("desc, input_text, expected", TEST_CASES)
def test_reg_parsing(desc, input_text, expected):
    """
    Runs the regex parser on the input text.
    Checks if the resulting 'details' list matches expected output.
    """
    # Access the nested RegParser class
    parsed = FishingSynopsisParser.RegParser.parse_reg(input_text)
    
    # Extract just the details for comparison
    # Note: parse_reg now handles all cleaning/merging internally
    actual_details = [p['details'] for p in parsed]
    
    # Normalize (strip whitespace) for comparison
    expected_norm = [s.strip() for s in expected]
    actual_norm = [s.strip() for s in actual_details]
    
    # Debug output
    if actual_norm != expected_norm:
        print(f"\n\n{'='*80}")
        print(f"FAILED: {desc}")
        print(f"{'='*80}")
        print(f"\nExpected {len(expected_norm)} items:")
        for i, item in enumerate(expected_norm, 1):
            print(f"  {i}. {item}")
        print(f"\nActual {len(actual_norm)} items:")
        for i, item in enumerate(actual_norm, 1):
            print(f"  {i}. {item}")
        print(f"{'='*80}\n")
    
    assert actual_norm == expected_norm, f"Failed on: {desc}"


def test_region_name_normalization():
    """Test that region names are normalized to consistent format."""
    parser = FishingSynopsisParser()
    
    # Test various input formats
    test_cases = [
        ("REGION 1 - VANCOUVER ISLAND", "REGION 1 - Vancouver Island"),
        ("region 1 - vancouver island", "REGION 1 - Vancouver Island"),
        ("Region 1 - Vancouver Island", "REGION 1 - Vancouver Island"),
        ("REGION 2 - LOWER MAINLAND", "REGION 2 - Lower Mainland"),
        ("REGION 3 - thompson-nicola", "REGION 3 - Thompson-Nicola"),
        ("REGION 4 - kootenay", "REGION 4 - Kootenay"),
        ("REGION 7A - omineca-peace", "REGION 7A - Omineca-Peace"),
    ]
    
    for input_name, expected_output in test_cases:
        actual = parser._normalize_region_name(input_name)
        assert actual == expected_output, f"Failed: {input_name!r} -> {actual!r} (expected {expected_output!r})"
