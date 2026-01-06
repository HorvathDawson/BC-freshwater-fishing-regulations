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
def regs_contain(row, text):
    """
    Helper to search for text within the parsed list of regulation dictionaries.
    Returns True if 'text' is found in the 'details' of any item in row['regs'].
    """
    if not row or 'regs' not in row:
        return False
    return any(text in item['details'] for item in row['regs'])

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

# Comprehensive list of pages containing regulation tables
PAGES_WITH_TABLES = [
    *range(16, 22),  # Region 1
    *range(24, 29),  # Region 2
    *range(31, 35),  # Region 3
    *range(37, 43),  # Region 4
    *range(49, 54),  # Region 5
    *range(56, 61),  # Region 6
    *range(65, 68),  # Region 7a
    *range(71, 73),  # Region 7b
    *range(75, 78)   # Region 8
]

@pytest.mark.parametrize("page_num", PAGES_WITH_TABLES)
def test_row_extraction_integrity(parser, pdf, page_num):
    """Verify each page returns data and filters out headers."""
    raw_page = pdf.pages[page_num - 1]
    rows = parser.extract_rows(raw_page)
    
    assert len(rows) > 0, f"Page {page_num}: No rows extracted."
    
    for row in rows:
        assert "WATER BODY" not in row["water"].upper()
        assert all(k in row for k in ["water", "mu", "regs", "symbols"])
        assert isinstance(row["symbols"], list)
        # NEW: Verify regs is a list of dicts, not a string
        assert isinstance(row["regs"], list), f"Regs for {row['water']} is not a list"
        if row["regs"]:
            assert isinstance(row["regs"][0], dict)
            assert "type" in row["regs"][0]

@pytest.mark.parametrize("page_num, expected", EXPECTED_FIRST_ROWS.items())
def test_first_row_content(parser, pdf, page_num, expected):
    """Verify the very first data row matches expectations (No-skip logic)."""
    rows = parser.extract_rows(pdf.pages[page_num - 1])
    actual = rows[0]
    
    assert expected["water"] in actual["water"], f"Page {page_num} first row mismatch."
    if "mu" in expected:
        assert expected["mu"] in actual["mu"]
    # UPDATED: Use helper to search list of dicts
    assert regs_contain(actual, expected["regs_contains"]), \
        f"Expected '{expected['regs_contains']}' in {actual['regs']}"

@pytest.mark.parametrize("page_num, expected", EXPECTED_LAST_ROWS.items())
def test_last_row_content(parser, pdf, page_num, expected):
    """Verify the last row of the page is captured correctly."""
    rows = parser.extract_rows(pdf.pages[page_num - 1])
    actual = rows[-1]
    
    assert expected["water"] in actual["water"], f"Page {page_num} last row mismatch."
    if "mu" in expected:
        assert expected["mu"] in actual["mu"]
    # UPDATED: Use helper to search list of dicts
    assert regs_contain(actual, expected["regs_contains"]), \
        f"Expected '{expected['regs_contains']}' in {actual['regs']}"

@pytest.mark.parametrize("page_num, symbol_checks", EXPECTED_SYMBOLS.items())
def test_symbol_logic(parser, pdf, page_num, symbol_checks):
    """Test Stocked, Classified, and Incl. Tribs detection."""
    rows = parser.extract_rows(pdf.pages[page_num - 1])
    
    for check in symbol_checks:
        matching_rows = [r for r in rows if check["water"] in r["water"]]
        assert len(matching_rows) > 0, f"Could not find {check['water']} on page {page_num}"
        
        actual_symbols = matching_rows[0]["symbols"]
        for sym in check["has"]:
            assert sym in actual_symbols, f"{check['water']} missing symbol {sym}. Got: {actual_symbols}"
            

def test_columbia_river_multiple_mus(parser, pdf):
    """
    Test specifically for COLUMBIA RIVER on Page 37.
    It lists multiple MUs: 4-8, 4-15, 4-26, 4-34, 4-38.
    The parser must return these as a list.
    """
    # Page 37 is index 36
    page = pdf.pages[36]
    rows = parser.extract_rows(page)
    
    # Find the Columbia River row
    columbia = next((r for r in rows if "COLUMBIA RIVER" in r["water"]), None)
    
    assert columbia is not None, "Could not find COLUMBIA RIVER on page 37"
    
    # Verify 'mu' is a list
    assert isinstance(columbia["mu"], list), f"Expected list for 'mu', got {type(columbia['mu'])}"
    
    # Verify exact MUs are present
    expected_mus = ["4-8", "4-15", "4-26", "4-34", "4-38"]
    
    # Check that all expected MUs are in the extracted list
    for mu in expected_mus:
        assert mu in columbia["mu"], f"Missing MU {mu} in extracted list: {columbia['mu']}"
        
    # Optional: Verify it didn't grab extra garbage
    assert len(columbia["mu"]) == len(expected_mus), \
        f"Extracted unexpected MUs. Got: {columbia['mu']}, Expected: {expected_mus}"

def test_management_unit_lookbehind(parser, pdf):
    """Verify that MUs in notes like '(also in M.U. 5-15)' are NOT in the MU column."""
    page = pdf.pages[16]
    rows = parser.extract_rows(page)
    elk = next(r for r in rows if "ELK RIVER" in r["water"])
    
    # NEW: Check list membership
    assert "1-9" in elk["mu"]
    assert "5-15" not in elk["mu"]

def test_parsing_separation(parser, pdf):
    """
    Verify that the parser correctly splits multiple distinct regulations.
    Replacing 'test_newline_preservation' since the new logic strips newlines
    but separates items into a list.
    """
    page = pdf.pages[16]
    rows = parser.extract_rows(page)
    copper = next(r for r in rows if "COPPER CREEK" in r["water"])
    
    # Check that we found multiple distinct regulations (it has bait ban + classified)
    assert len(copper["regs"]) >= 2, \
        f"Expected multiple regulation items for Copper Creek, got: {len(copper['regs'])}"
    
    # Check specifically for one of them
    assert regs_contain(copper, "Bait ban")

def test_within_bbox_buffer_check(parser, pdf):
    """Specifically check for CRAIGFLOWER CREEK which fails if buffers are missing."""
    page = pdf.pages[16]
    rows = parser.extract_rows(page)
    craig = [r for r in rows if "CRAIGFLOWER" in r["water"]]
    assert len(craig) > 0, "CRAIGFLOWER CREEK missing. within_bbox might be too strict."
    assert "1-1" in craig[0]["mu"]

def test_canim_river_specific_logic(parser, pdf):
    """Verify Canim River on page 31 retains bracketed notes in water body but excludes from MU."""
    page = pdf.pages[30]
    rows = parser.extract_rows(page)
    
    canim = next((r for r in rows if "CANIM RIVER" in r["water"]), None)
    assert canim is not None, "CANIM RIVER not found on page 31"
    
    assert "3-46" in canim["mu"]
    assert "5-15" not in canim["mu"]
    assert "(also in M.U. 5-15)" in canim["water"]
    assert regs_contain(canim, "catch and release")

TOTAL_PAGES = 88 
EMPTY_PAGES = [
    p for p in range(1, TOTAL_PAGES + 1) 
    if p not in PAGES_WITH_TABLES and p != 6
]

@pytest.mark.parametrize("page_num", EMPTY_PAGES)
def test_no_false_positives_on_non_table_pages(parser, pdf, page_num):
    """Verify that pages not in the known list do not return data."""
    raw_page = pdf.pages[page_num - 1]
    rows = parser.extract_rows(raw_page)
    assert len(rows) == 0, f"Page {page_num} detected rows but should be empty."


# ==========================================
#      PART 2: REGULATION PARSER TESTS
# ==========================================

# Each tuple is (description, input_str, expected_list)
TEST_CASES = [
    (
        "Coquihalla: Species + Brackets + Quota (Should stay together)",
        "Trout/char (including steelhead) catch and release, bait ban, downstream of the southern entrance",
        [
            "Trout/char (including steelhead) catch and release, bait ban, downstream of the southern entrance"
        ]
    ),
    (
        "Cultus Lake: Adjective + Species (No Wild Trout)",
        "No wild trout over 50 cm, 1 bull trout over 60 cm",
        [
            "No wild trout over 50 cm", 
            "1 bull trout over 60 cm"
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
            "bait ban upstream of the northern entrance"
        ]
    ),
    (
        "Exemptions & Quotas: Should keep 'single barbless' and 'daily quota' together",
        "EXEMPT from single barbless hooks. Walleye daily quota = unlimited. Northern pike daily quota = unlimited",
        [
            "EXEMPT from single barbless hooks",
            "Walleye daily quota = unlimited",
            "Northern pike daily quota = unlimited"
        ]
    ),
    (
        "Alice Lake: 'No trout over 50 cm' should stay together",
        "Bait ban, single barbless hook, no trout over 50 cm",
        [
            "Bait ban",
            "single barbless hook",
            "no trout over 50 cm"
        ]
    ),
    (
        "Anderson Lake: 'Trout and kokanee' should stay together",
        "Artificial fly only, bait ban, single barbless hook. Trout and kokanee catch and release",
        [
            "Artificial fly only",
            "bait ban",
            "single barbless hook",
            "Trout and kokanee catch and release"
        ]
    ),
    (
        "Buttle Lake: 'is closed' should stay together",
        "Fly fishing only; except Thelwood Creek is closed all year",
        [
            "Fly fishing only",
            "except Thelwood Creek is closed all year"
        ]
    ),
    (
        "Fuller Lake: Smallmouth Bass should stay together",
        "Smallmouth bass daily quota = 4; electric motor only - max 7.5 kW; wheelchair accessible fishing platform is located in Fuller Lake Park",
        [
            "Smallmouth bass daily quota = 4",
            "electric motor only - max 7.5 kW",
            "wheelchair accessible fishing platform is located in Fuller Lake Park"
        ]
    ),
    (
        "Great Central Lake: 'no wild rainbow trout' chain should stay together",
        "Single barbless hook, no wild rainbow trout over 50 cm",
        [
            "Single barbless hook",
            "no wild rainbow trout over 50 cm"
        ]
    ),
    (
        "Gunflint Lake: Semicolon should separate 'catch and release' from 'bait ban'",
        "Trout catch and release; bait ban, single barbless hook; electric motor only - max 7.5 kW",
        [
            "Trout catch and release",
            "bait ban",
            "single barbless hook",
            "electric motor only - max 7.5 kW"
        ]
    ),
    (
        "Little Qualicum: Full Block Test",
        "No Fishing July 15-Aug 31 [Includes tributaries] , No Fishing - All tributaries. No Fishing from the falls in Little Qualicum Falls Provincial Park downstream to the hatchery fence, Dec 1-May 31. The standard 100 m closure around a fish rearing facility has been reduced to a no fishing area from the hatchery fence to signs approximately 35 m downstream. Fly fishing only, Sept 1-Nov 30 (where open)",
        [
            "No Fishing July 15-Aug 31 [Includes tributaries]",
            "No Fishing - All tributaries",
            "No Fishing from the falls in Little Qualicum Falls Provincial Park downstream to the hatchery fence, Dec 1-May 31",
            "The standard 100 m closure around a fish rearing facility has been reduced to a no fishing area from the hatchery fence to signs approximately 35 m downstream",
            "Fly fishing only, Sept 1-Nov 30 (where open)"
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
    
    assert actual_norm == expected_norm, f"Failed on: {desc}"