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
