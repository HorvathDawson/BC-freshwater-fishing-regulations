import pytest
import pdfplumber
import os
import re

# Import the class from your script file
from extract_synopsis import FishingSynopsisParser

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PDF_PATH = os.path.join(SCRIPT_DIR, "output", "fishing_synopsis.pdf")

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

# --- COMPREHENSIVE TEST DATA ---

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

# --- TESTS ---

# Comprehensive list of pages containing regulation tables, 
# organized by BC Freshwater Fishing Regions.
PAGES_WITH_TABLES = [
    # Region 1: Vancouver Island
    # Contains major water bodies like Cowichan and Comox
    *range(16, 22), 

    # Region 2: Lower Mainland
    # Includes Fraser River tributaries and Alouette Lake
    *range(24, 29), 

    # Region 3: Thompson
    # Includes Canim River and Shuswap area
    *range(31, 35), 

    # Region 4: Kootenay
    *range(37, 43), 

    # Region 5: Cariboo
    # Covers central interior lake districts
    *range(49, 54), 

    # Region 6: Skeena
    *range(56, 61), 

    # Region 7a: Omineca
    # Northern interior region
    *range(65, 68), 
    
    # Region 7b: Peace
    *range(71, 73),

    # Region 8: Okanagan/Peace
    *range(75, 78)  # Final block including page 77 exceptions
]

# Note: Pages not included (e.g., 22-23, 29-30) were identified as 
# full-page maps or non-tabular informational content.
@pytest.mark.parametrize("page_num", PAGES_WITH_TABLES)
def test_row_extraction_integrity(parser, pdf, page_num):
    """Verify each page returns data and filters out headers."""
    raw_page = pdf.pages[page_num - 1]
    rows = parser.extract_rows(raw_page)
    
    assert len(rows) > 0, f"Page {page_num}: No rows extracted."
    
    for row in rows:
        # Header filtering check
        assert "WATER BODY" not in row["water"].upper()
        # Key existence
        assert all(k in row for k in ["water", "mu", "regs", "symbols"])
        assert isinstance(row["symbols"], list)

@pytest.mark.parametrize("page_num, expected", EXPECTED_FIRST_ROWS.items())
def test_first_row_content(parser, pdf, page_num, expected):
    """Verify the very first data row matches expectations (No-skip logic)."""
    rows = parser.extract_rows(pdf.pages[page_num - 1])
    actual = rows[0]
    
    assert expected["water"] in actual["water"], f"Page {page_num} first row mismatch."
    if "mu" in expected:
        assert expected["mu"] in actual["mu"]
    assert expected["regs_contains"] in actual["regs"]

@pytest.mark.parametrize("page_num, expected", EXPECTED_LAST_ROWS.items())
def test_last_row_content(parser, pdf, page_num, expected):
    """Verify the last row of the page is captured correctly."""
    rows = parser.extract_rows(pdf.pages[page_num - 1])
    actual = rows[-1]
    
    assert expected["water"] in actual["water"], f"Page {page_num} last row mismatch."
    if "mu" in expected:
        assert expected["mu"] in actual["mu"]
    assert expected["regs_contains"] in actual["regs"]

@pytest.mark.parametrize("page_num, symbol_checks", EXPECTED_SYMBOLS.items())
def test_symbol_logic(parser, pdf, page_num, symbol_checks):
    """Test Stocked, Classified, and Incl. Tribs detection."""
    rows = parser.extract_rows(pdf.pages[page_num - 1])
    
    for check in symbol_checks:
        # Find the row by water body name
        matching_rows = [r for r in rows if check["water"] in r["water"]]
        assert len(matching_rows) > 0, f"Could not find {check['water']} on page {page_num}"
        
        actual_symbols = matching_rows[0]["symbols"]
        for sym in check["has"]:
            assert sym in actual_symbols, f"{check['water']} missing symbol {sym}. Got: {actual_symbols}"

def test_management_unit_lookbehind(parser, pdf):
    """Verify that MUs in notes like '(also in M.U. 5-15)' are NOT in the MU column."""
    # This specific note appears on various pages, let's check a known one
    # Page 17 has 'ELK RIVER (also see Buttle Lake's...)'
    page = pdf.pages[16]
    rows = parser.extract_rows(page)
    elk = next(r for r in rows if "ELK RIVER" in r["water"])
    
    # MU column should only contain the primary 1-9
    assert elk["mu"] == "1-9"
    # Ensure no other MUs from the text were added to the MU string
    assert "5-15" not in elk["mu"]

def test_newline_preservation(parser, pdf):
    """Verify that the smart_wrap/layout logic preserves newlines in regulations."""
    page = pdf.pages[16] # Page 17
    rows = parser.extract_rows(page)
    
    # Copper Creek typically has multiple paragraphs/lines
    copper = next(r for r in rows if "COPPER CREEK" in r["water"])
    assert "\n" in copper["regs"], "Regulations did not preserve hard line breaks."

def test_within_bbox_buffer_check(parser, pdf):
    """Specifically check for CRAIGFLOWER CREEK which fails if buffers are missing."""
    page = pdf.pages[16] # Page 17
    rows = parser.extract_rows(page)
    
    craig = [r for r in rows if "CRAIGFLOWER" in r["water"]]
    assert len(craig) > 0, "CRAIGFLOWER CREEK missing. within_bbox might be too strict."
    assert craig[0]["mu"] == "1-1"
    

def test_canim_river_specific_logic(parser, pdf):
    """
    Verify Canim River on page 31:
    1. Primary MU '3-46' is captured.
    2. Bracketed MU note '(also in M.U. 5-15)' is ignored by MU logic.
    3. The note remains part of the water body name.
    """
    page = pdf.pages[30] # Page 31
    rows = parser.extract_rows(page)
    
    canim = next((r for r in rows if "CANIM RIVER" in r["water"]), None)
    assert canim is not None, "CANIM RIVER not found on page 31"
    
    # Check MU extraction
    assert canim["mu"] == "3-46", f"Expected primary MU 3-46, got {canim['mu']}"
    assert "5-15" not in canim["mu"], "MU column incorrectly captured the bracketed note '5-15'"
    
    # Check Water Body name integrity
    # It should still contain the note because the regex only replaces 'un-bracketed' MUs
    assert "(also in M.U. 5-15)" in canim["water"], "Bracketed note was incorrectly removed from Water Body name"
    
    # Check Regulations
    assert "catch and release" in canim["regs"]
    assert "bait ban" in canim["regs"]

# 1. Generate the list of pages that SHOULD be empty
# We assume the PDF has 88 pages based on your previous scan
TOTAL_PAGES = 88 
EMPTY_PAGES = [
    p for p in range(1, TOTAL_PAGES + 1) 
    if p not in PAGES_WITH_TABLES and p != 6
]

@pytest.mark.parametrize("page_num", EMPTY_PAGES)
def test_no_false_positives_on_non_table_pages(parser, pdf, page_num):
    """
    Verify that pages not in the known list do not return data.
    Ensures maps, ads, and info pages are ignored.
    """
    raw_page = pdf.pages[page_num - 1]
    rows = parser.extract_rows(raw_page)
    
    # Assert that no rows were found
    assert len(rows) == 0, (
        f"Page {page_num} is supposed to be empty/non-table, "
        f"but {len(rows)} rows were detected. Check for map artifacts or ads."
    )