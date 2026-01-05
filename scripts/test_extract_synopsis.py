import pytest
import pdfplumber
import os

# Import logic
from extract_synopsis import get_table_geometry, extract_rows_from_page, get_table_text_sizes

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PDF_PATH = os.path.join(SCRIPT_DIR, "output", "fishing_synopsis.pdf")

@pytest.fixture(scope="module")
def pdf():
    if not os.path.exists(PDF_PATH):
        pytest.fail(f"PDF not found at {PDF_PATH}")
    with pdfplumber.open(PDF_PATH) as p:
        yield p

def get_page(pdf, page_num):
    return pdf.pages[page_num - 1]

# --- TEST DATA ---
# Expected data for first rows on each page
EXPECTED_FIRST_ROWS = {
    17: [
        {"water_contains": "WATER BODY", "regs_contains": "EXCEPTIONS"},  # Header row - will fail due to doubled chars artifact in PDF
        {"water_contains": "COMOX LAKE", "regs_contains": "No wild rainbow trout over 50 cm"},
        {"water_contains": "CONSORT CREEK", "regs_contains": "No Fishing"},
    ],
    19: [
        {"water_contains": "WATER BODY | MGMT UNIT", "regs_contains": "EXCEPTIONS TO THE REGIONAL REGULATIONS"},
        {"water_contains": "LITTLE QUALICUM RIVER 1-6", "regs_contains": "Fly fishing only, Sept 1-Nov 30"},  # Multi-line regs
        {"water_contains": "LIZARD LAKE 1-3", "regs_contains": "electric motor only - max 7.5 kW"},
    ],
    24: [
        {"water_contains": "WATER BODY | MGMT UNIT", "regs_contains": "EXCEPTIONS TO THE REGIONAL REGULATIONS"},
        {"water_contains": "ALICE LAKE 2-7", "regs_contains": "No powered boats"},
        {"water_contains": "ALOUETTE LAKE 2-8", "regs_contains": "Bull trout (char) catch and release"},
    ],
    25: [
        {"water_contains": "WATER BODY | MGMT UNIT", "regs_contains": "EXCEPTIONS TO THE REGIONAL REGULATIONS"},
        {"water_contains": "COQUITLAM RIVER 2-8", "regs_contains": "Hatchery trout daily quota = 2"},  # Multi-line regs
        {"water_contains": "COMO (Welcome) LAKE 2-8", "regs_contains": "Trout/char daily quota = 2; single barbless hook"},
    ],
    26: [
        {"water_contains": "WATER BODY | MGMT UNIT", "regs_contains": "EXCEPTIONS TO THE REGIONAL REGULATIONS"},
        {"water_contains": "KOKOMO LAKE 2-5", "regs_contains": "Electric motor only - max 7.5 kW"},
        {"water_contains": "LAFARGE (Pinetree Gravel Pit) LAKE 2-8", "regs_contains": "Trout/char daily quota = 2; single barbless hook"},
    ],
    28: [
        {"water_contains": "WATER BODY | MGMT UNIT", "regs_contains": "EXCEPTIONS TO THE REGIONAL REGULATIONS"},
        {"water_contains": "SQUAMISH RIVER", "regs_contains": "Bait ban"},
        {"water_contains": "SQUAMISH RIVER", "regs_contains": "No Fishing tributaries EXCEPT: Ashlu Creek, Cheakamus, Elaho and Mamquam Rivers"},
    ],
    60: [
        {"water_contains": "WATER BODY | MGMT UNIT", "regs_contains": "EXCEPTIONS TO THE REGIONAL REGULATIONS"},
        {"water_contains": "TATSATUA CREEK", "regs_contains": "No Fishing from Dec 1-June 30 and Aug 20-Sept 15"},
        {"water_contains": "TCHESINKUT LAKE 6-4", "regs_contains": "Single barbless hook"},  # Multi-line regs
    ],
}

# Expected data for last rows on each page  
EXPECTED_LAST_ROWS = {
    17: [
        {"water_contains": "FAREWELL LAKE", "regs_contains": "Trout daily quota = 1 (none over 50 cm); artificial fly only, bait ban, single barbless hook"},
        {"water_contains": "FICKLE LAKE", "regs_contains": "No trout over 50 cm; bait ban, single barbless hook"},
    ],
    19: [
        {"water_contains": "PROSPECT LAKE 1-2", "regs_contains": "Smallmouth bass daily quota = 4; speed restriction on parts (8 and 60 km/h)"},
        {"water_contains": "PROVOST DAM 1-5", "regs_contains": "No powered boats"},
    ],
    24: [
        {"water_contains": "COLVIN CREEK 2-5", "regs_contains": "No Fishing"},
        {"water_contains": "COQUIHALLA RIVER 2-17", "regs_contains": "Trout/char (including steelhead) catch and release, bait ban, downstream of the southern entrance to the lower most railway tunnel, No"},
    ],
    25: [
        {"water_contains": "KHARTOUM LAKE 2-12", "regs_contains": "Wild trout/char daily quota = 2 (no wild trout 40 cm or more), hatchery rainbow trout"},
        {"water_contains": "KLEIN LAKE 2-5", "regs_contains": "Electric motor only - max 7.5 kW ; wheelchair accessible"},
    ],
    26: [
        {"water_contains": "MOSS POTHOLE", "regs_contains": "No Fishing; unnamed pothole lakes all located east of the power line"},
        {"water_contains": "NANTON LAKE 2-12", "regs_contains": "Wild trout/char daily quota = 2 (no wild trout over 40 cm); single barbless hook"},
    ],
    28: [
        {"water_contains": "WINDSOR LAKE 2-12", "regs_contains": "Wild trout/char daily quota = 2 (no wild trout over 40 cm); single barbless hook"},
        {"water_contains": "WOLF LAKE 2-19", "regs_contains": "No powered boats"},
    ],
    60: [
        {"water_contains": "WEST ROAD", "regs_contains": "Trout daily quota = 1; artificial fly only, bait ban"},
        {"water_contains": "ZYMOETZ (Copper) RIVER 6-9", "regs_contains": "No Fishing from McDonell Lake downstream approximately 3 km to fishing boundary signs"},
    ],
}

# Expected symbols for specific water bodies on each page
EXPECTED_SYMBOLS = {
    19: [
        {"water_contains": "OYSTER RIVER 1-6", "has_symbols": ["Stocked"]},
        {"water_contains": "PALLANT CREEK 6-12", "has_symbols": ["Includes Tributaries", "Classified Waters"]},
        {"water_contains": "LIZARD LAKE 1-3", "has_symbols": ["Stocked"]},
    ],
    24: [
        {"water_contains": "ALICE LAKE 2-7", "has_symbols": ["Stocked"]},
        {"water_contains": "ALOUETTE RIVER", "has_symbols": ["Includes Tributaries"]},
        {"water_contains": "ALPHA LAKE 2-9", "has_symbols": ["Stocked"]},
        {"water_contains": "COQUIHALLA RIVER 2-17", "has_symbols": ["Includes Tributaries"]},
    ],
    28: [
        {"water_contains": "SQUAMISH RIVER", "has_symbols": ["Includes Tributaries"]},
    ],
    60: [
        {"water_contains": "ZYMOETZ (Copper) RIVER 6-9", "has_symbols": ["Includes Tributaries", "Classified Waters"]},
    ],
}

# --- TESTS ---

@pytest.mark.parametrize("page_num", [17, 19, 24, 25, 26, 28, 60])
def test_page_geometry_detection(pdf, page_num):
    """Test that table geometry is correctly detected for each page."""
    page = get_page(pdf, page_num)
    x0, divider, x1, top = get_table_geometry(page)
    
    assert x0 is not None, f"Page {page_num}: Could not detect table geometry"
    assert divider is not None, f"Page {page_num}: Could not detect divider"
    assert x1 > x0, f"Page {page_num}: Invalid table width"
    assert divider > x0 and divider < x1, f"Page {page_num}: Divider outside table bounds"


@pytest.mark.parametrize("page_num", [17, 19, 24, 25, 26, 28, 60])
def test_page_extracts_rows(pdf, page_num):
    """Test that rows can be extracted from each page."""
    page = get_page(pdf, page_num)
    rows = extract_rows_from_page(page)
    
    assert len(rows) > 0, f"Page {page_num}: No rows extracted"
    
    # Check structure - all rows should have water, regs, and symbols keys
    for i, row in enumerate(rows[:3]):  # Check first 3 rows
        assert "water" in row, f"Page {page_num}, Row {i}: Missing 'water' key"
        assert "regs" in row, f"Page {page_num}, Row {i}: Missing 'regs' key"
        assert "symbols" in row, f"Page {page_num}, Row {i}: Missing 'symbols' key"
        assert isinstance(row["symbols"], list), f"Page {page_num}, Row {i}: 'symbols' should be a list"


@pytest.mark.parametrize("page_num,expected_symbols", [
    (19, EXPECTED_SYMBOLS[19]),
    (24, EXPECTED_SYMBOLS[24]),
    (28, EXPECTED_SYMBOLS[28]),
    (60, EXPECTED_SYMBOLS[60]),
])
def test_symbol_detection(pdf, page_num, expected_symbols):
    """Test that symbols (Stocked, Classified Waters, Includes Tributaries) are correctly detected."""
    page = get_page(pdf, page_num)
    rows = extract_rows_from_page(page)
    
    for expected in expected_symbols:
        # Find the water body
        matching_rows = [r for r in rows if expected["water_contains"] in r["water"]]
        
        assert len(matching_rows) > 0, \
            f"Page {page_num}: Could not find water body containing '{expected['water_contains']}'"
        
        row = matching_rows[0]
        actual_symbols = row.get("symbols", [])
        
        for expected_symbol in expected["has_symbols"]:
            assert expected_symbol in actual_symbols, \
                f"Page {page_num}: Water body '{row['water'][:50]}' should have symbol '{expected_symbol}', got {actual_symbols}"



@pytest.mark.parametrize("page_num,expected_rows", [
    (17, EXPECTED_FIRST_ROWS[17]),
    (19, EXPECTED_FIRST_ROWS[19]),
    (24, EXPECTED_FIRST_ROWS[24]),
    (25, EXPECTED_FIRST_ROWS[25]),
    (26, EXPECTED_FIRST_ROWS[26]),
    (28, EXPECTED_FIRST_ROWS[28]),
    (60, EXPECTED_FIRST_ROWS[60]),
])
def test_first_rows_content(pdf, page_num, expected_rows):
    """Test that first rows contain expected content."""
    page = get_page(pdf, page_num)
    rows = extract_rows_from_page(page)
    
    assert len(rows) >= len(expected_rows), \
        f"Page {page_num}: Expected at least {len(expected_rows)} rows, got {len(rows)}"
    
    for i, expected in enumerate(expected_rows):
        actual = rows[i]
        
        if expected["water_contains"]:
            assert expected["water_contains"] in actual["water"], \
                f"Page {page_num}, Row {i}: Expected water to contain '{expected['water_contains']}', got '{actual['water']}'"
        
        if expected["regs_contains"]:
            assert expected["regs_contains"] in actual["regs"], \
                f"Page {page_num}, Row {i}: Expected regs to contain '{expected['regs_contains']}', got '{actual['regs']}'"


@pytest.mark.parametrize("page_num", [17, 19, 24, 25, 26, 28, 60])
def test_last_rows_structure(pdf, page_num):
    """Test that last rows have proper structure."""
    page = get_page(pdf, page_num)
    rows = extract_rows_from_page(page)
    
    assert len(rows) > 0, f"Page {page_num}: No rows extracted"
    
    # Check last 2 rows
    for row in rows[-2:]:
        assert "water" in row, f"Page {page_num}: Last row missing 'water' key"
        assert "regs" in row, f"Page {page_num}: Last row missing 'regs' key"


@pytest.mark.parametrize("page_num,expected_rows", [
    (17, EXPECTED_LAST_ROWS[17]),
    (19, EXPECTED_LAST_ROWS[19]),
    (24, EXPECTED_LAST_ROWS[24]),
    (25, EXPECTED_LAST_ROWS[25]),
    (26, EXPECTED_LAST_ROWS[26]),
    (28, EXPECTED_LAST_ROWS[28]),
    (60, EXPECTED_LAST_ROWS[60]),
])
def test_last_rows_content(pdf, page_num, expected_rows):
    """Test that last rows contain expected content."""
    page = get_page(pdf, page_num)
    rows = extract_rows_from_page(page)
    
    assert len(rows) >= len(expected_rows), \
        f"Page {page_num}: Expected at least {len(expected_rows)} rows, got {len(rows)}"
    
    # Check the last N rows
    actual_last_rows = rows[-len(expected_rows):]
    
    for i, expected in enumerate(expected_rows):
        actual = actual_last_rows[i]
        
        if expected["water_contains"]:
            assert expected["water_contains"] in actual["water"], \
                f"Page {page_num}, Last row {i}: Expected water to contain '{expected['water_contains']}', got '{actual['water']}'"
        
        if expected["regs_contains"]:
            assert expected["regs_contains"] in actual["regs"], \
                f"Page {page_num}, Last row {i}: Expected regs to contain '{expected['regs_contains']}', got '{actual['regs']}'"


@pytest.mark.parametrize("page_num,max_sizes", [
    (17, 4), (19, 4), (24, 4), (25, 4), (26, 4), (28, 4), (60, 4)  # All pages should have consistent text sizes
])
def test_table_text_sizes_consistent(pdf, page_num, max_sizes):
    """Test that text sizes in table are consistent (mainly one size, possibly 2-3 for headers)."""
    page = get_page(pdf, page_num)
    sizes = get_table_text_sizes(page)
    
    assert len(sizes) > 0, f"Page {page_num}: No text sizes found in table"
    
    # Should have a limited number of sizes (typically 1-4)
    # If this fails, there may be a map or graphic in the table that needs to be filtered out
    assert len(sizes) <= max_sizes, \
        f"Page {page_num}: Too many text sizes ({len(sizes)}): {sizes[:10]}... Expected 1-{max_sizes} sizes. Likely has map/graphic."
    
    # Print for debugging
    print(f"\nPage {page_num} text sizes: {len(sizes)} unique sizes")


def test_specific_row_content_page_17(pdf):
    """Test specific known content on page 17."""
    page = get_page(pdf, 17)
    rows = extract_rows_from_page(page)
    
    # Find COMOX LAKE row
    comox_rows = [r for r in rows if "COMOX LAKE" in r["water"]]
    assert len(comox_rows) > 0, "Could not find COMOX LAKE on page 17"


def test_specific_row_content_page_19(pdf):
    """Test specific known content on page 19."""
    page = get_page(pdf, 19)
    rows = extract_rows_from_page(page)
    
    # Find LITTLE QUALICUM RIVER row (actually on page 19)
    qualicum_rows = [r for r in rows if "LITTLE QUALICUM RIVER" in r["water"]]
    assert len(qualicum_rows) > 0, "Could not find LITTLE QUALICUM RIVER on page 19"