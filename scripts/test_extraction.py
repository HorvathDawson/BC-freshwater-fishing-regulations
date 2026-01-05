import pytest
import os
import pdfplumber
from parse_synopsis import (
    extract_visual_lines, 
    extract_text_by_spatial_layout, 
    PDF_FILENAME
)

@pytest.fixture(scope="module")
def pdf_page():
    """
    Fixture that opens the PDF once, finds the specific target page,
    yields it to the tests, and then closes the PDF.
    """
    if not os.path.exists(PDF_FILENAME):
        pytest.fail(f"Could not find {PDF_FILENAME}. Run the main script once to download it.")
    
    with pdfplumber.open(PDF_FILENAME) as pdf:
        target_page = None
        # Locate the page with Bainbridge/Bear Lake/Cowichan
        for page in pdf.pages:
            text = page.extract_text() or ""
            if "BAINBRIDGE LAKE" in text and "BEAR LAKE" in text and "Cowichan Lake" in text:
                target_page = page
                print(f"Found test case on Page {page.page_number}")
                break
        
        if not target_page:
            # Fallback for debugging specific versions
            try:
                target_page = pdf.pages[15]
                print("Search failed, defaulting to Index 15.")
            except IndexError:
                pytest.fail("Could not find the page containing 'BAINBRIDGE LAKE' and 'BEAR LAKE'")
        
        yield target_page

def test_row_separation_bainbridge_bear(pdf_page):
    """
    Regression Test: Ensures BAINBRIDGE LAKE text does not bleed into BEAR LAKE.
    """
    # BBox for Row 15 (Bear Lake)
    bbox_bear = (36.137, 232.487, 173.155, 242.869)

    print("\nTesting Name Extraction...")
    bear_lines = extract_visual_lines(pdf_page, bbox_bear)
    bear_text = " ".join(bear_lines)
    
    print(f"  Extracted: '{bear_text}'")

    assert "BEAR LAKE" in bear_text, "Should contain the correct lake name"
    assert "BAINBRIDGE" not in bear_text, "Should NOT contain text from the row above"

def test_regulation_separation_bainbridge_bear(pdf_page):
    """
    Regression Test: Ensures regulations from Bainbridge don't bleed into Bear Lake.
    """
    # BBox for Row 15 (Bear Lake Regulations)
    bbox_bear_regs = (173.155, 232.487, 570.0, 242.869)

    print("\nTesting Regulation Extraction...")
    reg_lines = extract_text_by_spatial_layout(pdf_page, bbox_bear_regs)
    reg_text = " ".join(reg_lines)
    
    print(f"  Extracted: '{reg_text}'")

    assert "See Cowichan Lake" in reg_text, "Should contain the correct regulation"
    assert "No angling from boats" not in reg_text, "Should NOT capture 'No angling' from row above"

def test_ghost_height_logic(pdf_page):
    """
    Unit Test: Verifies that the 'height < 4.0' logic is actually filtering words.
    We manually crop a tiny slice that spans the boundary to simulate the bleed.
    """
    # Create a bbox that spans the boundary (part of Bainbridge, part of Bear)
    mixed_bbox = (36.137, 230.0, 173.155, 240.0)
    
    lines = extract_visual_lines(pdf_page, mixed_bbox)
    text = " ".join(lines)
    
    print(f"\nTesting Boundary Logic (Simulated Bleed)...")
    print(f"  Input BBox: {mixed_bbox}")
    print(f"  Result: '{text}'")

    assert "1-7" not in text, "Should filter out the clipped text '1-7' from the top edge"