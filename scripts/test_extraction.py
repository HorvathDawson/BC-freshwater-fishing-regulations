import pytest
import os
import pdfplumber
from parse_synopsis import (
    extract_visual_lines, 
    extract_text_by_spatial_layout, 
    PDF_FILENAME
)

@pytest.fixture(scope="module")
def pdf():
    """
    Fixture that opens the PDF once and yields it to all tests.
    """
    if not os.path.exists(PDF_FILENAME):
        pytest.fail(f"Could not find {PDF_FILENAME}. Run the main script once to download it.")
    
    with pdfplumber.open(PDF_FILENAME) as pdf_obj:
        yield pdf_obj

@pytest.fixture(scope="module")
def bainbridge_bear_page(pdf):
    """
    Fixture that finds the specific page with Bainbridge/Bear Lake test case.
    """
    target_page = None
    # Locate the page with Bainbridge/Bear Lake/Cowichan
    for page in pdf.pages:
        text = page.extract_text() or ""
        if "BAINBRIDGE LAKE" in text and "BEAR LAKE" in text and "Cowichan Lake" in text:
            target_page = page
            print(f"Found Bainbridge/Bear test case on Page {page.page_number}")
            break
    
    if not target_page:
        # Fallback for debugging specific versions
        try:
            target_page = pdf.pages[15]
            print("Search failed, defaulting to Index 15.")
        except IndexError:
            pytest.fail("Could not find the page containing 'BAINBRIDGE LAKE' and 'BEAR LAKE'")
    
    return target_page

def test_row_separation_bainbridge_bear(bainbridge_bear_page):
    """
    Regression Test: Ensures BAINBRIDGE LAKE text does not bleed into BEAR LAKE.
    """
    # BBox for Row 15 (Bear Lake)
    bbox_bear = (36.137, 232.487, 173.155, 242.869)

    print("\nTesting Name Extraction...")
    bear_lines = extract_visual_lines(bainbridge_bear_page, bbox_bear)
    bear_text = " ".join(bear_lines)
    
    print(f"  Extracted: '{bear_text}'")

    assert "BEAR LAKE" in bear_text, "Should contain the correct lake name"
    assert "BAINBRIDGE" not in bear_text, "Should NOT contain text from the row above"

def test_regulation_separation_bainbridge_bear(bainbridge_bear_page):
    """
    Regression Test: Ensures regulations from Bainbridge don't bleed into Bear Lake.
    """
    # BBox for Row 15 (Bear Lake Regulations)
    bbox_bear_regs = (173.155, 232.487, 570.0, 242.869)

    print("\nTesting Regulation Extraction...")
    reg_lines = extract_text_by_spatial_layout(bainbridge_bear_page, bbox_bear_regs)
    reg_text = " ".join(reg_lines)
    
    print(f"  Extracted: '{reg_text}'")

    assert "See Cowichan Lake" in reg_text, "Should contain the correct regulation"
    assert "No angling from boats" not in reg_text, "Should NOT capture 'No angling' from row above"

def test_ghost_height_logic(bainbridge_bear_page):
    """
    Unit Test: Verifies that the 'height < 4.0' logic is actually filtering words.
    We manually crop a tiny slice that spans the boundary to simulate the bleed.
    """
    # Create a bbox that spans the boundary (part of Bainbridge, part of Bear)
    mixed_bbox = (36.137, 230.0, 173.155, 240.0)
    
    lines = extract_visual_lines(bainbridge_bear_page, mixed_bbox)
    text = " ".join(lines)
    
    print(f"\nTesting Boundary Logic (Simulated Bleed)...")
    print(f"  Input BBox: {mixed_bbox}")
    print(f"  Result: '{text}'")

    assert "1-7" not in text, "Should filter out the clipped text '1-7' from the top edge"
    
def test_page_continuity():
    """
    Post-Processing Check: Scans the output text file to ensure no pages were skipped.
    
    If Region 1 starts on Page 15 and ends on Page 20, we expect:
    15, 16, 17, 18, 19, 20.
    
    If we see 15, 17... we know Page 16 was dropped (likely due to parsing errors).
    """
    import re
    import os

    # 1. Path to your output file
    # Adjust path if your structure is different
    OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "output", "fishing_regs.txt")
    
    if not os.path.exists(OUTPUT_FILE):
        pytest.fail(f"Output file not found: {OUTPUT_FILE}. Run the main script first.")

    print(f"\nAnalyzing continuity in: {OUTPUT_FILE}")

    # 2. Extract (Page Number, Region Name) pairs
    # We look for the divider lines: "========== PAGE 15 (REGION 1 - Vancouver Island) =========="
    page_pattern = re.compile(r'={10}\s*PAGE\s+(\d+)\s+\((.*?)\)\s*={10}')
    
    with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
        content = f.read()

    matches = page_pattern.findall(content)
    
    # Structure: { "Region 1": [15, 16, 17], "Region 2": [...] }
    region_pages = {}
    for page_str, region_name in matches:
        page_num = int(page_str)
        if region_name not in region_pages:
            region_pages[region_name] = []
        if page_num not in region_pages[region_name]:
            region_pages[region_name].append(page_num)

    # 3. Check for Gaps
    gaps_found = []
    
    for region, pages in region_pages.items():
        pages.sort()
        if not pages: continue
        
        start = pages[0]
        end = pages[-1]
        
        # Create the perfect sequence expected
        expected_sequence = set(range(start, end + 1))
        actual_sequence = set(pages)
        
        missing_pages = sorted(list(expected_sequence - actual_sequence))
        
        if missing_pages:
            gaps_found.append(f"  {region}: Missing pages {missing_pages}")

    # 4. Report Results
    if gaps_found:
        error_msg = "\n[FATAL] Gaps detected in extraction output:\n" + "\n".join(gaps_found)
        error_msg += "\n\nPossible Causes:\n  - Corrupt Text Layer (needs OCR fix)\n  - Missing 'EXCEPTIONS' header\n  - 'No Tables Found' error"
        pytest.fail(error_msg)
    
    else:
        print("\nSUCCESS: All regions have continuous page numbers. No gaps detected.")