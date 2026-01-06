import pdfplumber
import os
import argparse
from PIL import Image, ImageDraw
import numpy as np

# --- IMPORT LOGIC ---
from extract_synopsis import get_table_geometry, get_cleaned_page

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
DEBUG_FRAMES_DIR = os.path.join(OUTPUT_DIR, "debug_frames")
PDF_PATH = os.path.join(OUTPUT_DIR, "fishing_synopsis.pdf")

MANDATORY_PAGES = [17] 

os.makedirs(DEBUG_FRAMES_DIR, exist_ok=True)

def process_page_visual(page, page_num):
    print(f"  [Page {page_num}] Cleaning ghost text and generating visual...")
    
    # 1. Clean the page
    clean_page = get_cleaned_page(page)
    
    # 2. Setup rendering
    resolution = 150
    im_obj = clean_page.to_image(resolution=resolution)
    base_image = im_obj.original.convert("RGBA")
    scale = resolution / 72
    
    overlay = Image.new("RGBA", base_image.size, (255, 255, 255, 0))
    draw = ImageDraw.Draw(overlay)

    # 3. Draw Surviving Characters + Text labels
    for char in clean_page.chars:
        x0, y0, x1, y1 = char['x0'], char['top'], char['x1'], char['bottom']
        
        # Draw character bounding box (Black)
        draw.rectangle(
            [x0*scale, y0*scale, x1*scale, y1*scale], 
            outline=(0, 0, 0, 120), 
            width=1
        )
        
        # Draw the actual character text (Tiny Red)
        # Note: Without a specific .ttf font loaded, this uses a default bitmap font
        draw.text((x0*scale, (y0*scale) - 8), char['text'], fill=(255, 0, 0, 255))

    # 4. Draw Geometry Logic
    geometry = get_table_geometry(clean_page)
    if geometry and geometry[0] is not None:
        x0, divider, x1, top = geometry
        
        # Determine table bottom
        tables = clean_page.find_tables(table_settings={"vertical_strategy": "lines", "horizontal_strategy": "lines"})
        if tables:
            main_table = max(tables, key=lambda t: (t.bbox[2]-t.bbox[0]) * (t.bbox[3]-t.bbox[1]))
            table_bottom = main_table.bbox[3]

            # Red Box: Table Boundary
            draw.rectangle([(x0*scale, top*scale), (x1*scale, table_bottom*scale)], outline="red", width=3)

            # Blue Line: Column Divider
            if divider:
                draw.line([(divider*scale, top*scale), (divider*scale, table_bottom*scale)], fill="blue", width=2)

            # Green Lines: Grid Rows
            v_lines = [x0, x1]
            if divider: v_lines.append(divider)
            grid_settings = {"vertical_strategy": "explicit", "explicit_vertical_lines": v_lines, 
                             "horizontal_strategy": "lines", "intersection_y_tolerance": 10}
            
            try:
                crop = clean_page.crop((x0, top, x1, table_bottom))
                found_tables = crop.find_tables(grid_settings)
                for table in found_tables:
                    for row in table.rows:
                        for cell in row.cells:
                            if cell:
                                draw.line([(cell[0]*scale, cell[3]*scale), (cell[2]*scale, cell[3]*scale)], 
                                          fill=(0, 255, 0, 255), width=2)
            except Exception as e:
                print(f"    Error drawing grid: {e}")

    # 5. Finalize
    combined = Image.alpha_composite(base_image, overlay)
    save_path = os.path.join(DEBUG_FRAMES_DIR, f"page_{page_num}_debug.png")
    combined.save(save_path)
    print(f"  [Page {page_num}] Saved debug visual -> {save_path}")

def run_debug(target_page=None):
    if not os.path.exists(PDF_PATH):
        print(f"Error: PDF not found at {PDF_PATH}")
        return
    
    with pdfplumber.open(PDF_PATH) as pdf:
        if target_page:
            # Run single page mode
            if 1 <= target_page <= len(pdf.pages):
                print(f"--- DEBUGGING SINGLE PAGE: {target_page} ---")
                process_page_visual(pdf.pages[target_page-1], target_page)
            else:
                print(f"Error: Page {target_page} is out of range (1-{len(pdf.pages)})")
        else:
            # Run all pages mode
            print(f"--- GENERATING VISUALS FOR ALL TABLE PAGES ---")
            start_index = 10 
            for i, page in enumerate(pdf.pages[start_index:]):
                page_num = start_index + i + 1
                raw_text = (page.extract_text() or "").lower()
                has_keywords = any(k in raw_text for k in ["exceptions", "water body", "mgmt unit"])
                
                if has_keywords or page_num in MANDATORY_PAGES:
                    try:
                        process_page_visual(page, page_num)
                    except Exception as e:
                        print(f"Failed to process page {page_num}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Visualize PDF table extraction with ghost filtering.")
    parser.add_argument("--page", type=int, help="Specify a single page number to debug (e.g., --page 17)")
    
    args = parser.parse_args()
    run_debug(target_page=args.page)