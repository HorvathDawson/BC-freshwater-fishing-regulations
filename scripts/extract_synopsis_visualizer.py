import pdfplumber
import os
from PIL import Image, ImageDraw

# --- IMPORT LOGIC ---
from extract_synopsis import get_table_geometry, clean_doubled_chars

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
DEBUG_FRAMES_DIR = os.path.join(OUTPUT_DIR, "debug_frames")
PDF_PATH = os.path.join(OUTPUT_DIR, "fishing_synopsis.pdf")

# Ensure output directory exists
os.makedirs(DEBUG_FRAMES_DIR, exist_ok=True)

def process_page_visual(page, page_num):
    im_obj = page.to_image(resolution=150)
    base_image = im_obj.original.convert("RGBA")
    scale = base_image.width / page.width
    overlay = Image.new("RGBA", base_image.size, (255, 255, 255, 0))
    draw = ImageDraw.Draw(overlay)

    # CALL THE LOGIC
    geometry = get_table_geometry(page)
    
    if not geometry or geometry[0] is None:
        print(f"  [Page {page_num}] No table found.")
        return

    x0, divider, x1, top = geometry

    # Get the full table for visualization
    tables = page.find_tables(table_settings={"vertical_strategy": "lines", "horizontal_strategy": "lines"})
    if not tables:
        print(f"  [Page {page_num}] No table found.")
        return
    
    main_table = max(tables, key=lambda t: (t.bbox[2]-t.bbox[0]) * (t.bbox[3]-t.bbox[1]))
    table_bottom = main_table.bbox[3]

    # 1. DRAW RED BOX (The Table Zone)
    draw.rectangle(
        [(x0*scale, top*scale), (x1*scale, table_bottom*scale)], 
        outline="red", width=int(3*scale)
    )

    # 2. DRAW BLUE DIVIDER
    if divider:
        draw.line(
            [(divider*scale, top*scale), (divider*scale, table_bottom*scale)], 
            fill="blue", width=int(2*scale)
        )

    # 3. DRAW GRID ROWS (Green - Line Strategy)
    v_lines = [x0, x1]
    if divider: v_lines.append(divider)
    
    grid_settings = { 
        "vertical_strategy": "explicit", 
        "explicit_vertical_lines": v_lines,
        "horizontal_strategy": "lines", 
        "intersection_y_tolerance": 10
    }
    
    try:
        crop = page.crop((x0, top, x1, table_bottom))
        tables = crop.find_tables(grid_settings)
        for table in tables:
            for row in table.rows:
                # Cell 1
                c1 = row.cells[0]
                if c1: draw.line([(c1[0]*scale, c1[3]*scale), (c1[2]*scale, c1[3]*scale)], 
                                 fill=(0, 255, 0, 255), width=int(2*scale))
                # Cell 2
                if len(row.cells) > 1 and row.cells[1]: 
                    c2 = row.cells[1]
                    draw.line([(c2[0]*scale, c2[3]*scale), (c2[2]*scale, c2[3]*scale)], 
                              fill=(0, 255, 0, 255), width=int(2*scale))
    except Exception as e:
        print(f"Error drawing grid rows: {e}")

    combined = Image.alpha_composite(base_image, overlay)
    save_path = os.path.join(DEBUG_FRAMES_DIR, f"page_{page_num}.png")
    combined.save(save_path)
    print(f"  [Page {page_num}] Saved debug visual -> {save_path}")

def debug_all_pages():
    if not os.path.exists(PDF_PATH):
        print("PDF not found.")
        return
    print(f"--- GENERATING VISUALS (Using Logic from extract_synopsis.py) ---")
    
    with pdfplumber.open(PDF_PATH) as pdf:
        # Start after intro
        start_index = 10 
        pages = pdf.pages[start_index:]
        
        for i, page in enumerate(pages):
            page_num = start_index + i + 1
            
            # Simple keyword check to skip non-table pages
            txt = clean_doubled_chars(page.extract_text() or "")
            if "EXCEPTIONS" not in txt and "WATER BODY" not in txt:
                continue

            try: page = page.dedupe_chars(tolerance=1)
            except: pass 
            
            process_page_visual(page, page_num)

if __name__ == "__main__":
    debug_all_pages()