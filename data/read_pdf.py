import pdfplumber
import re

# --- CONFIGURATION ---
# Keywords that indicate a row is actually map text/garbage, not a regulation
INVALID_KEYWORDS = [
    "Kilometres", "courtesy of", "purchase a larger map", 
    "reprinted", "Haig-Brown", "scale", "www.", ".ca", ".com",
    "Department of Fisheries", "Management Unit", "Road", "Hwy"
]

def is_fish_vector(curve):
    """ Detects the vector fish icon based on size and shape. """
    width = curve['x1'] - curve['x0']
    height = curve['bottom'] - curve['top']
    
    # Fish icon sizing (approx 5px - 40px)
    if not (4 < width < 40) or not (4 < height < 40): return False
    
    # Aspect ratio check
    ratio = width / height if height > 0 else 0
    return 0.2 < ratio < 5.0

def get_fish_locations(page):
    """ Returns center coordinates (x, y) for all fish vectors. """
    centers = []
    for curve in page.curves:
        if is_fish_vector(curve):
            cx = (curve['x0'] + curve['x1']) / 2
            cy = (curve['top'] + curve['bottom']) / 2
            centers.append((cx, cy))
    return centers

def clean_text(text):
    if not text: return ""
    # Standardize whitespace
    text = text.replace('\n', ' ').strip()
    return re.sub(r'\s+', ' ', text)

def is_valid_row(name, regs):
    """ Filters out rows that contain map legends or disclaimer text. """
    combined = (name + " " + regs).lower()
    
    # Check 1: Explicit junk keywords
    if any(k.lower() in combined for k in INVALID_KEYWORDS):
        return False
        
    # Check 2: Header rows
    if "WATER BODY" in name or "EXCEPTIONS" in regs:
        return False
        
    # Check 3: Empty rows
    if not name.strip() and not regs.strip():
        return False
        
    return True

def parse_water_col(text, bbox, fish_locations):
    """ Extracts symbols from Col 1 and returns cleaned name + symbol list. """
    text = clean_text(text)
    symbols = []
    
    # 1. Text Symbols
    if '\uf0dc' in text or '*' in text:
        symbols.append("Tributaries")
        text = text.replace('\uf0dc', '').replace('*', '')

    if "CW" in text and re.search(r'\bCW\b', text):
        symbols.append("Classified Waters")
        text = re.sub(r'\bCW\b', '', text)

    # 2. Vector Symbols (Spatial Match)
    if bbox:
        x0, top, x1, bottom = bbox
        for (fx, fy) in fish_locations:
            if x0 < fx < x1 and top < fy < bottom:
                symbols.append("Stocked")
                break 

    return text.strip(), symbols

def parse_regs_col(text):
    """ Replaces symbols in Col 2 with [Bracketed Text]. """
    text = clean_text(text)
    # Replace tributary symbols in-place
    text = text.replace('\uf0dc', ' [TRIBUTARY] ').replace('*', ' [TRIBUTARY] ')
    return re.sub(r'\s+', ' ', text).strip()

def get_table_geometry(page):
    """ specific logic to find the vertical divider between columns. """
    tables = page.find_tables(table_settings={"vertical_strategy": "lines", "horizontal_strategy": "lines"})
    if not tables: return None, None, None, None

    # Assume the largest table is the main data table
    main_table = max(tables, key=lambda t: (t.bbox[2]-t.bbox[0]) * (t.bbox[3]-t.bbox[1]))
    x0, top, x1, bottom = main_table.bbox
    
    # Find the divider by looking at the first split row
    cells = sorted(main_table.cells, key=lambda c: (c[1], c[0]))
    divider = None
    for c in cells:
        # Check if cell starts at left edge but ends before right edge
        if abs(c[0] - x0) < 2 and abs(c[2] - x1) > 5:
            divider = c[2]
            break
            
    return x0, divider, x1, top

def process_table(table, fish_locations):
    structured_data = []
    current_entry = None

    text_rows = table.extract()
    row_objs = table.rows 

    if len(text_rows) != len(row_objs): return []

    for i, row in enumerate(row_objs):
        if not row.cells or len(row.cells) < 2: continue
        
        # --- Extract Raw Data ---
        c1_box = row.cells[0]
        c1_raw = text_rows[i][0] or ""
        c2_raw = text_rows[i][1] if len(text_rows[i]) > 1 else ""

        # --- Parse & Clean ---
        name, c1_syms = parse_water_col(c1_raw, c1_box, fish_locations)
        regs = parse_regs_col(c2_raw)

        # --- Filter Garbage Rows ---
        if not is_valid_row(name, regs):
            continue

        # --- Merge Logic ---
        if name:
            # New Entry
            if current_entry: structured_data.append(current_entry)
            current_entry = {
                "Water": name, 
                "Symbols": c1_syms,
                "Regs": regs
            }
        elif current_entry and regs:
            # Continuation
            current_entry["Regs"] += " " + regs
            # Check for hidden symbols in empty Col 1 cells
            _, extra_syms = parse_water_col(c1_raw, c1_box, fish_locations)
            if extra_syms:
                current_entry["Symbols"] = list(set(current_entry["Symbols"] + extra_syms))

    if current_entry: structured_data.append(current_entry)
    return structured_data

def extract_fishing_regs(pdf_path, output_path):
    print(f"Scanning {pdf_path}...")
    last_geom = None 

    with pdfplumber.open(pdf_path) as pdf, open(output_path, "w", encoding="utf-8") as f:
        
        # Pages 14+ contain the water-specific tables
        for page in pdf.pages[14:]: 
            # Quick check to skip non-table pages
            if "EXCEPTIONS" not in (page.extract_text() or ""): continue

            f.write(f"\n{'='*20} PAGE {page.page_number} {'='*20}\n")
            
            # 1. Page-Level Analysis
            fish_locs = get_fish_locations(page)
            geom = get_table_geometry(page)
            
            # 2. Geometry Inheritance (Handle pages with broken lines)
            if geom[1]: # valid divider found
                last_geom = geom
            elif last_geom:
                # Use current page margins but previous page divider
                x0, _, x1, top = geom if geom[0] else last_geom
                divider = last_geom[1]
                geom = (x0, divider, x1, top)
            else:
                continue # Skip if no geometry established

            # 3. Extract Tables using Explicit Columns
            x0, divider, x1, top = geom
            table_settings = {
                "vertical_strategy": "explicit", 
                "explicit_vertical_lines": [x0, divider, x1],
                "horizontal_strategy": "lines", 
                "intersection_y_tolerance": 10,
                "text_x_tolerance": 2, 
            }
            
            # Crop to ignore header text above the table
            try:
                crop = page.crop((0, top, page.width, page.height))
            except:
                crop = page

            tables = crop.find_tables(table_settings)
            
            for table in tables:
                data = process_table(table, fish_locs)
                
                for entry in data:
                    sym_str = ", ".join(entry['Symbols']) if entry['Symbols'] else "None"
                    f.write(f"WATER:   {entry['Water']}\n")
                    f.write(f"SYMBOLS: {sym_str}\n")
                    f.write(f"REGS:    {entry['Regs']}\n")
                    f.write("-" * 50 + "\n")

    print(f"Done. Saved to {output_path}")

if __name__ == "__main__":
    extract_fishing_regs('fishing_synopsis.pdf', 'final_regs_clean.txt')