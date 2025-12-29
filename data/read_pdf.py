import pdfplumber

def clean_text(text):
    if not text: return ""
    return text.replace('\uf0dc', ' [SYMBOL/FISH] ').replace('\n', ' ').strip()

def get_table_geometry(page):
    """
    Finds the largest table on the page and calculates the divider 
    based on the first non-merged row's cells.
    """
    # 1. FIND TABLES
    # We use 'lines' strategy which is generally most accurate for these grids
    tables = page.find_tables(table_settings={
        "vertical_strategy": "lines", 
        "horizontal_strategy": "lines"
    })

    if not tables:
        return None, None, None, None

    # 2. IDENTIFY THE BIGGEST TABLE
    biggest_table = max(tables, key=lambda t: (t.bbox[2] - t.bbox[0]) * (t.bbox[3] - t.bbox[1]))
    
    x0, top, x1, bottom = biggest_table.bbox
    left_edge = x0
    right_edge = x1
    table_start_y = top

    # 3. FIND DIVIDER USING TABLE CELLS
    # The table object already knows where the cells are. 
    # We just need the X-coordinate of the right side of the first column.
    divider = None
    
    # Sort cells by vertical position (top) then horizontal (left)
    # biggest_table.cells is a list of tuples: (x0, top, x1, bottom)
    cells = sorted(biggest_table.cells, key=lambda c: (c[1], c[0]))

    for cell in cells:
        cell_x0, cell_top, cell_x1, cell_bottom = cell
        
        # Check if this cell starts at the left margin of the table
        # (Allowing a tiny 2px variance for float precision)
        if abs(cell_x0 - left_edge) < 2:
            
            # CRITICAL CHECK: Does this cell span the entire width? (Merged Header)
            # If so, skip it. We need a row that is split.
            if abs(cell_x1 - right_edge) < 5:
                continue
            
            # Found a split cell! The divider is the right edge of this cell.
            divider = cell_x1
            break

    return left_edge, divider, right_edge, table_start_y

def process_table_data(table):
    structured_data = []
    current_entry = None

    for row in table:
        if not row or len(row) < 2: continue
        
        col1 = clean_text(row[0])
        col2 = clean_text(row[1])

        if not col1 and not col2: continue
        if "WATER BODY" in col1 or "EXCEPTIONS" in col2: continue
        if "Legend" in col1: continue

        # Merging Logic
        if col1:
            if current_entry: structured_data.append(current_entry)
            current_entry = {"Water/Unit": col1, "Regs": col2}
        elif current_entry and col2:
            current_entry["Regs"] += "$" + col2

    if current_entry: structured_data.append(current_entry)
    return structured_data

def extract_fishing_regs_final(pdf_path, output_txt_path):
    print(f"Scanning {pdf_path}...")
    
    last_geom = None 

    with pdfplumber.open(pdf_path) as pdf, open(output_txt_path, "w", encoding="utf-8") as f:
        
        for page in pdf.pages[14:]: 
            text_check = page.extract_text()
            if not text_check or "EXCEPTIONS" not in text_check: continue

            f.write(f"\n{'='*20} PAGE {page.page_number} {'='*20}\n")
            
            # 1. Get Geometry
            left, divider, right, start_y = get_table_geometry(page)
            
            # 2. Logic to handle detection failures (Inheritance)
            if left and right and divider:
                # CASE A: Perfect detection
                last_geom = (left, divider, right)
                
            elif left and right and not divider:
                # CASE B: Found table borders, but logic couldn't determine divider
                if last_geom:
                    print(f"  Pg {page.page_number}: Found borders, inheriting divider.")
                    _, old_div, _ = last_geom
                    divider = old_div
                    # Update geometry with current borders but old divider
                    last_geom = (left, divider, right)
                else:
                    print(f"  Pg {page.page_number}: Found borders but no divider/history. Skipping.")
                    continue

            elif not left:
                # CASE C: No table found at all
                if last_geom:
                    print(f"  Pg {page.page_number}: No table structure. Inheriting layout.")
                    left, divider, right = last_geom
                    start_y = 50 
                else:
                    print(f"  Pg {page.page_number}: No table found and no history. Skipping.")
                    continue

            # 3. Define Precise Explicit Lines
            vertical_lines = [left, divider, right]
            
            # 4. Crop & Extract
            try:
                table_area = page.crop((0, start_y, page.width, page.height))
            except ValueError:
                table_area = page

            table_settings = {
                "vertical_strategy": "explicit", 
                "explicit_vertical_lines": vertical_lines,
                "horizontal_strategy": "lines", 
                "intersection_y_tolerance": 10,
                "text_x_tolerance": 2, 
            }

            tables = table_area.extract_tables(table_settings)

            if not tables:
                print(f"  Pg {page.page_number}: Geometry found, but extraction failed.")
            else:
                print(f"  Pg {page.page_number}: Success! Found {len(tables)} tables.")

            for table in tables:
                data = process_table_data(table)
                for entry in data:
                    f.write(f"WATER: {entry['Water/Unit']}\n")
                    f.write(f"REGS:  {entry['Regs']}\n")
                    f.write("-" * 50 + "\n")

    print(f"Extraction complete.")

if __name__ == "__main__":
    extract_fishing_regs_final('fishing_synopsis.pdf', 'final_regs_output.txt')