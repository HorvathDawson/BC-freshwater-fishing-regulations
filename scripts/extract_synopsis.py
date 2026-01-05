import pdfplumber
import re

# --- CONFIGURATION ---
VERTICAL_GAP_THRESHOLD = 3.0

# Maximum vertical separation in units to consider text part of main content
# (filters legend text that appears far below the main water body name)
MAX_VERTICAL_SEPARATION = 30

# Keywords that indicate map/legend content that should be filtered out
MAP_KEYWORDS = ["legend", "map", "scale", "courtesy"]

# Maximum text length for single-letter map legend entries (e.g., "K")
MAX_SINGLE_LETTER_LENGTH = 2

def clean_text(text): 
    return re.sub(r'\s+', ' ', text).strip() if text else ""

def clean_doubled_chars(text):
    """
    Removes doubled characters only when it appears to be a PDF artifact.
    Only applies the fix if most characters in the text are doubled (e.g., "WWAATTEERR" -> "WATER").
    This avoids removing legitimate double letters like "oo" in "hook" or "ss" in "class".
    """
    if not text or len(text) < 4:
        return text
    
    # Check if this looks like a doubled artifact
    # Count how many character pairs are identical
    doubled_count = 0
    total_pairs = 0
    
    for i in range(0, len(text) - 1, 2):
        if i + 1 < len(text):
            total_pairs += 1
            if text[i] == text[i + 1]:
                doubled_count += 1
    
    # If more than 70% of pairs are doubled, it's likely an artifact
    if total_pairs > 0 and (doubled_count / total_pairs) > 0.7:
        # Remove every other character
        result = ''.join([text[i] for i in range(0, len(text), 2)])
        return result
    
    return text

# --- SPATIAL EXTRACTION HELPERS (Fixed Logic) ---

def extract_visual_lines(page, bbox):
    """
    Extracts text lines from a bbox, filtering out 'ghost' text from 
    adjacent rows using a minimum height threshold.
    Also filters out text that is spatially separated from the main content
    (like legend text appearing at the bottom of a cell).
    """
    if not bbox: return []
    x0, top, x1, bottom = bbox
    
    try: 
        cell_crop = page.crop((x0, top, x1, bottom))
    except ValueError: 
        return []

    words = cell_crop.extract_words(x_tolerance=2, y_tolerance=2, keep_blank_chars=True)
    if not words: return []

    # --- FIX: MINIMUM HEIGHT FILTER ---
    # Discard words with tiny height (< 4.0 units).
    # Real text is > 6.0 units. Artifacts/Bleed are usually < 1.0.
    valid_words = []
    for w in words:
        if (w['bottom'] - w['top']) > 4.0:
            valid_words.append(w)
    # ----------------------------------

    if not valid_words: return []

    # Group words into lines
    lines = []
    current_line = [valid_words[0]]
    for word in valid_words[1:]:
        if abs(word['top'] - current_line[-1]['top']) < 3:
            current_line.append(word)
        else:
            lines.append(current_line)
            current_line = [word]
    lines.append(current_line)

    # Calculate the average vertical position of each line
    line_positions = []
    for line in lines:
        avg_top = sum(w['top'] for w in line) / len(line)
        line_positions.append(avg_top)
    
    # If we have multiple lines, filter out lines that are spatially separated
    # from the main group (likely legend/map text at bottom of cell)
    if len(lines) > 1:
        # Calculate gaps between consecutive lines
        gaps = []
        for i in range(len(line_positions) - 1):
            gap = line_positions[i+1] - line_positions[i]
            gaps.append(gap)
        
        # If there's a large gap (> MAX_VERTICAL_SEPARATION), filter out lines after it
        # This handles cases like "COQUIHALLA RIVER" followed by "TABLE LEGEND:" much lower
        max_gap_idx = None
        max_gap = 0
        for i, gap in enumerate(gaps):
            if gap > max_gap:
                max_gap = gap
                max_gap_idx = i
        
        # Only filter if the largest gap is significantly large
        if max_gap > MAX_VERTICAL_SEPARATION:
            # Keep lines before the large gap, discard lines after
            lines = lines[:max_gap_idx + 1]

    text_lines = []
    for line in lines:
        text = " ".join([w['text'] for w in line])
        text_lines.append(clean_text(text))
    
    return text_lines

def extract_text_by_spatial_layout(page, bbox):
    """
    Extracts regulation text blocks, filtering out 'ghost' text from
    adjacent rows using a minimum height threshold.
    """
    if not bbox: return []
    x0, top, x1, bottom = bbox
    try: 
        cell_crop = page.crop((x0, top, x1, bottom))
    except ValueError: 
        return []

    words = cell_crop.extract_words(x_tolerance=2, y_tolerance=2, keep_blank_chars=True)
    if not words: return []

    # --- FIX: MINIMUM HEIGHT FILTER ---
    valid_words = []
    for w in words:
        if (w['bottom'] - w['top']) > 4.0:
            valid_words.append(w)
    
    words = valid_words
    if not words: return []
    # ----------------------------------

    lines = []
    current_line = [words[0]]
    for word in words[1:]:
        if abs(word['top'] - current_line[-1]['top']) < 3: 
            current_line.append(word)
        else: 
            lines.append(current_line)
            current_line = [word]
    lines.append(current_line)

    spatial_blocks = []
    current_text_block = []
    
    for i, line in enumerate(lines):
        line_text = " ".join([w['text'] for w in line])
        line_text = line_text.replace('\uf0dc', ' [Includes tributaries] ').replace('*', ' [Includes tributaries] ')
        
        if i == 0: 
            current_text_block.append(line_text)
            continue
            
        gap = line[0]['top'] - lines[i-1][0]['bottom']
        if gap > VERTICAL_GAP_THRESHOLD: 
            spatial_blocks.append(" ".join(current_text_block))
            current_text_block = [line_text]
        else: 
            current_text_block.append(line_text)
            
    if current_text_block: 
        spatial_blocks.append(" ".join(current_text_block))
        
    return [clean_text(b) for b in spatial_blocks]

def extract_and_clean_water_name(page, bbox, fish_locations):
    raw_lines = extract_visual_lines(page, bbox)
    if not raw_lines: return "", []

    full_text = " ".join(raw_lines)
    symbols = []

    if '\uf0dc' in full_text or '*' in full_text or "Includes tributaries" in full_text:
        symbols.append("Includes Tributaries")
    if "CW" in full_text and re.search(r'\bCW\b', full_text):
        symbols.append("Classified Waters")
    if bbox:
        x0, top, x1, bottom = bbox
        for (fx, fy) in fish_locations:
            if x0 < fx < x1 and top < fy < bottom:
                symbols.append("Stocked")
                break 

    clean_lines = []
    for i, line in enumerate(raw_lines):
        if line.strip().endswith(":"): continue
        if i > 0 and any(x in line.lower() for x in ["legend", "map", "page ", "see "]): continue
        
        clean_line = line.replace('\uf0dc', '').replace('*', '')
        clean_line = re.sub(r'\[?Includes tributaries\]?', '', clean_line, flags=re.IGNORECASE)
        clean_line = re.sub(r'\bCW\b', '', clean_line)
        
        clean_line = clean_line.strip()
        if clean_line:
            clean_lines.append(clean_line)

    final_name = " ".join(clean_lines).strip()
    final_name = re.sub(r'\s+', ' ', final_name)
    
    return final_name, symbols

def get_table_geometry(page):
    tables = page.find_tables(table_settings={"vertical_strategy": "lines", "horizontal_strategy": "lines"})
    if not tables: return None, None, None, None
    main_table = max(tables, key=lambda t: (t.bbox[2]-t.bbox[0]) * (t.bbox[3]-t.bbox[1]))
    x0, top, x1, bottom = main_table.bbox
    cells = sorted(main_table.cells, key=lambda c: (c[1], c[0]))
    divider = None
    for c in cells:
        if abs(c[0] - x0) < 2 and abs(c[2] - x1) > 5: divider = c[2]; break
    return x0, divider, x1, top

def is_fish_vector(curve):
    """Check if a curve object looks like a fish icon (stocked water indicator)."""
    width = curve['x1'] - curve['x0']
    height = curve['bottom'] - curve['top']
    if not (4 < width < 40) or not (4 < height < 40):
        return False
    ratio = width / height if height > 0 else 0
    return 0.2 < ratio < 5.0

def get_fish_locations(page):
    """Extract the center coordinates of all fish icons on the page."""
    centers = []
    for curve in page.curves:
        if is_fish_vector(curve):
            centers.append(((curve['x0'] + curve['x1']) / 2, (curve['top'] + curve['bottom']) / 2))
    return centers

def extract_rows_from_page(page):
    """
    Extracts rows from a page table into structured dictionaries.
    Returns list of dicts like: {'water': 'LAKE NAME', 'regs': 'regulations text', 'symbols': ['Stocked', 'Classified Waters']}
    Filters out map/legend content and other non-data rows.
    Handles merged cells where one water body name spans multiple regulation rows.
    """
    # Get basic geometry
    geometry = get_table_geometry(page)
    if not geometry or geometry[0] is None:
        return []
    
    x0, divider, x1, top = geometry
    
    # Get fish locations for stocked water detection
    fish_locations = get_fish_locations(page)
    
    # Get the full table to find its bottom boundary
    tables = page.find_tables(table_settings={"vertical_strategy": "lines", "horizontal_strategy": "lines"})
    if not tables:
        return []
    
    main_table = max(tables, key=lambda t: (t.bbox[2]-t.bbox[0]) * (t.bbox[3]-t.bbox[1]))
    table_bottom = main_table.bbox[3]
    
    # Crop page to just the table area
    try:
        table_page = page.crop((x0, top, x1, table_bottom))
    except:
        return []
    
    v_lines = [x0, x1]
    if divider:
        v_lines.append(divider)
    
    table_settings = {
        "vertical_strategy": "explicit",
        "explicit_vertical_lines": v_lines,
        "horizontal_strategy": "lines",
        "intersection_y_tolerance": 10
    }
    
    tables = table_page.find_tables(table_settings)
    if not tables:
        return []
    
    results = []
    current_entry = None
    last_c1_box = None
    
    for table in tables:
        for row in table.rows:
            if len(row.cells) < 2:
                continue
            
            c1, c2 = row.cells[0], row.cells[1]
            
            # If c1 is None, this row is part of a merged cell from above
            # If c2 is None, skip this row (no regulations)
            if not c2:
                continue
            
            # Check if this is a merged cell (c1 is None OR same as previous water body cell)
            is_merged_cell = (c1 is None) or (c1 == last_c1_box)
            
            # Only update last_c1_box if c1 is not None
            if c1 is not None:
                last_c1_box = c1
            
            # Extract text from each cell using spatial layout extraction
            # This preserves multi-line regulation blocks
            # For merged cells (c1 is None), use the water name from current_entry
            if c1 is not None:
                water_lines = extract_visual_lines(page, c1)
                water = " ".join(water_lines) if water_lines else ""
                
                # Detect symbols for this water body
                symbols = []
                full_text = water + " " + " ".join(water_lines)
                
                # Check for "Includes Tributaries" symbol (bullet or asterisk)
                if '\uf0dc' in full_text or '*' in full_text or "Includes tributaries" in full_text:
                    symbols.append("Includes Tributaries")
                
                # Check for "Classified Waters" (CW)
                if "CW" in full_text and re.search(r'\bCW\b', full_text):
                    symbols.append("Classified Waters")
                
                # Check for "Stocked" (fish icon in the cell area)
                if c1:
                    c1_x0, c1_top, c1_x1, c1_bottom = c1
                    for (fx, fy) in fish_locations:
                        if c1_x0 < fx < c1_x1 and c1_top < fy < c1_bottom:
                            symbols.append("Stocked")
                            break
                
                # Clean the water name by removing symbol markers
                # Remove bullet character and asterisk
                water = water.replace('\uf0dc', '').replace('*', '')
                # Remove "CW" marker
                water = re.sub(r'\bCW\b', '', water)
                # Remove "Includes tributaries" text if present
                water = re.sub(r'\[?Includes tributaries\]?', '', water, flags=re.IGNORECASE)
                # Clean up extra whitespace
                water = re.sub(r'\s+', ' ', water).strip()
            else:
                # This is a continuation row - use the water name and symbols from current_entry
                water = current_entry["water"] if current_entry else ""
                symbols = current_entry["symbols"] if current_entry else []
            
            regs_blocks = extract_text_by_spatial_layout(page, c2)
            regs = "\n".join(regs_blocks) if regs_blocks else ""
            
            # Just strip whitespace - don't clean doubled chars to preserve original text
            water = water.strip()
            regs = regs.strip()
            
            # Skip if both are empty
            if not water and not regs:
                continue
            
            # Filter out map/legend content
            # Filter out map/legend content
            # Only filter if the WATER NAME itself contains map keywords (not regulations)
            # This allows legitimate regulations like "(see map on page 24)" to pass through
            water_lower = water.lower()
            is_map_keyword = any(keyword in water_lower for keyword in MAP_KEYWORDS)
            
            # Check for very short single-letter content (likely map legend like "K")
            is_single_letter = (len(water.strip()) <= MAX_SINGLE_LETTER_LENGTH and len(regs.strip()) <= MAX_SINGLE_LETTER_LENGTH)
            
            # Skip this row if it looks like map content
            if is_map_keyword or is_single_letter:
                continue
            
            # Handle merged cells: If this row shares the same water body cell as the previous row,
            # append regulations to the current entry instead of creating a new one
            if is_merged_cell and current_entry:
                # Append regulations from this row to the current entry
                if regs:
                    if current_entry["regs"]:
                        current_entry["regs"] += "\n" + regs
                    else:
                        current_entry["regs"] = regs
            else:
                # This is a new water body - finalize the previous entry if any
                if current_entry:
                    results.append(current_entry)
                
                # Start a new entry
                current_entry = {
                    "water": water,
                    "regs": regs,
                    "symbols": symbols
                }
    
    # Don't forget the last entry!
    if current_entry:
        results.append(current_entry)
    
    return results

def get_table_text_sizes(page):
    """
    Gets all unique text sizes in the data portion of the table (excluding maps/graphics).
    Returns sorted list of font sizes found in the table.
    """
    geometry = get_table_geometry(page)
    if not geometry or geometry[0] is None:
        return []
    
    x0, divider, x1, top = geometry
    
    # Get the full table bbox
    tables = page.find_tables(table_settings={"vertical_strategy": "lines", "horizontal_strategy": "lines"})
    if not tables:
        return []
    
    main_table = max(tables, key=lambda t: (t.bbox[2]-t.bbox[0]) * (t.bbox[3]-t.bbox[1]))
    
    # Find the last valid data row by checking for map content
    last_valid_y = main_table.bbox[3]
    
    # Check rows from bottom up to find where map content starts
    for row in reversed(main_table.rows):
        if len(row.cells) < 2:
            continue
        
        c1, c2 = row.cells[0], row.cells[1]
        if not c1 or not c2:
            continue
        
        # Extract text from cells
        try:
            water = page.crop(c1).extract_text() or ""
            regs = page.crop(c2).extract_text() or ""
            combined = (water + " " + regs).lower().strip()
            
            # Check if this looks like map content
            is_map = any(k in combined for k in MAP_KEYWORDS)
            is_single_letter = len(water.strip()) <= MAX_SINGLE_LETTER_LENGTH and len(regs.strip()) <= MAX_SINGLE_LETTER_LENGTH
            
            if is_map or is_single_letter:
                # This row has map content, so valid data ends above this
                last_valid_y = c1[1]  # Top of this cell
            else:
                # Found a valid data row, stop searching
                break
        except:
            continue
    
    # Crop to table area excluding map content
    try:
        table_crop = page.crop((x0, top, x1, last_valid_y))
        chars = table_crop.chars
        if not chars:
            return []
        
        sizes = set()
        for char in chars:
            if 'size' in char:
                sizes.add(round(char['size'], 1))
        
        return sorted(list(sizes))
    except:
        return []