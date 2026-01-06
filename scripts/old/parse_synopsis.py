import requests
import pdfplumber
import re
import json
import os
from reg_parser import RegParser, merge_orphaned_details

# --- CONFIGURATION ---
PDF_URL = "https://www2.gov.bc.ca/assets/gov/sports-recreation-arts-and-culture/outdoor-recreation/fishing-and-hunting/freshwater-fishing/fishing_synopsis.pdf"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

PDF_FILENAME = os.path.join(OUTPUT_DIR, "fishing_synopsis.pdf")
TXT_OUTPUT = os.path.join(OUTPUT_DIR, "fishing_regs.txt")
JSON_OUTPUT = os.path.join(OUTPUT_DIR, "fishing_data.json")
DROPPED_OUTPUT = os.path.join(OUTPUT_DIR, "dropped_lines.txt")
SANITY_OUTPUT = os.path.join(OUTPUT_DIR, "sanity_check_log.txt")

# --- VALID REGIONS WHITELIST ---
VALID_REGIONS = {
    "1": "Vancouver Island", "2": "Lower Mainland", "3": "Thompson-Nicola",
    "4": "Kootenay", "5": "Cariboo", "6": "Skeena",
    "7A": "Omineca", "7B": "Peace", "8": "Okanagan"
}

# --- GLOBAL FILTERING ---
INVALID_KEYWORDS = [
    "courtesy of", "purchase a larger map", "reprinted", "Haig-Brown",
    "www.", ".ca", ".com", "Department of Fisheries", "Management Unit",
    "Please refer to", "Check website for", "Regulation Changes",
    "front cover", "back cover"
]

VERTICAL_GAP_THRESHOLD = 3.0 

# --- HELPERS (String & Logic) ---

def clean_text(text): 
    return re.sub(r'\s+', ' ', text).strip() if text else ""

def get_clean_key_name(raw_name):
    if not raw_name: return ""
    clean = re.sub(r'\(.*?\)', '', raw_name)
    clean = clean.replace('“', '').replace('”', '').replace('"', '').replace("'", "")
    clean = re.sub(r'\s+', ' ', clean).strip().lower()
    return clean

def extract_all_mus(text):
    mu_pattern = r'\b(\d{1,2}-\d{1,2})\b'
    mus = re.findall(mu_pattern, text)
    clean_name = re.sub(mu_pattern, ' ', text)
    clean_name = re.sub(r'\s+', ' ', clean_name).strip()
    return clean_name, mus

def is_mu_line(text):
    leftover = re.sub(r'[\d\-\,\s]', '', text)
    return len(leftover) < 2 and re.search(r'\d{1,2}-\d{1,2}', text)

def deduplicate_regs(regs_list):
    seen = set(); unique = []
    for r in regs_list:
        comp = (r['type'], r['details'].strip())
        if comp not in seen: seen.add(comp); unique.append(r)
    return unique

def clean_doubled_chars(text):
    if not text: return ""
    return re.sub(r'(.)\1', r'\1', text)

def check_invalid_row(name, regs):
    combined = (name + " " + " ".join(regs)).lower()
    combined = re.sub(r'\s+', ' ', combined)
    for k in INVALID_KEYWORDS:
        if k.lower() in combined: return f"Contains invalid keyword: '{k}'"
    if "water body" in name.lower(): return "Header row detected"
    if not name.strip() and not regs: return "Empty row"
    return None

def validate_entry(current_entry, previous_entry, is_merged_cell):
    """
    Smart validation to detect bleed errors while ignoring safe 'Tributaries' entries.
    Provides detailed output for debugging.
    """
    warnings = []
    if not previous_entry:
        return warnings

    curr_name = current_entry['unprocessed_name'].upper()
    prev_name = previous_entry['unprocessed_name'].upper()
    
    # --- CHECK 1: NAME LEAK ---
    # We flag if the previous name is inside the current name.
    if len(prev_name) > 4 and prev_name in curr_name:
        # Get the extra text (the suffix)
        suffix = curr_name.replace(prev_name, "").strip()
        
        # Safe keywords that denote a valid sub-listing
        safe_suffixes = [
            "TRIBUTARIES", "PARK", "INLET", "OUTLET", "ABOVE", "BELOW", 
            "INTERPRETIVE FOREST", "RESERVOIR", "WATERSHED", "CREEK", "RIVER",
            "OXBOWS", "SLOUGH", "CHANNEL", "SYSTEM", "WEST", "EAST", "NORTH", "SOUTH"
        ]
        
        # We also allow simple punctuation leftovers like "'S" or "()"
        clean_suffix = re.sub(r'[^A-Z]', '', suffix) # Remove spaces/punctuation
        
        is_safe = False
        for safe in safe_suffixes:
            if safe in suffix or safe in clean_suffix:
                is_safe = True
                break
        
        # If the names are identical (Suffix is empty), it's a Duplicate Row, not a merge error.
        if not suffix:
             # We ignore this here because the duplicate reg check usually catches it,
             # or it's a valid split table. But if you want to see it:
             pass 
        elif not is_safe:
            warnings.append(
                f"[POSSIBLE MERGE ERROR] Current Name '{curr_name}' contains Previous Name '{prev_name}'.\n"
                f"          -> Unsafe Suffix: '{suffix}'"
            )

    # --- CHECK 2: SUSPICIOUS REGULATION DUPLICATION ---
    curr_regs = current_entry['original_reg_text'].strip()
    prev_regs = previous_entry['original_reg_text'].strip()
    
    if curr_regs and prev_regs and (curr_regs == prev_regs) and not is_merged_cell:
        # Only flag if the duplicated text is LONG (>60 chars)
        if len(curr_regs) > 60:
            warnings.append(
                f"[SUSPICIOUS DUPLICATE] Current Water '{curr_name}' has identical regs to Previous Water '{prev_name}'.\n"
                f"          -> Length: {len(curr_regs)} chars\n"
                f"          -> FULL TEXT: {curr_regs}"
            )

    return warnings
# --- SPATIAL EXTRACTION HELPERS (Fixed Logic) ---

def extract_visual_lines(page, bbox):
    """
    Extracts text lines from a bbox, filtering out 'ghost' text from 
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
    # Discard words with tiny height (< 4.0 units).
    # Real text is > 6.0 units. Artifacts/Bleed are usually < 1.0.
    valid_words = []
    for w in words:
        if (w['bottom'] - w['top']) > 4.0:
            valid_words.append(w)
    # ----------------------------------

    if not valid_words: return []

    lines = []
    current_line = [valid_words[0]]
    for word in valid_words[1:]:
        if abs(word['top'] - current_line[-1]['top']) < 3:
            current_line.append(word)
        else:
            lines.append(current_line)
            current_line = [word]
    lines.append(current_line)

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

# --- MAIN PROCESSING ---

def process_table(page, table, fish_locations, dropped_entries, sanity_warnings, page_num):
    structured_data = []
    current_entry = None
    last_c2_box = None 
    last_processed_entry = None 

    text_rows = table.extract()
    row_objs = table.rows 

    if len(text_rows) != len(row_objs): return []

    for i, row in enumerate(row_objs):
        if not row.cells or len(row.cells) < 2: continue
        
        c1_box = row.cells[0]
        c1_raw = text_rows[i][0] or "" 
        c2_box = row.cells[1]
        
        is_duplicate_regs = (c2_box == last_c2_box)
        last_c2_box = c2_box 

        if is_duplicate_regs:
            c2_raw_list = []
            structured_regs = []
        else:
            c2_raw_list = extract_text_by_spatial_layout(page, c2_box)
            structured_regs = []
            for raw_reg in c2_raw_list:
                parsed_list = RegParser.parse_reg(raw_reg)
                structured_regs.extend(parsed_list)

        name_text, c1_syms = extract_and_clean_water_name(page, c1_box, fish_locations)

        fail_reason = check_invalid_row(name_text, c2_raw_list)
        if fail_reason:
            if name_text or c2_raw_list:
                dropped_entries.append(f"[{fail_reason}] NAME: {name_text} | REGS: {' '.join(c2_raw_list)[:50]}...")
            continue

        if name_text:
            if is_mu_line(name_text):
                if current_entry:
                    _, extra_mus = extract_all_mus(name_text)
                    current_entry["management_units"].extend(extra_mus)
                    if c1_syms:
                        current_entry["Symbols"] = list(set(current_entry["Symbols"] + c1_syms))
                    if structured_regs:
                        current_entry["regs"].extend(structured_regs)
                        for raw_line in c2_raw_list:
                            if raw_line not in current_entry["original_reg_text"]:
                                current_entry["original_reg_text"] += "\n" + raw_line
            else:
                final_name, mus = extract_all_mus(name_text)
                
                # --- FINALIZE PREVIOUS ENTRY ---
                if current_entry: 
                    current_entry["regs"] = merge_orphaned_details(deduplicate_regs(current_entry["regs"]))
                    
                    # Run sanity check with correct page number
                    if last_processed_entry:
                        warns = validate_entry(current_entry, last_processed_entry, is_merged_cell=False)
                        for w in warns:
                            sanity_warnings.append(f"PAGE {page_num} | {w}")

                    structured_data.append(current_entry)
                    last_processed_entry = current_entry
                
                raw_text_block = "\n".join(c2_raw_list)
                
                current_entry = {
                    "unprocessed_name": final_name,
                    "management_units": mus,
                    "Symbols": c1_syms,
                    "original_reg_text": raw_text_block,
                    "regs": structured_regs,
                    "page": page_num # Ensure page is available immediately
                }
        elif current_entry and structured_regs:
            current_entry["regs"].extend(structured_regs)
            for raw_line in c2_raw_list:
                if raw_line not in current_entry["original_reg_text"]:
                    current_entry["original_reg_text"] += "\n" + raw_line
            if c1_syms:
                current_entry["Symbols"] = list(set(current_entry["Symbols"] + c1_syms))

    if current_entry: 
        current_entry["regs"] = merge_orphaned_details(deduplicate_regs(current_entry["regs"]))
        if last_processed_entry:
            warns = validate_entry(current_entry, last_processed_entry, is_merged_cell=False)
            for w in warns:
                sanity_warnings.append(f"PAGE {page_num} | {w}")
        structured_data.append(current_entry)
        
    return structured_data

# --- GEOMETRY & DOWNLOAD HELPERS ---

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
    width = curve['x1'] - curve['x0']; height = curve['bottom'] - curve['top']
    if not (4 < width < 40) or not (4 < height < 40): return False
    ratio = width / height if height > 0 else 0
    return 0.2 < ratio < 5.0

def get_fish_locations(page):
    centers = []
    for curve in page.curves:
        if is_fish_vector(curve): centers.append(((curve['x0'] + curve['x1']) / 2, (curve['top'] + curve['bottom']) / 2))
    return centers

def get_header_region(page_obj):
    header_crop = page_obj.crop((0, 0, page_obj.width, page_obj.height * 0.20))
    raw_text = header_crop.extract_text() or ""
    match = re.search(r'REGION\s+(\d+[A-Z]?)', raw_text, re.IGNORECASE)
    if not match:
        clean_text = clean_doubled_chars(raw_text)
        match = re.search(r'REGION\s+(\d+[A-Z]?)', clean_text, re.IGNORECASE)
    if match:
        region_id = match.group(1).upper()
        if region_id in VALID_REGIONS: return f"REGION {region_id} - {VALID_REGIONS[region_id]}"
    return None

def download_pdf(url, filename):
    if os.path.exists(filename): return
    print(f"Downloading PDF from {url}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, stream=True, headers=headers); response.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192): f.write(chunk)
    except Exception as e: print(f"Error: {e}"); exit()

# --- MAIN EXECUTION ---

def extract_fishing_data():
    download_pdf(PDF_URL, PDF_FILENAME)
    print(f"Scanning {PDF_FILENAME}...")
    
    all_regions_data = {} 
    last_geom = None 
    current_region_name = "General Information"
    
    dropped_entries = []
    sanity_warnings = []

    with pdfplumber.open(PDF_FILENAME) as pdf, open(TXT_OUTPUT, "w", encoding="utf-8") as f_txt:
        number_pages_skip_beginning = 10
        pages = pdf.pages[number_pages_skip_beginning:]
        
        for i, page in enumerate(pages):
            page_num = number_pages_skip_beginning + i + 1
            found_region = get_header_region(page)
            if not found_region:
                page_text = page.extract_text() or ""
                if "CONTACT INFORMATION" in page_text and i + 1 < len(pages):
                    next_page_region = get_header_region(pages[i+1])
                    if next_page_region: found_region = next_page_region

            if found_region and found_region != current_region_name:
                print(f"Switching Region from '{current_region_name}' to '{found_region}' at Page {page_num}")
                current_region_name = found_region
            if current_region_name not in all_regions_data: all_regions_data[current_region_name] = {}

            page_text = page.extract_text() or ""
            if "EXCEPTIONS" not in page_text: continue

            f_txt.write(f"\n{'='*20} PAGE {page_num} ({current_region_name}) {'='*20}\n")
            fish_locs = get_fish_locations(page)
            geom = get_table_geometry(page)
            
            if geom[1]: last_geom = geom
            elif last_geom:
                x0, _, x1, top = geom if geom[0] else last_geom
                divider = last_geom[1]; geom = (x0, divider, x1, top)
            else: continue 

            x0, divider, x1, top = geom
            table_settings = { "vertical_strategy": "explicit", "explicit_vertical_lines": [x0, divider, x1], "horizontal_strategy": "lines", "intersection_y_tolerance": 10, "text_x_tolerance": 2 }
            try: crop = page.crop((0, top, page.width, page.height))
            except: crop = page
            tables = crop.find_tables(table_settings)
            
            for table in tables:
                # Pass page_num so sanity checks have context
                data = process_table(page, table, fish_locs, dropped_entries, sanity_warnings, page_num)
                
                for entry in data:
                    clean_key = get_clean_key_name(entry['unprocessed_name'])
                    if clean_key not in all_regions_data[current_region_name]: all_regions_data[current_region_name][clean_key] = []
                    all_regions_data[current_region_name][clean_key].append(entry)

                    mu_str = f" (MUs: {', '.join(entry['management_units'])})" if entry['management_units'] else ""
                    sym_str = ", ".join(entry['Symbols']) if entry['Symbols'] else "None"
                    
                    raw_text_pretty = entry['original_reg_text'].replace('\n', '\n          ')
                    
                    f_txt.write(f"WATER:   {entry['unprocessed_name']}{mu_str}\n")
                    f_txt.write(f"PAGE:    {entry['page']}\n")
                    f_txt.write(f"SYMBOLS: {sym_str}\n")
                    f_txt.write(f"ORIGINAL:\n         {raw_text_pretty}\n")
                    f_txt.write("REGS:\n")
                    
                    for r in entry['regs']:
                        f_txt.write(f"          * {r['details']}\n")
                    
                    f_txt.write("-" * 50 + "\n")

    with open(DROPPED_OUTPUT, "w", encoding="utf-8") as f_drop: f_drop.write("\n".join(dropped_entries))
    
    with open(SANITY_OUTPUT, "w", encoding="utf-8") as f_sanity:
        if sanity_warnings:
            f_sanity.write(f"FOUND {len(sanity_warnings)} POTENTIAL ERRORS:\n")
            f_sanity.write("\n".join(sanity_warnings))
        else:
            f_sanity.write("No suspicious patterns detected.")

    final_regions_data = {k: v for k, v in all_regions_data.items() if len(v) > 0}
    final_json = { "regionsData": final_regions_data }
    with open(JSON_OUTPUT, 'w', encoding='utf-8') as f_json: json.dump(final_json, f_json, indent=4, ensure_ascii=False)
    
    print(f"Extraction complete.")
    print(f"Dropped lines log: {DROPPED_OUTPUT}")
    print(f"Sanity check log:  {SANITY_OUTPUT}")
    print(f"Text report:       {TXT_OUTPUT}")
    print(f"JSON Data:         {JSON_OUTPUT}")

if __name__ == "__main__":
    extract_fishing_data()