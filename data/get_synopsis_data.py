import requests
import pdfplumber
import re
import json
import os

# --- CONFIGURATION ---
PDF_URL = "https://www2.gov.bc.ca/assets/gov/sports-recreation-arts-and-culture/outdoor-recreation/fishing-and-hunting/freshwater-fishing/fishing_synopsis.pdf"
PDF_FILENAME = "fishing_synopsis.pdf"
TXT_OUTPUT = "fishing_regs.txt"
JSON_OUTPUT = "fishing_data.json"

# Keywords to filter out map text/junk rows
INVALID_KEYWORDS = [
    "Kilometres", "courtesy of", "purchase a larger map", 
    "reprinted", "Haig-Brown", "scale", "www.", ".ca", ".com",
    "Department of Fisheries", "Management Unit", "Road", "Hwy",
    "Please refer to", "Check website for", "Species"
]

def download_pdf(url, filename):
    if os.path.exists(filename):
        print(f"{filename} already exists. Skipping download.")
        return
    print(f"Downloading PDF from {url}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print("Download complete.")
    except Exception as e:
        print(f"Error downloading PDF: {e}")
        exit()

# --- HELPER FUNCTIONS ---

def is_fish_vector(curve):
    """ Detects the vector fish icon based on size and shape. """
    width = curve['x1'] - curve['x0']
    height = curve['bottom'] - curve['top']
    if not (4 < width < 40) or not (4 < height < 40): return False
    ratio = width / height if height > 0 else 0
    return 0.2 < ratio < 5.0

def get_fish_locations(page):
    centers = []
    for curve in page.curves:
        if is_fish_vector(curve):
            cx = (curve['x0'] + curve['x1']) / 2
            cy = (curve['top'] + curve['bottom']) / 2
            centers.append((cx, cy))
    return centers

def clean_text(text):
    if not text: return ""
    text = text.replace('\n', ' ').strip()
    return re.sub(r'\s+', ' ', text)

def extract_management_unit(text):
    """
    Extracts patterns like '1-15', '5-2', '7-20' from the text.
    Returns (cleaned_name, mu_found).
    """
    # Regex looks for digit-digit at the end of the string or surrounded by spaces
    # We use a non-greedy match for the name part
    match = re.search(r'(.*?)\b(\d{1,2}-\d{1,2})\b(.*)', text)
    
    if match:
        # Group 1: Name before MU
        # Group 2: The MU (e.g. 1-15)
        # Group 3: Anything after (rare, but handles stray chars)
        
        name_part = match.group(1).strip()
        mu = match.group(2).strip()
        suffix = match.group(3).strip()
        
        # Recombine name if there was suffix text (unlikely but safe)
        final_name = f"{name_part} {suffix}".strip()
        return final_name, mu
    
    return text, None

def is_valid_row(name, regs):
    combined = (name + " " + regs).lower()
    if any(k.lower() in combined for k in INVALID_KEYWORDS): return False
    if "WATER BODY" in name or "EXCEPTIONS" in regs: return False
    if not name.strip() and not regs.strip(): return False
    return True

def parse_water_col(text, bbox, fish_locations):
    text = clean_text(text)
    symbols = []
    
    # 1. Text Symbols
    if '\uf0dc' in text or '*' in text:
        symbols.append("Includes Tributaries")
        text = text.replace('\uf0dc', '').replace('*', '')

    if "CW" in text and re.search(r'\bCW\b', text):
        symbols.append("Classified Waters")
        text = re.sub(r'\bCW\b', '', text)

    # 2. Vector Symbols
    if bbox:
        x0, top, x1, bottom = bbox
        for (fx, fy) in fish_locations:
            if x0 < fx < x1 and top < fy < bottom:
                symbols.append("Stocked")
                break 

    return text.strip(), symbols

def parse_regs_col(text):
    text = clean_text(text)
    text = text.replace('\uf0dc', ' [Includes tributaries] ').replace('*', ' [Includes tributaries] ')
    return re.sub(r'\s+', ' ', text).strip()

def get_table_geometry(page):
    tables = page.find_tables(table_settings={"vertical_strategy": "lines", "horizontal_strategy": "lines"})
    if not tables: return None, None, None, None

    main_table = max(tables, key=lambda t: (t.bbox[2]-t.bbox[0]) * (t.bbox[3]-t.bbox[1]))
    x0, top, x1, bottom = main_table.bbox
    
    cells = sorted(main_table.cells, key=lambda c: (c[1], c[0]))
    divider = None
    for c in cells:
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
        
        c1_box = row.cells[0]
        c1_raw = text_rows[i][0] or ""
        c2_raw = text_rows[i][1] if len(text_rows[i]) > 1 else ""

        name_text, c1_syms = parse_water_col(c1_raw, c1_box, fish_locations)
        regs = parse_regs_col(c2_raw)

        if not is_valid_row(name_text, regs): continue

        if name_text:
            # Extract MU from the clean name
            final_name, mu = extract_management_unit(name_text)
            
            if current_entry: structured_data.append(current_entry)
            
            current_entry = {
                "Water": final_name,
                "MU": mu,
                "Symbols": c1_syms,
                "Regs": regs
            }
        elif current_entry and regs:
            current_entry["Regs"] += " " + regs
            # Check for latent symbols/MU in continuation lines (rare but possible)
            _, extra_syms = parse_water_col(c1_raw, c1_box, fish_locations)
            if extra_syms:
                current_entry["Symbols"] = list(set(current_entry["Symbols"] + extra_syms))

    if current_entry: structured_data.append(current_entry)
    return structured_data

def get_region_name(text):
    """
    Scans page text for 'REGION X - Name'.
    Returns strictly the clean region name or None.
    """
    # Regex stops capturing at the first newline or double space to avoid junk
    match = re.search(r'(REGION\s+\d+[A-Z]?\s*[-–]\s*[^\n\r]+)', text)
    if match:
        raw_region = match.group(1).strip()
        # Post-process: Remove common noise if regex grabbed too much
        # Split by newline if it somehow got in, or common next-line headers
        clean = re.split(r'\n|CONTACT|Regional|Water-Specific', raw_region)[0]
        return clean.strip()
    return None

# --- MAIN LOGIC ---

def extract_fishing_data():
    download_pdf(PDF_URL, PDF_FILENAME)
    print(f"Scanning {PDF_FILENAME}...")
    
    # We use a temp dict to collect data, then filter empty ones later
    # Format: { "Region Name": { "water_key": {data} } }
    all_regions_data = {} 
    
    last_geom = None 
    current_region_name = "General Information" # Default fallback

    with pdfplumber.open(PDF_FILENAME) as pdf, open(TXT_OUTPUT, "w", encoding="utf-8") as f_txt:
        
        # Start scanning from page 14 where tables typically start
        for page in pdf.pages: 
            page_text = page.extract_text() or ""
            
            # 1. DETECT REGION NAME
            # We check every page. If a new header appears, we switch regions.
            # If not, we assume we are still in the previous region.
            found_region = get_region_name(page_text)
            if found_region:
                current_region_name = found_region
                
            # Initialize dict if new
            if current_region_name not in all_regions_data:
                all_regions_data[current_region_name] = {}

            # Skip pages that clearly aren't tables
            if "EXCEPTIONS" not in page_text: 
                continue

            f_txt.write(f"\n{'='*20} PAGE {page.page_number} ({current_region_name}) {'='*20}\n")
            
            fish_locs = get_fish_locations(page)
            geom = get_table_geometry(page)
            
            # Geometry Inheritance
            if geom[1]: 
                last_geom = geom
            elif last_geom:
                x0, _, x1, top = geom if geom[0] else last_geom
                divider = last_geom[1]
                geom = (x0, divider, x1, top)
            else:
                continue 

            # Extract Table
            x0, divider, x1, top = geom
            table_settings = {
                "vertical_strategy": "explicit", 
                "explicit_vertical_lines": [x0, divider, x1],
                "horizontal_strategy": "lines", 
                "intersection_y_tolerance": 10,
                "text_x_tolerance": 2, 
            }
            
            try:
                crop = page.crop((0, top, page.width, page.height))
            except:
                crop = page

            tables = crop.find_tables(table_settings)
            
            for table in tables:
                data = process_table(table, fish_locs)
                
                for entry in data:
                    water_name = entry['Water']
                    mu = entry['MU']
                    
                    # 1. Write to Text File (Human Readable)
                    sym_str = ", ".join(entry['Symbols']) if entry['Symbols'] else "None"
                    mu_str = f" (MU: {mu})" if mu else ""
                    
                    f_txt.write(f"WATER:   {water_name}{mu_str}\n")
                    f_txt.write(f"SYMBOLS: {sym_str}\n")
                    f_txt.write(f"REGS:    {entry['Regs']}\n")
                    f_txt.write("-" * 50 + "\n")

                    # 2. Add to Memory (JSON Structure)
                    # Normalize key to lowercase
                    water_key = water_name.lower()
                    
                    # If this water body already exists in this region (split across pages), append regs
                    if water_key in all_regions_data[current_region_name]:
                        existing = all_regions_data[current_region_name][water_key]
                        # Merge regs
                        if entry['Regs'] not in existing['regs']:
                            existing['regs'].append(entry['Regs'])
                        # Merge symbols
                        existing['symbols'] = list(set(existing['symbols'] + entry['Symbols']))
                        # Keep MU if missing (rare)
                        if not existing.get('management_unit') and mu:
                            existing['management_unit'] = mu
                    else:
                        # Create new entry
                        all_regions_data[current_region_name][water_key] = {
                            "symbols": entry['Symbols'],
                            "regs": [entry['Regs']],
                            "management_unit": mu
                        }

    # --- FINAL JSON CLEANUP ---
    # Filter out regions that ended up having no water bodies (headers only)
    final_regions_data = {k: v for k, v in all_regions_data.items() if v}

    final_json = {
        "regionsData": final_regions_data,
        "regionOverviews": {} # Placeholder structure as requested
    }

    with open(JSON_OUTPUT, 'w', encoding='utf-8') as f_json:
        json.dump(final_json, f_json, indent=4, ensure_ascii=False)

    print(f"Extraction complete.")
    print(f"Text report: {TXT_OUTPUT}")
    print(f"JSON data:   {JSON_OUTPUT}")

if __name__ == "__main__":
    extract_fishing_data()