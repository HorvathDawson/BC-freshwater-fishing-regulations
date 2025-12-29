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
    match = re.search(r'(.*?)\b(\d{1,2}-\d{1,2})\b(.*)', text)
    if match:
        name_part = match.group(1).strip()
        mu = match.group(2).strip()
        suffix = match.group(3).strip()
        final_name = f"{name_part} {suffix}".strip()
        return final_name, mu
    return text, None

def is_valid_row(name, regs):
    combined = (name + " " + regs).lower()
    if any(k.lower() in combined for k in INVALID_KEYWORDS): return False
    if "WATER BODY" in name or "EXCEPTIONS" in regs: return False
    if not name.strip() and not regs.strip(): return False
    return True

def split_regulations(text):
    """
    Splits a regulation string into a list based on newlines and semicolons.
    Cleans up whitespace for each item.
    """
    if not text: return []
    # Split by newline OR semicolon
    parts = re.split(r'[;\n]', text)
    # Clean and filter empty strings
    cleaned = [p.strip() for p in parts if p.strip()]
    return cleaned

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
    # We do NOT remove newlines here immediately, because we want to split by them later
    text = text.replace('\uf0dc', ' [Includes tributaries] ').replace('*', ' [Includes tributaries] ')
    return text.strip()

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
        # Note: We pass raw text to parse_regs_col to preserve newlines for splitting later
        regs = parse_regs_col(c2_raw)

        if not is_valid_row(name_text, regs): continue

        if name_text:
            final_name, mu = extract_management_unit(name_text)
            
            if current_entry: structured_data.append(current_entry)
            
            current_entry = {
                "Water": final_name,
                "MU": mu,
                "Symbols": c1_syms,
                "Regs": regs 
            }
        elif current_entry and regs:
            # Append with a newline separator to ensure clean splitting
            current_entry["Regs"] += "\n" + regs
            
            _, extra_syms = parse_water_col(c1_raw, c1_box, fish_locations)
            if extra_syms:
                current_entry["Symbols"] = list(set(current_entry["Symbols"] + extra_syms))

    if current_entry: structured_data.append(current_entry)
    return structured_data

def get_region_name(text):
    """
    Scans page text for 'REGION X - Name'.
    Cleanly extracts strictly the region name.
    """
    # Regex captures REGION + Number + Dash + Name
    # Stops at newline or specific keywords
    match = re.search(r'(REGION\s+\d+[A-Z]?\s*[-–]\s*[^\n\r]+)', text)
    if match:
        raw_region = match.group(1).strip()
        # Split by newline or common junk headers that follow immediately
        clean = re.split(r'(\n|CONTACT|Regional|Water-Specific|General)', raw_region)[0]
        return clean.strip()
    return None

# --- MAIN LOGIC ---

def extract_fishing_data():
    download_pdf(PDF_URL, PDF_FILENAME)
    print(f"Scanning {PDF_FILENAME}...")
    
    all_regions_data = {} 
    last_geom = None 
    current_region_name = "General Information"

    with pdfplumber.open(PDF_FILENAME) as pdf, open(TXT_OUTPUT, "w", encoding="utf-8") as f_txt:
        
        for page in pdf.pages: 
            page_text = page.extract_text() or ""
            
            # 1. DETECT REGION
            found_region = get_region_name(page_text)
            if found_region:
                current_region_name = found_region
                
            if current_region_name not in all_regions_data:
                all_regions_data[current_region_name] = {}

            if "EXCEPTIONS" not in page_text: 
                continue

            f_txt.write(f"\n{'='*20} PAGE {page.page_number} ({current_region_name}) {'='*20}\n")
            
            fish_locs = get_fish_locations(page)
            geom = get_table_geometry(page)
            
            if geom[1]: 
                last_geom = geom
            elif last_geom:
                x0, _, x1, top = geom if geom[0] else last_geom
                divider = last_geom[1]
                geom = (x0, divider, x1, top)
            else:
                continue 

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
                    
                    # Convert raw Regs string into a clean List
                    reg_list = split_regulations(entry['Regs'])
                    
                    # 1. Text Output
                    sym_str = ", ".join(entry['Symbols']) if entry['Symbols'] else "None"
                    mu_str = f" (MU: {mu})" if mu else ""
                    regs_str = "\n         ".join(reg_list) # Pretty print for text file
                    
                    f_txt.write(f"WATER:   {water_name}{mu_str}\n")
                    f_txt.write(f"SYMBOLS: {sym_str}\n")
                    f_txt.write(f"REGS:    {regs_str}\n")
                    f_txt.write("-" * 50 + "\n")

                    # 2. JSON Output
                    water_key = water_name.lower()
                    
                    if water_key in all_regions_data[current_region_name]:
                        existing = all_regions_data[current_region_name][water_key]
                        
                        # Merge regs (extend list, avoid dupes if necessary)
                        for r in reg_list:
                            if r not in existing['regs']:
                                existing['regs'].append(r)
                                
                        existing['symbols'] = list(set(existing['symbols'] + entry['Symbols']))
                        
                        if not existing.get('management_unit') and mu:
                            existing['management_unit'] = mu
                    else:
                        all_regions_data[current_region_name][water_key] = {
                            "symbols": entry['Symbols'],
                            "regs": reg_list, # List of strings
                            "management_unit": mu
                        }

    # --- FINAL CLEANUP ---
    # remove regions with 0 items
    final_regions_data = {k: v for k, v in all_regions_data.items() if len(v) > 0}

    final_json = {
        "regionsData": final_regions_data,
        "regionOverviews": {} 
    }

    with open(JSON_OUTPUT, 'w', encoding='utf-8') as f_json:
        json.dump(final_json, f_json, indent=4, ensure_ascii=False)

    print(f"Extraction complete.")
    print(f"Text report: {TXT_OUTPUT}")
    print(f"JSON data:   {JSON_OUTPUT}")

if __name__ == "__main__":
    extract_fishing_data()