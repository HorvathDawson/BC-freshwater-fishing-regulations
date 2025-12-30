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

INVALID_KEYWORDS = [
    "Kilometres", "courtesy of", "purchase a larger map", 
    "reprinted", "Haig-Brown", "scale", "www.", ".ca", ".com",
    "Department of Fisheries", "Management Unit", 
    "Please refer to", "Check website for", 
    "Regulation Changes", "NOTE:", "front cover", "back cover"
]

VERTICAL_GAP_THRESHOLD = 3.0 

# --- REGULATION PARSING LOGIC ---

class RegParser:
    PATTERNS = {
        "Advisory": [r"WARNING", r"Mercury", r"Thin ice", r"NOTICE", r"consumption"],
        "Fishing Closure": [r"No Fishing", r"Closed", r"No Ice Fishing"],
        "Classified Waters": [r"Class I", r"Class II", r"Steelhead Stamp"],
        "Access Restriction": [r"Youth", r"Disabled", r"Permit"],
        "Boating Restriction": [r"boat", r"motor", r"speed", r"towing", r"vessel", r"power"],
        "Gear Restriction": [r"barbless", r"hook", r"bait ban", r"fly only", r"artificial fly", r"set line", r"spear"],
        "Quota / Catch Limit": [r"quota", r"limit", r"daily", r"possession", r"catch and release", r"release", r"retain"]
    }

    DATE_PATTERN = r"([A-Z][a-z]{2,8}\s+\d{1,2}\s*[-–]\s*[A-Z][a-z]{2,8}\s+\d{1,2})"

    @staticmethod
    def classify(text):
        for category, patterns in RegParser.PATTERNS.items():
            for pat in patterns:
                if re.search(pat, text, re.IGNORECASE):
                    return category
        return "General Restriction"

    @staticmethod
    def pre_clean(text):
        text = re.sub(r'(Fishing)\s+(Bait)', r'\1; \2', text, flags=re.IGNORECASE)
        text = re.sub(r'(Fishing)\s+(Artificial)', r'\1; \2', text, flags=re.IGNORECASE)
        text = re.sub(r'(Fishing)\s+(Single)', r'\1; \2', text, flags=re.IGNORECASE)
        text = re.sub(r'(\d)\s+(bait)', r'\1; \2', text, flags=re.IGNORECASE)
        text = re.sub(r'([^\.;])\s+(WARNING!)', r'\1; \2', text)
        text = re.sub(r'([^\.;])\s+(NOTICE)', r'\1; \2', text)
        return text

    @staticmethod
    def parse_reg(text):
        text = RegParser.pre_clean(text)

        if ";" in text:
            parts = [p.strip() for p in text.split(';') if p.strip()]
            results = []
            for p in parts:
                results.extend(RegParser.parse_reg(p))
            return results

        if "," in text:
            parts = [p.strip() for p in text.split(',')]
            significant_count = 0
            for p in parts:
                if RegParser.classify(p) != "General Restriction":
                    significant_count += 1
            
            if significant_count > 1:
                split_results = []
                for p in parts:
                    sub_res = RegParser.parse_reg(p)
                    split_results.extend(sub_res) 
                return split_results

        result = {
            "type": RegParser.classify(text),
            "details": text,
            "date_range": None
            # Removed 'original_text' as requested
        }

        date_match = re.search(RegParser.DATE_PATTERN, text)
        if date_match:
            result["date_range"] = date_match.group(1)

        return [result]

# --- STANDARD HELPER FUNCTIONS ---

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

def is_fish_vector(curve):
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
    return re.sub(r'\s+', ' ', text).strip()

def extract_all_mus(text):
    mu_pattern = r'\b(\d{1,2}-\d{1,2})\b'
    mus = re.findall(mu_pattern, text)
    clean_name = re.sub(mu_pattern, ' ', text)
    clean_name = re.sub(r'\s+', ' ', clean_name).strip()
    return clean_name, mus

def is_mu_line(text):
    leftover = re.sub(r'[\d\-\,\s]', '', text)
    return len(leftover) < 2 and re.search(r'\d{1,2}-\d{1,2}', text)

def is_valid_row(name, regs):
    combined = (name + " " + " ".join(regs)).lower()
    combined = re.sub(r'\s+', ' ', combined)
    if any(k.lower() in combined for k in INVALID_KEYWORDS): return False
    if "WATER BODY" in name: return False 
    if not name.strip() and not regs: return False
    return True

def extract_text_by_spatial_layout(page, bbox):
    if not bbox: return []
    x0, top, x1, bottom = bbox
    
    try:
        cell_crop = page.crop((x0, top, x1, bottom))
    except ValueError:
        return []

    words = cell_crop.extract_words(x_tolerance=2, y_tolerance=2, keep_blank_chars=True)
    if not words: return []

    lines = []
    current_line = [words[0]]
    for word in words[1:]:
        last_word = current_line[-1]
        if abs(word['top'] - last_word['top']) < 3:
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
            
        prev_line = lines[i-1]
        gap = line[0]['top'] - prev_line[0]['bottom']
        
        if gap > VERTICAL_GAP_THRESHOLD:
            spatial_blocks.append(" ".join(current_text_block))
            current_text_block = [line_text]
        else:
            current_text_block.append(line_text)

    if current_text_block:
        spatial_blocks.append(" ".join(current_text_block))

    final_reg_list = []
    for block in spatial_blocks:
        parts = block.split(';')
        for p in parts:
            cleaned = clean_text(p)
            if cleaned:
                final_reg_list.append(cleaned)

    return final_reg_list

def parse_water_col(text, bbox, fish_locations):
    text_raw = text.replace('\n', ' ').strip()
    symbols = []
    
    if '\uf0dc' in text_raw or '*' in text_raw:
        symbols.append("Includes Tributaries")
        text_raw = text_raw.replace('\uf0dc', '').replace('*', '')

    if "CW" in text_raw and re.search(r'\bCW\b', text_raw):
        symbols.append("Classified Waters")
        text_raw = re.sub(r'\bCW\b', '', text_raw)

    if bbox:
        x0, top, x1, bottom = bbox
        for (fx, fy) in fish_locations:
            if x0 < fx < x1 and top < fy < bottom:
                symbols.append("Stocked")
                break 

    clean_str = re.sub(r'\s+', ' ', text_raw).strip()
    return clean_str, symbols

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

def process_table(page, table, fish_locations):
    structured_data = []
    current_entry = None

    text_rows = table.extract()
    row_objs = table.rows 

    if len(text_rows) != len(row_objs): return []

    for i, row in enumerate(row_objs):
        if not row.cells or len(row.cells) < 2: continue
        
        c1_box = row.cells[0]
        c1_raw = text_rows[i][0] or ""
        
        # This returns a LIST of strings (e.g. ["No fishing", "Bait ban"])
        c2_box = row.cells[1]
        c2_raw_list = extract_text_by_spatial_layout(page, c2_box)

        name_text, c1_syms = parse_water_col(c1_raw, c1_box, fish_locations)
        
        structured_regs = []
        for raw_reg in c2_raw_list:
            parsed_list = RegParser.parse_reg(raw_reg)
            structured_regs.extend(parsed_list)

        if not is_valid_row(name_text, c2_raw_list): continue

        if name_text:
            if is_mu_line(name_text):
                # Continuation of Previous MU
                if current_entry:
                    _, extra_mus = extract_all_mus(name_text)
                    current_entry["MUs"].extend(extra_mus)
                    if c1_syms:
                        current_entry["Symbols"] = list(set(current_entry["Symbols"] + c1_syms))
                    if structured_regs:
                        current_entry["regs"].extend(structured_regs)
                        # Append raw text to parent field
                        raw_text_block = "\n".join(c2_raw_list)
                        if raw_text_block:
                            current_entry["original_reg_text"] += "\n" + raw_text_block
            else:
                # New Water Body
                final_name, mus = extract_all_mus(name_text)
                if current_entry: structured_data.append(current_entry)
                
                # Combine the raw list into a single block for the parent field
                raw_text_block = "\n".join(c2_raw_list)
                
                current_entry = {
                    "Water": final_name,
                    "MUs": mus,
                    "Symbols": c1_syms,
                    "original_reg_text": raw_text_block, # NEW PARENT FIELD
                    "regs": structured_regs
                }
        elif current_entry and structured_regs:
            # Continuation of Regulations
            current_entry["regs"].extend(structured_regs)
            
            # Append raw text to parent field
            raw_text_block = "\n".join(c2_raw_list)
            if raw_text_block:
                current_entry["original_reg_text"] += "\n" + raw_text_block
                
            _, extra_syms = parse_water_col(c1_raw, c1_box, fish_locations)
            if extra_syms:
                current_entry["Symbols"] = list(set(current_entry["Symbols"] + extra_syms))

    if current_entry: structured_data.append(current_entry)
    return structured_data

def get_region_name(text):
    match = re.search(r'(REGION\s+\d+[A-Z]?\s*[-–]\s*[^\n\r]+)', text)
    if match:
        raw_region = match.group(1).strip()
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
                data = process_table(page, table, fish_locs)
                
                for entry in data:
                    water_name = entry['Water']
                    mu = entry['MUs']
                    
                    sym_str = ", ".join(entry['Symbols']) if entry['Symbols'] else "None"
                    mu_str = f" (MUs: {', '.join(mu)})" if mu else ""
                    
                    # For Text Output, we can now use the nice parent field!
                    # Indent it slightly for readability
                    raw_text_pretty = entry['original_reg_text'].replace('\n', '\n         ')
                    
                    f_txt.write(f"WATER:   {water_name}{mu_str}\n")
                    f_txt.write(f"SYMBOLS: {sym_str}\n")
                    f_txt.write(f"REGS:    {raw_text_pretty}\n")
                    f_txt.write("-" * 50 + "\n")

                    water_key = water_name.lower()
                    
                    if water_key in all_regions_data[current_region_name]:
                        existing = all_regions_data[current_region_name][water_key]
                        
                        # Merge Dicts (avoid exact duplicates by checking 'details')
                        existing_details = {r['details'] for r in existing['regs']}
                        for r in entry['regs']:
                            if r['details'] not in existing_details:
                                existing['regs'].append(r)
                                existing_details.add(r['details'])
                                
                        existing['symbols'] = list(set(existing['symbols'] + entry['Symbols']))
                        
                        # Merge Original Text (Append with newline)
                        if entry['original_reg_text']:
                            existing['original_reg_text'] += "\n" + entry['original_reg_text']

                        if entry['MUs']:
                            current_mus = existing.get('management_units', [])
                            if not current_mus: current_mus = []
                            existing['management_units'] = list(set(current_mus + entry['MUs']))
                            
                    else:
                        all_regions_data[current_region_name][water_key] = {
                            "symbols": entry['Symbols'],
                            "original_reg_text": entry['original_reg_text'], # NEW FIELD
                            "regs": entry['regs'],
                            "management_units": entry['MUs']
                        }

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