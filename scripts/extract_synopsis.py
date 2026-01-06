import pdfplumber
import re
import numpy as np
from collections import namedtuple
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from PIL import Image
import os

# --- CONFIGURATION ---
DBSCAN_EPS = 0.15 
DBSCAN_MIN_SAMPLES = 3 
CLUSTER_PASS_THRESHOLD = 0.80 

# Visual Audit Thresholds
COLOR_DIFF_THRESHOLD = 30 
LUMINANCE_THRESHOLD = 180 
MIN_INK_PIXELS_STANDARD = 4
MIN_INK_PIXELS_SYMBOL = 1
SYMBOLS_TO_RELAX = ".,:;'-_\"`~"

# Font Size Thresholds
MIN_FONT_SIZE = 4.0
MAX_FONT_SIZE = 30.0

# Spatial Layout Constants
VERTICAL_GAP_THRESHOLD = 3.0
MAX_VERTICAL_SEPARATION = 30
MAP_KEYWORDS = ["legend", "map", "scale", "courtesy"]
MAX_SINGLE_LETTER_LENGTH = 2

Sequence = namedtuple('Sequence', ['bbox', 'y_mid', 'avg_render_idx', 'text', 'char_indices'])

# --- 1. GHOST FILTERING CORE ---

def get_global_background_palette(page_image_pil):
    small = page_image_pil.resize((200, int(200 * page_image_pil.height / page_image_pil.width)))
    colors = small.convert("P", palette=Image.ADAPTIVE, colors=256).convert("RGB").getcolors(maxcolors=256*256)
    colors.sort(key=lambda x: x[0], reverse=True)
    backgrounds = []
    for count, rgb in colors:
        if sum(rgb) < 150: continue 
        backgrounds.append(np.array(rgb))
        if len(backgrounds) >= 2: break
    if len(backgrounds) < 2: backgrounds.append(np.array([255, 255, 255]))
    return backgrounds

def check_char_sanity(char, page_image_array, scale, bg_palette):
    fs = char.get('size', 0)
    if fs < MIN_FONT_SIZE or fs > MAX_FONT_SIZE: return False
    if char['text'].strip() == '': return True
    
    x0, y0, x1, y1 = [int(v * scale) for v in [char['x0'], char['top'], char['x1'], char['bottom']]]
    h, w = page_image_array.shape[:2]
    crop = page_image_array[max(0,y0):min(h,y1), max(0,x0):min(w,x1)]
    if crop.size == 0: return True
    
    pixels = crop.reshape(-1, 3)
    bg1, bg2 = bg_palette[0], bg_palette[1]
    
    is_distinct = (np.linalg.norm(pixels - bg1, axis=1) > COLOR_DIFF_THRESHOLD) & \
                  (np.linalg.norm(pixels - bg2, axis=1) > COLOR_DIFF_THRESHOLD)
    is_dark = np.dot(pixels, [0.299, 0.587, 0.114]) < LUMINANCE_THRESHOLD
    
    ink_count = np.sum(is_distinct & is_dark)
    thresh = MIN_INK_PIXELS_SYMBOL if char['text'] in SYMBOLS_TO_RELAX else MIN_INK_PIXELS_STANDARD
    return ink_count >= thresh

def check_group_sanity(chars, page_image_array, scale, bg_palette):
    if not "".join([c['text'] for c in chars]).strip(): return False
    bad_chars = sum(1 for c in chars if not check_char_sanity(c, page_image_array, scale, bg_palette))
    return (bad_chars / len(chars)) < 0.2

def get_cleaned_page(page):
    resolution = 400
    scale = resolution / 72
    img = page.to_image(resolution=resolution).original.convert('RGB')
    np_img = np.array(img)
    bg_pal = get_global_background_palette(img)
    
    all_chars = page.chars
    for i, c in enumerate(all_chars): c['render_index'] = i
    
    chars_sorted = sorted(all_chars, key=lambda c: c['top'])
    lines = []
    for c in chars_sorted:
        placed = False
        for l in lines:
            overlap = max(0, min(c['bottom'], l[-1]['bottom']) - max(c['top'], l[-1]['top']))
            if overlap / (c['bottom'] - c['top']) > 0.4:
                l.append(c); placed = True; break
        if not placed: lines.append([c])

    sequences, raw_groups = [], []
    for row in lines:
        row.sort(key=lambda x: x['render_index'])
        active = [row[0]]
        for i in range(1, len(row)):
            if -3 < (row[i]['x0'] - active[-1]['x1']) < 3: active.append(row[i])
            else:
                txt = "".join([c['text'] for c in active])
                if txt.strip():
                    idx = [c['render_index'] for c in active]
                    sequences.append(Sequence((min(c['x0'] for c in active), min(c['top'] for c in active), max(c['x1'] for c in active), max(c['bottom'] for c in active)), (active[0]['top']+active[0]['bottom'])/2, sum(idx)/len(idx), txt, idx))
                    raw_groups.append(active)
                active = [row[i]]
        if active:
            txt = "".join([c['text'] for c in active])
            if txt.strip():
                idx = [c['render_index'] for c in active]
                sequences.append(Sequence((min(c['x0'] for c in active), min(c['top'] for c in active), max(c['x1'] for c in active), max(c['bottom'] for c in active)), (active[0]['top']+active[0]['bottom'])/2, sum(idx)/len(idx), txt, idx))
                raw_groups.append(active)

    if not sequences: return page

    X = np.array([[s.y_mid, s.avg_render_idx] for s in sequences])
    labels = DBSCAN(eps=DBSCAN_EPS, min_samples=DBSCAN_MIN_SAMPLES).fit(StandardScaler().fit_transform(X)).labels_

    keep_indices = set()
    for k in set(labels):
        indices = [i for i, x in enumerate(labels) if x == k]
        if k == -1:
            for idx in indices:
                if check_group_sanity(raw_groups[idx], np_img, scale, bg_pal):
                    keep_indices.update(sequences[idx].char_indices)
        else:
            sane_count = sum(1 for idx in indices if check_group_sanity(raw_groups[idx], np_img, scale, bg_pal))
            if (sane_count / len(indices)) >= CLUSTER_PASS_THRESHOLD:
                for idx in indices: keep_indices.update(sequences[idx].char_indices)

    return page.filter(lambda o: o.get("render_index") in keep_indices if o.get("object_type") == "char" else True)

# --- 2. SPATIAL EXTRACTION ---

def clean_text(text): 
    return re.sub(r'\s+', ' ', text).strip() if text else ""

def extract_visual_lines(page, bbox):
    if not bbox: return []
    try: cell_crop = page.crop(bbox)
    except: return []
    words = cell_crop.extract_words(x_tolerance=2, y_tolerance=2)
    if not words: return []
    
    lines = []
    current_line = [words[0]]
    for word in words[1:]:
        if abs(word['top'] - current_line[-1]['top']) < 3: current_line.append(word)
        else: lines.append(current_line); current_line = [word]
    lines.append(current_line)

    if len(lines) > 1:
        line_tops = [sum(w['top'] for w in l)/len(l) for l in lines]
        for i in range(len(line_tops)-1):
            if line_tops[i+1] - line_tops[i] > MAX_VERTICAL_SEPARATION:
                lines = lines[:i+1]; break
                
    return [clean_text(" ".join([w['text'] for w in l])) for l in lines]

def extract_text_blocks(page, bbox):
    if not bbox: return []
    try: cell_crop = page.crop(bbox)
    except: return []
    words = cell_crop.extract_words(x_tolerance=2, y_tolerance=2)
    if not words: return []
    
    lines = []
    current_line = [words[0]]
    for w in words[1:]:
        if abs(w['top'] - current_line[-1]['top']) < 3: current_line.append(w)
        else: lines.append(current_line); current_line = [w]
    lines.append(current_line)

    blocks, current_block = [], []
    for i, line in enumerate(lines):
        txt = " ".join([w['text'] for w in line]).replace('\uf0dc', ' [Tributaries] ').replace('*', ' [Tributaries] ')
        if i == 0: current_block.append(txt); continue
        if (line[0]['top'] - lines[i-1][0]['bottom']) > VERTICAL_GAP_THRESHOLD:
            blocks.append(clean_text(" ".join(current_block)))
            current_block = [txt]
        else: current_block.append(txt)
    if current_block: blocks.append(clean_text(" ".join(current_block)))
    return blocks

def get_table_geometry(page):
    tables = page.find_tables(table_settings={"vertical_strategy": "lines", "horizontal_strategy": "lines"})
    if not tables: return None, None, None, None
    main = max(tables, key=lambda t: (t.bbox[2]-t.bbox[0]) * (t.bbox[3]-t.bbox[1]))
    x0, top, x1, _ = main.bbox
    divider = None
    for c in sorted(main.cells, key=lambda x: (x[1], x[0])):
        if abs(c[0] - x0) < 2 and abs(c[2] - x1) > 5: divider = c[2]; break
    return x0, divider, x1, top

def get_fish_locations(page):
    centers = []
    for curve in page.curves:
        w, h = curve['x1']-curve['x0'], curve['bottom']-curve['top']
        if (4 < w < 40) and (4 < h < 40) and (0.2 < w/h < 5.0):
            centers.append(((curve['x0']+curve['x1'])/2, (curve['top']+curve['bottom'])/2))
    return centers

# --- 3. RE-ADDED FOR TEST COMPATIBILITY ---

def get_table_text_sizes(page):
    """Re-added to satisfy test_extract_synopsis.py imports."""
    clean_page = get_cleaned_page(page)
    geo = get_table_geometry(clean_page)
    if not geo or geo[0] is None: return []
    x0, div, x1, top = geo
    try:
        # Check chars within the table area
        table_crop = clean_page.crop((x0, top, x1, clean_page.bbox[3]))
        sizes = sorted(list(set(round(c['size'], 1) for c in table_crop.chars if 'size' in c)))
        return sizes
    except: return []

# --- 4. MAIN WRAPPER ---

def extract_rows_from_page(page):
    # PRE-PROCESS: Filter ghost text
    clean_page = get_cleaned_page(page)
    
    geo = get_table_geometry(clean_page)
    if not geo or geo[0] is None: return []
    x0, divider, x1, top = geo
    
    # We need the bottom boundary too
    tables = clean_page.find_tables(table_settings={"vertical_strategy": "lines", "horizontal_strategy": "lines"})
    main_table = max(tables, key=lambda t: (t.bbox[2]-t.bbox[0]) * (t.bbox[3]-t.bbox[1]))
    bottom = main_table.bbox[3]
    
    fish_locs = get_fish_locations(clean_page)
    
    v_lines = [x0, x1]
    if divider: v_lines.append(divider)
    
    table_settings = {"vertical_strategy": "explicit", "explicit_vertical_lines": v_lines, "horizontal_strategy": "lines", "intersection_y_tolerance": 10}
    
    # Create the crop and find internal tables
    try:
        table_page = clean_page.crop((x0, top, x1, bottom))
        tables = table_page.find_tables(table_settings)
    except: return []
    
    if not tables: return []
    
    results, current_entry, last_c1 = [], None, None
    for table in tables:
        for row in table.rows:
            if len(row.cells) < 2 or not row.cells[1]: continue
            c1, c2 = row.cells[0], row.cells[1]
            is_merged = (c1 is None) or (c1 == last_c1)
            if c1: last_c1 = c1
            
            if not is_merged:
                water_lines = extract_visual_lines(clean_page, c1)
                water = " ".join(water_lines)
                symbols = []
                if any(x in water.lower() for x in ['\uf0dc', '*', 'tributaries']): symbols.append("Includes Tributaries")
                if "CW" in water: symbols.append("Classified Waters")
                for (fx, fy) in fish_locs:
                    if c1[0] < fx < c1[2] and c1[1] < fy < c1[3]: symbols.append("Stocked"); break
                
                water = re.sub(r'\uf0dc|\*|CW|\[?Includes tributaries\]?', '', water, flags=re.I).strip()
                water = clean_text(water)
            else:
                water = current_entry['water'] if current_entry else ""
                symbols = current_entry['symbols'] if current_entry else []

            regs = "\n".join(extract_text_blocks(clean_page, c2))
            if not water and not regs: continue
            if any(k in water.lower() for k in MAP_KEYWORDS) or (len(water) < 2 and len(regs) < 2): continue

            if is_merged and current_entry:
                current_entry['regs'] += "\n" + regs
            else:
                if current_entry: results.append(current_entry)
                current_entry = {"water": water, "regs": regs, "symbols": symbols}
                
    if current_entry: results.append(current_entry)
    return results

if __name__ == "__main__":
    PDF_PATH = os.path.join("output", "fishing_synopsis.pdf")
    if os.path.exists(PDF_PATH):
        with pdfplumber.open(PDF_PATH) as pdf:
            data = extract_rows_from_page(pdf.pages[16])
            for entry in data:
                print(f"WATER: {entry['water']}\nSYMBOLS: {entry['symbols']}\nREGS: {entry['regs']}\n{'-'*30}")