import pdfplumber
from PIL import Image
import numpy as np
from collections import namedtuple
import os
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

# --- CONFIGURATION ---
DBSCAN_EPS = 0.15 
DBSCAN_MIN_SAMPLES = 3 
CLUSTER_PASS_THRESHOLD = 0.90 

COLOR_DIFF_THRESHOLD = 30 
LUMINANCE_THRESHOLD = 180 
MIN_INK_PIXELS_STANDARD = 4
MIN_INK_PIXELS_SYMBOL = 1
SYMBOLS_TO_RELAX = ".,:;'-_\"`~"

MIN_FONT_SIZE = 4.0
MAX_FONT_SIZE = 30.0

Sequence = namedtuple('Sequence', ['bbox', 'y_mid', 'avg_render_idx', 'text', 'char_indices'])

# --- HELPER FUNCTIONS ---

def get_bbox_from_chars(chars):
    if not chars: return (0, 0, 0, 0)
    return (min(c['x0'] for c in chars), min(c['top'] for c in chars),
            max(c['x1'] for c in chars), max(c['bottom'] for c in chars))

def get_global_background_palette(page_image_pil):
    small = page_image_pil.resize((200, int(200 * page_image_pil.height / page_image_pil.width)))
    colors = small.convert("P", palette=Image.ADAPTIVE, colors=256).convert("RGB").getcolors(maxcolors=256*256)
    colors.sort(key=lambda x: x[0], reverse=True)
    backgrounds = []
    for count, rgb in colors:
        if sum(rgb) < 150: continue 
        backgrounds.append(np.array(rgb))
        if len(backgrounds) >= 2: break
    if len(backgrounds) == 1: backgrounds.append(np.array([255, 255, 255]))
    return backgrounds

def check_char_sanity(char, page_image_array, scale, bg_palette):
    if char.get('size', 0) < MIN_FONT_SIZE or char.get('size', 0) > MAX_FONT_SIZE:
        return False
    if char['text'].strip() == '': return True
    
    x0, y0 = int(char['x0'] * scale), int(char['top'] * scale)
    x1, y1 = int(char['x1'] * scale), int(char['bottom'] * scale)
    h, w = page_image_array.shape[:2]
    x0, y0 = max(0, x0), max(0, y0)
    x1, y1 = min(w, x1), min(h, y1)
    
    if (x1 - x0) < 2 or (y1 - y0) < 2: return True
    crop = page_image_array[y0:y1, x0:x1, :3]
    pixels = crop.reshape(-1, 3)
    bg1, bg2 = bg_palette[0], bg_palette[1]
    
    dist_bg1 = np.linalg.norm(pixels - bg1, axis=1)
    dist_bg2 = np.linalg.norm(pixels - bg2, axis=1)
    is_distinct = (dist_bg1 > COLOR_DIFF_THRESHOLD) & (dist_bg2 > COLOR_DIFF_THRESHOLD)
    
    luminance = np.dot(pixels, [0.299, 0.587, 0.114])
    is_dark = luminance < LUMINANCE_THRESHOLD
    
    ink_pixels = np.sum(is_distinct & is_dark)
    thresh = MIN_INK_PIXELS_SYMBOL if char['text'] in SYMBOLS_TO_RELAX else MIN_INK_PIXELS_STANDARD
    return ink_pixels >= thresh

def check_group_sanity(chars, page_image_array, scale, bg_palette):
    if not "".join([c['text'] for c in chars]).strip(): return False
    return all(check_char_sanity(c, page_image_array, scale, bg_palette) for c in chars)

def cluster_chars_into_lines(chars):
    chars_sorted = sorted(chars, key=lambda c: c['top'])
    lines = []
    for char in chars_sorted:
        placed = False
        for line in lines:
            line_top = min(c['top'] for c in line)
            line_bottom = max(c['bottom'] for c in line)
            overlap = max(0, min(char['bottom'], line_bottom) - max(char['top'], line_top))
            if overlap / (char['bottom'] - char['top']) > 0.4:
                line.append(char)
                placed = True
                break
        if not placed: lines.append([char])
    return lines

# --- MAIN ---

def clean_and_extract_page17():
    pdf_path = 'output/fishing_synopsis.pdf'
    resolution = 400
    scale = resolution / 72
    os.makedirs('output', exist_ok=True)
    
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[16] # Page 17
        
        # 1. Setup Data for Clustering
        base_image = page.to_image(resolution=resolution).original.convert('RGBA')
        np_image = np.array(base_image.convert('RGB'))
        bg_palette = get_global_background_palette(base_image.convert('RGB'))
        
        all_chars = page.chars
        for i, c in enumerate(all_chars): 
            c['render_index'] = i # Tag every char
            
        lines = cluster_chars_into_lines(all_chars)
        
        sequences, raw_groups = [], []
        for row_chars in lines:
            row_chars.sort(key=lambda x: x['render_index'])
            active = [row_chars[0]]
            for i in range(1, len(row_chars)):
                curr, prev = row_chars[i], active[-1]
                if -3 < (curr['x0'] - prev['x1']) < 3: active.append(curr)
                else:
                    text = "".join([c['text'] for c in active])
                    if text.strip():
                        bbox = get_bbox_from_chars(active)
                        indices = [c['render_index'] for c in active]
                        sequences.append(Sequence(bbox, (bbox[1]+bbox[3])/2, sum(indices)/len(indices), text, indices))
                        raw_groups.append(active)
                    active = [curr]
            if active:
                text = "".join([c['text'] for c in active])
                if text.strip():
                    bbox = get_bbox_from_chars(active)
                    indices = [c['render_index'] for c in active]
                    sequences.append(Sequence(bbox, (bbox[1]+bbox[3])/2, sum(indices)/len(indices), text, indices))
                    raw_groups.append(active)

        # 2. DBSCAN Clustering & Audit
        raw_data = np.array([[s.y_mid, s.avg_render_idx] for s in sequences])
        data_norm = StandardScaler().fit_transform(raw_data)
        db = DBSCAN(eps=DBSCAN_EPS, min_samples=DBSCAN_MIN_SAMPLES).fit(data_norm)
        labels = db.labels_
        
        keep_indices = set()
        for k in set(labels):
            if k == -1: continue
            indices = [i for i, x in enumerate(labels) if x == k]
            sane_count = sum(1 for idx in indices if check_group_sanity(raw_groups[idx], np_image, scale, bg_palette))
            
            if (sane_count / len(indices)) >= CLUSTER_PASS_THRESHOLD:
                for idx in indices:
                    keep_indices.update(sequences[idx].char_indices)

        # 3. USE FILTER METHOD TO CLEAN PAGE
        # This creates a new 'Page' instance containing only whitelisted chars
        def ghost_filter(obj):
            # Only apply to characters
            if obj.get("object_type") == "char":
                return obj.get("render_index") in keep_indices
            return True # Keep other objects (rects, lines)

        clean_page = page.filter(ghost_filter)

        print(f"Cleaning Complete.")
        print(f"Original Characters: {len(page.chars)}")
        print(f"Vetted Characters: {len(clean_page.chars)}")
        
        # 4. Extract Text from the Clean Page
        print("\n--- FINAL CLEAN TEXT EXTRACTION ---")
        clean_text = clean_page.extract_text(x_tolerance=3, y_tolerance=3)
        print(clean_text)

        # Save Visual
        clean_page.to_image(resolution=150).draw_rects(clean_page.chars).save("output/page17_fully_cleaned.png")
        print("\nSaved visualization: output/page17_fully_cleaned.png")

if __name__ == "__main__":
    clean_and_extract_page17()