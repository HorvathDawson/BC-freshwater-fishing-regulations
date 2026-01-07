import pdfplumber
import os
import argparse
import re
import textwrap
import shutil
import numpy as np
from operator import itemgetter
from collections import namedtuple
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from PIL import Image, ImageDraw
import matplotlib.pyplot as plt
import matplotlib.patches as patches

Sequence = namedtuple('Sequence', ['bbox', 'y_mid', 'avg_render_idx', 'text', 'char_indices'])

class FishingSynopsisParser:
    def __init__(self, output_dir="output", debug_dir="debug", audit_dir="debug"):
        self.output_dir = output_dir
        self.debug_dir = os.path.join(output_dir, debug_dir)
        self.audit_dir = os.path.join(output_dir, audit_dir)
        
        # DBSCAN & Cleaning Constants
        self.DBSCAN_EPS = 0.15
        self.DBSCAN_MIN_SAMPLES = 3
        self.CLUSTER_PASS_THRESHOLD = 0.80
        self.COLOR_DIFF_THRESHOLD = 30
        self.LUMINANCE_THRESHOLD = 180
        self.MIN_INK_PIXELS_STANDARD = 4
        self.SYMBOLS_TO_RELAX = ".,:;'-_\"`~"
        self.MIN_FONT_SIZE = 4.0
        self.MAX_FONT_SIZE = 30.0
        
        # Symbol Rejection Thresholds
        self.MIN_SYMBOL_DIM = 4.0
        self.MAX_SYMBOL_DIM = 25.0 
        self.MAP_REJECTION_DIM = 35.0 

    # --- REG PARSER (NESTED CLASS) ---
    class RegParser:
        # START_KEYWORDS triggers a new regulation line.
        START_KEYWORDS = [
            "No Fishing", "Closed", "No Ice Fishing", "Bait ban", "Fly fishing", 
            "Artificial fly", "Barbless", "Quota", "Daily", "Catch and release", 
            "Release", "Class", "Steelhead", "Trout", "Char", "Kokanee", "Chinook", 
            "Coho", "Rainbow", "Cutthroat", "Salmon", "Bass", "Walleye", "Pike", 
            "Perch", "Burbot", "Crayfish", "Single", "No", "The standard"
        ]

        PATTERNS = {
            "Advisory": [r"WARNING", r"Mercury", r"Thin ice", r"NOTICE", r"consumption"],
            "Fishing Closure": [r"No Fishing", r"Closed", r"No Ice Fishing", "The standard"],
            "Classified Waters": [r"Class I", r"Class II", r"Steelhead Stamp"],
            "Access Restriction": [r"Youth", r"Disabled", r"Permit"],
            "Boating Restriction": [r"boat", r"motor", r"speed", r"towing", r"vessel", r"power"],
            "Gear Restriction": [r"barbless", r"hook", r"bait ban", r"fly only", r"artificial fly", r"set line", r"spear"],
            "Quota / Catch Limit": [r"quota", r"limit", r"daily", r"possession", r"catch and release", r"release", r"retain", r"trout", r"char", r"salmon", r"bass", r"walleye", r"pike"]
        }
        DATE_PATTERN = r"([A-Z][a-z]{2,8}\s+\d{1,2}\s*[-–]\s*[A-Z][a-z]{2,8}\s+\d{1,2})"

        @staticmethod
        def classify(text):
            for category, patterns in FishingSynopsisParser.RegParser.PATTERNS.items():
                for pat in patterns:
                    if re.search(pat, text, re.IGNORECASE): return category
            return "General Restriction"
        @staticmethod
        def pre_clean(text):
            text = text.replace('\n', ' ')
            text = re.sub(r'\s+', ' ', text).strip()
            
            # --- DEFINE COMMON PATTERNS ---
            adjectives = r'Bull|Lake|Brook|Brown|Golden|Dolly|Rainbow|Cutthroat|Summer|Winter|Wild|Hatchery|Northern|No|Smallmouth|Largemouth'
            targets = r'Trout|Char|Varden|Steelhead|Salmon|Pike|Bass|Rainbow|Cutthroat|Kokanee'

            # 1. Insert semicolons before ALL start keywords
            keyword_pattern = r'(?<!;)\s+\b(' + '|'.join(FishingSynopsisParser.RegParser.START_KEYWORDS) + r')\b'
            text = re.sub(keyword_pattern, r'; \1', text, flags=re.IGNORECASE)
            
            # 1.5 FIX: Remove semicolons that were inserted right after a period (sentence boundary)
            text = re.sub(r'\.\s*;\s*', '. ', text)
            
            # 2. Fix: Remove semicolons inside parentheses/includes
            text = re.sub(r'(\bincluding|\bincludes|\bexcept|\(|\[)\s*;\s*', r'\1 ', text, flags=re.IGNORECASE)

            # 3. Fix: Remove semicolons after Forward Slashes
            text = re.sub(r'/\s*;\s*', '/', text)
            
            # 3.1 FIX: Remove semicolons after Colons (Fixes boundary descriptions followed by quotas)
            # e.g., "...Waneta Dam): ; Northern pike..." -> "...Waneta Dam): Northern pike..."
            text = re.sub(r'(:)\s*;\s*', r'\1 ', text)

            # 3.2 FIX: Protect "EXEMPT" clauses from internal splitting
            # The parser tries to split on "trout" and "char" inside "EXEMPT from ... trout/char catch and release"
            # We run this twice to handle multiple keywords appearing in the exemption string
            text = re.sub(r'(EXEMPT\s+from\s+[^;]+?)\s*;\s*', r'\1 ', text, flags=re.IGNORECASE)
            text = re.sub(r'(EXEMPT\s+from\s+[^;]+?)\s*;\s*', r'\1 ', text, flags=re.IGNORECASE)

            # 3.5 FIX: Specific Healer for "Wild/Hatchery" + Species
            text = re.sub(r'\b(Wild|Hatchery)\s*;\s*(Rainbow|Cutthroat|Steelhead|Trout|Char)', r'\1 \2', text, flags=re.IGNORECASE)

            # 3.6 FIX: Split distinct quotas that start with a number (e.g. "...50 cm, 1 bull trout...")
            quota_split_pattern = rf',\s+(\d+\s+(?:{adjectives}|{targets}))'
            text = re.sub(quota_split_pattern, r'; \1', text, flags=re.IGNORECASE)

            # 4. Fix: Compound Species Names (Bull Trout, Smallmouth Bass, etc)
            compound_pattern = rf'\b({adjectives})\s*;\s*({targets})'
            text = re.sub(compound_pattern, r'\1 \2', text, flags=re.IGNORECASE)
            text = re.sub(compound_pattern, r'\1 \2', text, flags=re.IGNORECASE)

            # 5. FIX: "Single Barbless"
            text = re.sub(r'(\bsingle)\s*;\s*barbless', r'\1 barbless', text, flags=re.IGNORECASE)

            # 6. FIX: Fish Daily Quotas
            fish_names = r'(Walleye|Pike|Perch|Bass|Trout|Char|Salmon|Steelhead|Kokanee|Burbot|Crayfish)'
            quota_merge_pattern = rf'\b{fish_names}\s*;\s*(Daily|Quota|Limit)'
            text = re.sub(quota_merge_pattern, r'\1 \2', text, flags=re.IGNORECASE)
            
            # 6.5 FIX: Daily/Limit + Quota (e.g., "daily; quota")
            text = re.sub(r'\b(daily|limit)\s*;\s*(quota|limit)', r'\1 \2', text, flags=re.IGNORECASE)
            
            # 6.6 FIX: Species + "catch and release" (multi-word action phrase)
            fish_species = r'(Walleye|Pike|Perch|Bass|Trout|Char|Salmon|Steelhead|Kokanee|Burbot|Crayfish)'
            text = re.sub(rf'\b{fish_species}\s*;\s*catch\s+and\s+release', r'\1 catch and release', text, flags=re.IGNORECASE)

            # 7. FIX: Coquihalla Healer (Mandatory Comma)
            text = re.sub(r'(catch\s+and\s+release),\s*;\s*(bait\s+ban)', r'\1, \2', text, flags=re.IGNORECASE)

            # 8. Generic Species + Quota Action (Backup)
            species_merge_pattern = r'\b(Trout|Char|Steelhead|Salmon|Kokanee|Chinook|Coho|Rainbow|Cutthroat|Bass|Walleye|Pike|Burbot)(.*?);\s*(catch|limit|quota|daily|release)'
            def merge_match(match):
                species = match.group(1)
                middle = match.group(2)
                action = match.group(3)
                if len(middle) < 40: return f"{species}{middle} {action}"
                return match.group(0)
            # Apply repeatedly until no more merges happen
            for _ in range(5):  # Max 5 iterations to prevent infinite loops
                new_text = re.sub(species_merge_pattern, merge_match, text, flags=re.IGNORECASE)
                if new_text == text:
                    break
                text = new_text

            # 9. FIX: "Exempt From Single"
            text = re.sub(r'(\bfrom)\s*;\s*(single)', r'\1 \2', text, flags=re.IGNORECASE)

            # 10. FIX: "Trout and Kokanee"
            text = re.sub(r'(\band)\s*;\s*', r'\1 ', text, flags=re.IGNORECASE)

            # 11. FIX: "Is Closed" (Buttle Lake)
            text = re.sub(r'(\bis|\bare|\bremain)\s*;\s*(closed)', r'\1 \2', text, flags=re.IGNORECASE)

            # 12. FIX: "A No Fishing Area" (Little Qualicum)
            text = re.sub(r'(\ba)\s*;\s*(no\s+fishing)', r'\1 \2', text, flags=re.IGNORECASE)

            return text
        @staticmethod
        def clean_and_split(text):
            initial_chunks = [c.strip() for c in text.split(';') if c.strip()]
            final_items = []
            for chunk in initial_chunks:
                # Build regex pattern to split on period + space + (uppercase OR start keyword)
                # This catches both "...tunnel. No Fishing..." and "...only. bait ban..."
                # Lookbehind changed to accept any character (not just [a-z0-9]) to handle cases like "...24). Fly fishing"
                keyword_pattern = '|'.join(re.escape(kw) for kw in FishingSynopsisParser.RegParser.START_KEYWORDS)
                split_pattern = rf'\.\s+(?=[A-Z]|(?:{keyword_pattern}))'
                sentences = re.split(split_pattern, chunk, flags=re.IGNORECASE)
                for sentence in sentences:
                    clean_sentence = sentence.strip(' ,;.') 
                    if clean_sentence:
                        final_items.append(clean_sentence)
            return final_items

        @staticmethod
        def parse_reg(text):
            text = FishingSynopsisParser.RegParser.pre_clean(text)
            chunks = FishingSynopsisParser.RegParser.clean_and_split(text)
            results = []
            for chunk in chunks:
                if not chunk: continue
                res = { 
                    "type": FishingSynopsisParser.RegParser.classify(chunk), 
                    "details": chunk, 
                    "date_ranges": [] 
                }
                
                date_matches = re.findall(FishingSynopsisParser.RegParser.DATE_PATTERN, chunk)
                if date_matches:
                    res["date_ranges"] = date_matches
                    
                results.append(res)
            return results

    # --- 1. PAGE CLEANING ENGINE ---

    def _get_bg_palette(self, img_pil):
        small = img_pil.resize((200, int(200 * img_pil.height / img_pil.width)))
        colors = small.convert("P", palette=Image.ADAPTIVE, colors=256).convert("RGB").getcolors(maxcolors=256*256)
        colors.sort(key=lambda x: x[0], reverse=True)
        backgrounds = []
        for count, rgb in colors:
            if sum(rgb) < 150: continue 
            backgrounds.append(np.array(rgb))
            if len(backgrounds) >= 2: break
        if len(backgrounds) < 2: backgrounds.append(np.array([255, 255, 255]))
        return backgrounds

    def _check_char_sanity(self, char, np_img, scale, bg_pal):
        fs = char.get('size', 0)
        if fs < self.MIN_FONT_SIZE or fs > self.MAX_FONT_SIZE: return False
        if char['text'].strip() == '': return True
        x0, y0, x1, y1 = [int(v * scale) for v in [char['x0'], char['top'], char['x1'], char['bottom']]]
        crop = np_img[max(0,y0):min(np_img.shape[0],y1), max(0,x0):min(np_img.shape[1],x1)]
        if crop.size == 0: return True
        pixels = crop.reshape(-1, 3)
        bg1, bg2 = bg_pal[0], bg_pal[1]
        is_distinct = (np.linalg.norm(pixels - bg1, axis=1) > self.COLOR_DIFF_THRESHOLD) & \
                      (np.linalg.norm(pixels - bg2, axis=1) > self.COLOR_DIFF_THRESHOLD)
        is_dark = np.dot(pixels, [0.299, 0.587, 0.114]) < self.LUMINANCE_THRESHOLD
        ink_count = np.sum(is_distinct & is_dark)
        thresh = 1 if char['text'] in self.SYMBOLS_TO_RELAX else self.MIN_INK_PIXELS_STANDARD
        return ink_count >= thresh

    def _check_group_sanity(self, chars, np_img, scale, bg_pal):
        if not "".join([c['text'] for c in chars]).strip(): return False
        bad_chars = sum(1 for c in chars if not self._check_char_sanity(c, np_img, scale, bg_pal))
        return (bad_chars / len(chars)) < 0.2

    def get_cleaned_page(self, page, save_debug=False, page_num=None):
        resolution = 400
        scale = resolution / 72
        img = page.to_image(resolution=resolution).original.convert('RGB')
        np_img = np.array(img)
        bg_pal = self._get_bg_palette(img)
        
        all_chars = page.chars
        for i, c in enumerate(all_chars): c['render_index'] = i
        
        chars_sorted = sorted(all_chars, key=lambda c: c['top'])
        lines = []
        for c in chars_sorted:
            placed = False
            for l in lines:
                overlap = max(0, min(c['bottom'], l[-1]['bottom']) - max(c['top'], l[-1]['top']))
                if (c['bottom'] - c['top']) > 0 and overlap / (c['bottom'] - c['top']) > 0.4:
                    l.append(c); placed = True; break
            if not placed: lines.append([c])

        sequences, raw_groups = [], []
        for row in lines:
            row.sort(key=lambda x: x['render_index'])
            if not row: continue
            active = [row[0]]
            for i in range(1, len(row)):
                if -3 < (row[i]['x0'] - active[-1]['x1']) < 3: active.append(row[i])
                else:
                    txt = "".join([c['text'] for c in active])
                    if txt.strip():
                        idx = [c['render_index'] for c in active]
                        bbox = (min(c['x0'] for c in active), min(c['top'] for c in active), max(c['x1'] for c in active), max(c['bottom'] for c in active))
                        sequences.append(Sequence(bbox, (active[0]['top']+active[0]['bottom'])/2, sum(idx)/len(idx), txt, idx))
                        raw_groups.append(active)
                    active = [row[i]]
            if active:
                txt = "".join([c['text'] for c in active])
                if txt.strip():
                    idx = [c['render_index'] for c in active]
                    bbox = (min(c['x0'] for c in active), min(c['top'] for c in active), max(c['x1'] for c in active), max(c['bottom'] for c in active))
                    sequences.append(Sequence(bbox, (active[0]['top']+active[0]['bottom'])/2, sum(idx)/len(idx), txt, idx))
                    raw_groups.append(active)

        if not sequences: return page
        X = np.array([[s.y_mid, s.avg_render_idx] for s in sequences])
        labels = DBSCAN(eps=self.DBSCAN_EPS, min_samples=self.DBSCAN_MIN_SAMPLES).fit(StandardScaler().fit_transform(X)).labels_
        
        # Calculate passthrough rates for each cluster
        cluster_stats = {}
        keep_indices = set()
        for k in set(labels):
            indices = [i for i, val in enumerate(labels) if val == k]
            if k == -1:
                kept = 0
                for idx in indices:
                    if self._check_group_sanity(raw_groups[idx], np_img, scale, bg_pal):
                        keep_indices.update(sequences[idx].char_indices)
                        kept += 1
                cluster_stats[k] = {'total': len(indices), 'kept': kept, 'rate': kept / len(indices) if len(indices) > 0 else 0}
            else:
                sane_count = sum(1 for idx in indices if self._check_group_sanity(raw_groups[idx], np_img, scale, bg_pal))
                passthrough = (sane_count / len(indices)) >= self.CLUSTER_PASS_THRESHOLD
                if passthrough:
                    for idx in indices: keep_indices.update(sequences[idx].char_indices)
                cluster_stats[k] = {'total': len(indices), 'kept': len(indices) if passthrough else 0, 'rate': 1.0 if passthrough else 0.0}
        
        # Save debug visualizations if requested
        if save_debug and page_num is not None:
            self._save_cluster_debug(page, page_num, sequences, labels, cluster_stats, keep_indices, np_img, scale)

        # ============================================================================
        # WARNING: THIS CODE IS BROKEN AND SHOULD NOT BE SHIPPED
        # ============================================================================
        # The overlap detection below incorrectly removes valid characters (e.g., 
        # the 'c' in "Omineca"). Using mean render_index as a threshold is a naive
        # approach that doesn't distinguish between watermarks and actual content.
        # 
        # FIXME: Implement proper watermark detection using:
        #   - Text pattern matching (detect repeated region names)
        #   - Spatial clustering (group characters by position/alignment)
        #   - Font metadata (watermarks often use different fonts)
        # 
        # Leaving this as a TODO for later is BAD PRACTICE. Fix it properly NOW.
        # ============================================================================
        
        # Debug: Print first line characters sorted by render_index
        if save_debug and lines and lines[0]:
            first_line_chars = sorted(lines[0], key=lambda c: c.get('render_index', 0))
            first_line_text = ''.join([c.get('text', '') for c in first_line_chars])
            render_indices = [c.get('render_index', -1) for c in first_line_chars]
            # Print characters with their render indices aligned
            char_display = ' '.join([f"{c.get('text', ' '):>4}" for c in first_line_chars])
            idx_display = ' '.join([f"{c.get('render_index', -1):>4}" for c in first_line_chars])
            print(f"  [Debug] First line chars (n={len(first_line_chars)}):")
            print(f"    Chars:   {char_display}")
            print(f"    Indices: {idx_display}")

        # Remove overlapping characters from first line only
        overlap_exists = False
        
        if lines and lines[0]:
            # Get only the characters from first line that are in keep_indices
            line_chars = [c for c in lines[0] if c['render_index'] in keep_indices]
            
            # Check each character against all others on the same line for any overlap
            for i, c1 in enumerate(line_chars):
                for j in range(i + 1, len(line_chars)):
                    c2 = line_chars[j]
                    # Shrink bboxes by 10% to avoid accidental overlap from font rendering
                    shrink = 0.1
                    c1_w, c1_h = c1['x1'] - c1['x0'], c1['bottom'] - c1['top']
                    c2_w, c2_h = c2['x1'] - c2['x0'], c2['bottom'] - c2['top']
                    
                    c1_x0 = c1['x0'] + c1_w * shrink
                    c1_x1 = c1['x1'] - c1_w * shrink
                    c1_top = c1['top'] + c1_h * shrink
                    c1_bottom = c1['bottom'] - c1_h * shrink
                    
                    c2_x0 = c2['x0'] + c2_w * shrink
                    c2_x1 = c2['x1'] - c2_w * shrink
                    c2_top = c2['top'] + c2_h * shrink
                    c2_bottom = c2['bottom'] - c2_h * shrink
                    
                    x_overlap = max(0, min(c1_x1, c2_x1) - max(c1_x0, c2_x0))
                    y_overlap = max(0, min(c1_bottom, c2_bottom) - max(c1_top, c2_top))
                    
                    if x_overlap * y_overlap > 0:
                        overlap_exists = True
                        break
        
        chars_to_remove = set()
        if overlap_exists:
            mean_render_idx = np.mean([c['render_index'] for c in lines[0] if c['render_index'] in keep_indices])
            chars_to_remove = set([c['render_index'] for c in lines[0] if c['render_index'] < mean_render_idx and c['render_index'] in keep_indices])
        # Update keep_indices to exclude duplicates
        keep_indices -= chars_to_remove
        
        # Debug: Print what remains on first line
        if save_debug and lines and lines[0]:
            remaining = [c for c in sorted(lines[0], key=lambda c: c.get('render_index', 0)) if c['render_index'] in keep_indices]
            remaining_text = ''.join([c.get('text', '') for c in remaining])
            print(f"  [Debug] After overlap removal (n={len(remaining)}): {remaining_text}")
        
        # ===========================================================================
        # END OF BROKEN CODE
        # ==========================================================================

        return page.filter(lambda o: o.get("render_index") in keep_indices if o.get("object_type") == "char" else True)

    # --- 2. SYMBOL & DEBUG LOGIC ---

    def _save_cluster_debug(self, page, page_num, sequences, labels, cluster_stats, keep_indices, np_img, scale):
        """Save cluster scatter plot and character bbox images organized by cluster."""
        page_debug_dir = os.path.join(self.debug_dir, f"page_{page_num:03d}")
        os.makedirs(page_debug_dir, exist_ok=True)
        
        # 0. Create bbox visualization of all groups BEFORE clustering (colored by render_index)
        fig, ax = plt.subplots(figsize=(14, 10))
        ax.imshow(np_img)
        
        # Get render indices for colormap
        render_indices = np.array([s.avg_render_idx for s in sequences])
        if len(render_indices) > 0:
            vmin, vmax = render_indices.min(), render_indices.max()
        else:
            vmin, vmax = 0, 1
        
        # Create colormap
        cmap = plt.cm.viridis
        norm = plt.Normalize(vmin=vmin, vmax=vmax)
        
        # Draw bounding boxes for each sequence, colored by render index
        for seq in sequences:
            color = cmap(norm(seq.avg_render_idx))
            x0, y0 = seq.bbox[0] * scale, seq.bbox[1] * scale
            x1, y1 = seq.bbox[2] * scale, seq.bbox[3] * scale
            
            rect = patches.Rectangle((x0, y0), x1 - x0, y1 - y0,
                                    linewidth=1.5, edgecolor='black', 
                                    facecolor=color, alpha=0.5)
            ax.add_patch(rect)
        
        # Add colorbar
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        cbar = plt.colorbar(sm, ax=ax, fraction=0.046, pad=0.04)
        cbar.set_label('Average Render Index', fontsize=12)
        
        ax.set_title(f'Page {page_num} - All Groups Before Clustering (colored by render index)', fontsize=12)
        ax.axis('off')
        
        pre_cluster_path = os.path.join(page_debug_dir, 'pre_clustering_render_order.png')
        plt.savefig(pre_cluster_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  [Debug] Saved pre-clustering visualization: {pre_cluster_path}")
        
        # 1. Create scatter plot of clusters
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Extract data for plotting
        X = np.array([[s.y_mid, s.avg_render_idx] for s in sequences])
        unique_labels = set(labels)
        colors = plt.cm.Spectral(np.linspace(0, 1, len(unique_labels)))
        
        for k, col in zip(unique_labels, colors):
            if k == -1:
                col = 'black'  # Noise points
            
            class_member_mask = (labels == k)
            xy = X[class_member_mask]
            
            stats = cluster_stats.get(k, {})
            rate = stats.get('rate', 0)
            total = stats.get('total', 0)
            kept = stats.get('kept', 0)
            
            label = f"Cluster {k}: {kept}/{total} ({rate*100:.1f}%)"
            ax.scatter(xy[:, 0], xy[:, 1], c=[col], s=100, alpha=0.6, edgecolors='black', label=label)
        
        ax.set_xlabel('Y Position (mid)', fontsize=12)
        ax.set_ylabel('Average Render Index', fontsize=12)
        ax.set_title(f'Page {page_num} - DBSCAN Clusters (eps={self.DBSCAN_EPS}, min_samples={self.DBSCAN_MIN_SAMPLES})', fontsize=14)
        ax.legend(loc='best', fontsize=9)
        ax.grid(True, alpha=0.3)
        
        scatter_path = os.path.join(page_debug_dir, 'cluster_scatter.png')
        plt.savefig(scatter_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  [Debug] Saved cluster scatter: {scatter_path}")
        
        # 2. Save character bbox images for each cluster
        for k in unique_labels:
            cluster_dir = os.path.join(page_debug_dir, f"cluster_{k if k != -1 else 'noise'}")
            os.makedirs(cluster_dir, exist_ok=True)
            
            # Get sequences in this cluster
            cluster_mask = (labels == k)
            cluster_sequences = [seq for i, seq in enumerate(sequences) if cluster_mask[i]]
            
            if not cluster_sequences:
                continue
            
            # Create visualization showing all characters in this cluster
            fig, ax = plt.subplots(figsize=(14, 10))
            ax.imshow(np_img)
            
            stats = cluster_stats.get(k, {})
            kept = stats.get('kept', 0)
            total = stats.get('total', 0)
            rate = stats.get('rate', 0)
            
            # Draw bounding boxes for each character in this cluster
            for seq in cluster_sequences:
                for char_idx in seq.char_indices:
                    # Find the character by render_index
                    char = next((c for c in page.chars if c.get('render_index') == char_idx), None)
                    if char:
                        x0, y0, x1, y1 = char['x0'] * scale, char['top'] * scale, char['x1'] * scale, char['bottom'] * scale
                        
                        # Color: green if kept, red if filtered out
                        color = 'green' if char_idx in keep_indices else 'red'
                        rect = patches.Rectangle((x0, y0), x1 - x0, y1 - y0, 
                                                linewidth=1, edgecolor=color, facecolor='none', alpha=0.7)
                        ax.add_patch(rect)
            
            status = "KEPT" if kept == total else f"FILTERED ({kept}/{total})"
            ax.set_title(f"Cluster {k if k != -1 else 'Noise'} - {status} - Passthrough: {rate*100:.1f}%", fontsize=12)
            ax.axis('off')
            
            cluster_img_path = os.path.join(cluster_dir, 'character_bboxes.png')
            plt.savefig(cluster_img_path, dpi=150, bbox_inches='tight')
            plt.close()
        
        print(f"  [Debug] Saved cluster visualizations to: {page_debug_dir}")

    def detect_visual_symbols(self, section_context):
        """Identifies icons via vector curve geometry and rejects large map artifacts."""
        symbols = []
        for curve in section_context.curves:
            width = curve['x1'] - curve['x0']
            height = curve['bottom'] - curve['top']
            
            # If we find a massive vector object, trigger a rejection flag
            if width > self.MAP_REJECTION_DIM or height > self.MAP_REJECTION_DIM:
                return ["REJECT_REGION"]
            
            # Normal 'Stocked' symbol detection
            if (self.MIN_SYMBOL_DIM < width < self.MAX_SYMBOL_DIM) and \
               (self.MIN_SYMBOL_DIM < height < self.MAX_SYMBOL_DIM):
                if "Stocked" not in symbols:
                    symbols.append("Stocked")
        return symbols

    def _generate_audit_image(self, clean_page, page_num):
        page_debug_dir = os.path.join(self.audit_dir, f"page_{page_num:03d}")
        os.makedirs(page_debug_dir, exist_ok=True)
        res = 150
        scale = res / 72
        im = clean_page.to_image(resolution=res).original.convert("RGBA")
        overlay = Image.new("RGBA", im.size, (255, 255, 255, 0))
        draw = ImageDraw.Draw(overlay)
        for char in clean_page.chars:
            draw.rectangle([char['x0']*scale, char['top']*scale, char['x1']*scale, char['bottom']*scale], outline=(255, 0, 0, 80))
        combined = Image.alpha_composite(im, overlay)
        save_path = os.path.join(page_debug_dir, "character_audit.png")
        combined.save(save_path)
        print(f"  [Debug] Saved character audit: {save_path}")

    def _save_row_crops(self, page, page_num, sections, div_x, x0, x1):
        page_debug_dir = os.path.join(self.debug_dir, f"page_{page_num:03d}")
        row_crops_dir = os.path.join(page_debug_dir, "row_crops")
        os.makedirs(row_crops_dir, exist_ok=True)
        page_img = page.to_image(resolution=150)
        img_w, img_h = page_img.original.size
        scale = img_w / float(page.width)
        for i, sec in enumerate(sections):
            l, t = int(max(0, (x0 - 5) * scale)), int(max(0, sec['y0'] * scale))
            r, b = int(min(img_w, (x1 + 5) * scale)) , int(min(img_h, sec['y1'] * scale))
            if r <= l or b <= t: continue
            crop = page_img.original.crop((l, t, r, b)).copy()
            draw = ImageDraw.Draw(crop)
            rel_div = int((div_x * scale) - l)
            draw.line([(rel_div, 0), (rel_div, crop.height)], fill="red", width=2)
            crop.save(os.path.join(row_crops_dir, f"row_{i:03d}.png"))
        print(f"  [Debug] Saved {len(sections)} row crops to: {row_crops_dir}")

    # --- 3. EXTRACTION ENGINE ---

    def _extract_region_header(self, page):
        """
        Scans the top 15% of the page for large text resembling 'REGION X - Name'.
        Returns the found string or None.
        """
        w, h = page.width, page.height
        
        # Look only at the top 15% of the page
        header_area = page.within_bbox((0, 0, w, h * 0.15))
        
        # Extract words with size info
        words = header_area.extract_words(keep_blank_chars=True)
        
        # Filter for large text (headers are usually > 12pt, typically 14-20pt)
        header_text = []
        for word in words:
            # Check font size (heuristic: headers are usually larger than body text ~9pt)
            if word['bottom'] - word['top'] > 10: 
                header_text.append(word['text'])
        
        full_text = " ".join(header_text)
        
        # If no large text found, try with a lower threshold (8pt)
        if not full_text.strip():
            header_text = []
            for word in words:
                if word['bottom'] - word['top'] > 8:
                    header_text.append(word['text'])
            full_text = " ".join(header_text)
        
        # Regex to find "REGION 4 - Kootenay" or "REGION 7A - Omineca" or "REGION 3 - Thompson-Nicola"
        # Handles region names with hyphens (Thompson-Nicola) and spaces (Lower Mainland)
        # Pattern: REGION + number + optional letter + dash + region name (words with hyphens/spaces)
        match = re.search(r"(REGION\s+\d+[A-Z]?\s*[-–]\s+[A-Za-z]+(?:[-\s][A-Za-z]+)*)", full_text, re.IGNORECASE)
        
        if match:
            region = match.group(1).strip()
            # Remove common suffix words that aren't part of the region name
            region = re.sub(r'\s+(Water-Specific|Regional|Water|Regulations|Specific|EXCEPTIONS|BODY).*$', '', region, flags=re.IGNORECASE)
            return region.strip()
        
        # Fallback: Look for just "REGION X -" pattern and be more flexible
        match = re.search(r"(REGION\s+\d+[A-Z]?\s*[-–]\s+[\w-]+(?:\s+[\w-]+)?)", full_text, re.IGNORECASE)
        if match:
            region = match.group(1).strip()
            region = re.sub(r'\s+(Water-Specific|Regional|Water|Regulations|Specific|EXCEPTIONS|BODY).*$', '', region, flags=re.IGNORECASE)
            return region.strip()
            
        return None

    def extract_rows(self, raw_page, save_debug=False):
        page_num = raw_page.page_number
        print(f"\n--- Processing Page {page_num} ---")
        
        # 1. Clean the page (removes background noise)
        page = self.get_cleaned_page(raw_page, save_debug=save_debug, page_num=page_num)
        
        # 2. Extract Metadata (Region Name)
        region_header = self._extract_region_header(page)
        
        metadata = {
            "page_number": page_num,
            "region": region_header
        }
        
        # Initialize the return object
        result = {
            "metadata": metadata,
            "rows": []
        }
        
        tables = page.find_tables(table_settings={"vertical_strategy": "lines", "horizontal_strategy": "lines"})
        if not tables: return result
        
        main_t = max(tables, key=lambda t: (t.bbox[2]-t.bbox[0]) * (t.bbox[3]-t.bbox[1]))
        x0, top, x1, bottom = main_t.bbox
        
        # Try to find the column divider - if not found, this isn't a regulation table
        try:
            div_x = next(c[2] for c in sorted(main_t.cells, key=itemgetter(1, 0)) if abs(c[0]-x0) < 2 and abs(c[2]-x1) > 5)
        except StopIteration:
            print(f"  [Notice] Could not find column divider - skipping non-regulation table")
            return result
        
        h_buf, v_buf = 2.0, 1.0
        
        sections = self.get_color_sections(page, x0, top, bottom)
        
        # Validate that one of the first few rows contains the header
        has_header = False
        if sections:
            # Check first 2-3 sections for header
            for first_sec in sections[:min(3, len(sections))]:
                y0, y1 = first_sec['y0'], first_sec['y1']
                def is_centered(obj):
                    mid = (obj.get("top",0)+obj.get("bottom",0))/2
                    return y0 <= mid <= y1
                
                first_row = page.filter(is_centered)
                
                # Check left column
                left_x0 = max(0, x0 - h_buf)
                left_y0 = max(0, y0 - v_buf)
                left_x1 = min(page.width, div_x + h_buf)
                left_y1 = min(page.height, y1 + v_buf)
                
                first_left = first_row.within_bbox((left_x0, left_y0, left_x1, left_y1))
                left_text = (first_left.extract_text(layout=True) or "").upper()
                
                # Check right column
                right_x0 = max(0, div_x - h_buf)
                right_y0 = max(0, y0 - v_buf)
                right_x1 = min(page.width, x1 + h_buf)
                right_y1 = min(page.height, y1 + v_buf)
                
                first_right = first_row.within_bbox((right_x0, right_y0, right_x1, right_y1))
                right_text = (first_right.extract_text(layout=True) or "").upper()
                
                if (("WATER BODY" in left_text or "MGMT UNIT" in left_text) and 
                    ("REGULATION" in right_text or "EXCEPTION" in right_text)):
                    has_header = True
                    break
            
            if not has_header:
                print(f"  [Notice] No header row found - skipping entire table")
                return result
        
        if save_debug: 
            self._generate_audit_image(page, page_num)
            self._save_row_crops(page, page_num, sections, div_x, x0, x1)

        structured_data = []

        for sec in sections:
            y0, y1 = sec['y0'], sec['y1']
            def is_centered(obj):
                mid = (obj.get("top",0)+obj.get("bottom",0))/2
                return y0 <= mid <= y1

            row_context = page.filter(is_centered)
            
            # Clamp bbox coordinates to page boundaries
            bbox_x0 = max(0, x0 - h_buf)
            bbox_y0 = max(0, y0 - v_buf)
            bbox_x1 = min(page.width, x1 + h_buf)
            bbox_y1 = min(page.height, y1 + v_buf)
            
            # --- Symbol Detection & Map Rejection ---
            # Run this first to see if we should skip the row entirely
            v_sym_raw = self.detect_visual_symbols(row_context.within_bbox((bbox_x0, bbox_y0, bbox_x1, bbox_y1)))
            
            if "REJECT_REGION" in v_sym_raw:
                print(f"  [Notice] Rejecting row at Y={y0:.1f} due to large map-like vector artifacts.")
                continue

            # Clamp left/right column bboxes
            left_x0 = max(0, x0 - h_buf)
            left_y0 = max(0, y0 - v_buf)
            left_x1 = min(page.width, div_x + h_buf)
            left_y1 = min(page.height, y1 + v_buf)
            
            right_x0 = max(0, div_x - h_buf)
            right_y0 = max(0, y0 - v_buf)
            right_x1 = min(page.width, x1 + h_buf)
            right_y1 = min(page.height, y1 + v_buf)

            left = row_context.within_bbox((left_x0, left_y0, left_x1, left_y1))
            right = row_context.within_bbox((right_x0, right_y0, right_x1, right_y1))
            
            water_raw = left.extract_text(layout=True) or ""
            regs_raw = right.extract_text(layout=True) or ""
            
            w_txt, w_sym, mus = self.process_column_text(water_raw, is_regs=False)
            r_txt, r_sym, _ = self.process_column_text(regs_raw, is_regs=True)
            
            if "WATER BODY" in w_txt.upper() or "MGMT UNIT" in w_txt.upper():
                continue
                
            all_syms = list(set(v_sym_raw + w_sym + r_sym))
            
            # Only include rows that have a water body name OR management units (real regulation data)
            if (w_txt.strip() or mus) and (w_txt or r_txt or mus or all_syms):
                structured_data.append({
                    'water': w_txt, 
                    'mu': mus, 
                    'regs': r_txt, 
                    'symbols': all_syms,
                    'page': page_num
                })
        
        result["rows"] = structured_data
        print(f"  [Success] Extracted {len(structured_data)} data rows for {region_header or 'Unknown Region'}.")
        return result

    def get_color_sections(self, page, x0, top, bottom):
        img = page.to_image(resolution=150).original
        scale = img.width / float(page.width)
        px_x = int((x0 + 2) * scale)
        sections, last_color, start_y = [], None, top
        for py in range(int(top * scale), int(bottom * scale)):
            color = img.getpixel((px_x, py))
            if all(c < 50 for c in color[:3]): continue
            if last_color is None: last_color = color; continue
            if color != last_color:
                sections.append({'y0': start_y, 'y1': py/scale, 'color': last_color})
                start_y, last_color = py/scale, color
        sections.append({'y0': start_y, 'y1': bottom, 'color': last_color or (255,255,255)})
        return sections

    def process_column_text(self, text, is_regs=False):
        symbols, mu_list = [], []
        
        # 1. Handle Empty Input
        if not text: 
            return ([] if is_regs else ""), symbols, [] 
        
        # 2. Extract Management Units (MUs)
        # Finds patterns like '4-8', '4-15' but ignores '(5-15)' or 'M.U. 5-15'
        mu_pattern = r'(?<!\()(?<!M\.U\. )\b\d{1,2}-\d{1,2}\b'
        
        if not is_regs:
            found_mus = re.findall(mu_pattern, text)
            if found_mus:
                # Store unique MUs preserving order
                mu_list = list(dict.fromkeys(found_mus))
                for mu in mu_list: text = text.replace(mu, "")
        
        # 3. Extract CW (Classified Waters)
        if re.search(r'\bCW\b', text):
            symbols.append("Classified")
            if not is_regs: text = re.sub(r'\bCW\b', "", text)
        
        # 4. Extract Tributaries Symbols
        trib_pattern = r'[\uf0dc\uf02a\*]'
        if re.search(trib_pattern, text) or "Includes tributaries" in text or "Incl. Tribs" in text:
            if "Incl. Tribs" not in symbols: symbols.append("Incl. Tribs")
            text = re.sub(trib_pattern, " [Incl. Tribs] " if is_regs else "", text)
            if not is_regs: text = text.replace("Includes tributaries", "").replace("Incl. Tribs", "")
            
        # 5. Clean Text
        lines = text.split('\n')
        cleaned_lines = [re.sub(r'[ \t]+', ' ', l).strip() for l in lines]
        cleaned_text = "\n".join(cleaned_lines).strip()
        
        # 6. Parse Regulations if needed
        if is_regs:
            if cleaned_text:
                return self.RegParser.parse_reg(cleaned_text), symbols, mu_list
            else:
                return [], symbols, mu_list

        return cleaned_text, symbols, mu_list


# --- 4. PRESENTATION ---

def smart_wrap(text, width):
    if not text:
        return []
    paragraphs = text.split('\n')
    wrapped_lines = []
    for para in paragraphs:
        if not para.strip():
            wrapped_lines.append("")
            continue
        wrapped_lines.extend(textwrap.wrap(para, width=width))
    return wrapped_lines

def print_pretty_table(page_result):
    if not page_result or not page_result.get("rows"):
        return

    meta = page_result["metadata"]
    rows = page_result["rows"]

    # Print Page Metadata Header
    print("\n" + "#" * 60)
    print(f"  PAGE: {meta['page_number']}  |  REGION: {meta['region'] or 'N/A'}")
    print("#" * 60)

    avail = shutil.get_terminal_size((80, 20)).columns - 15
    w_w, m_w, s_w = int(avail * 0.25), int(avail * 0.10), int(avail * 0.15)
    r_w = avail - w_w - m_w - s_w
    
    sep = f"{'-'*w_w}-+-{'-'*m_w}-+-{'-'*s_w}-+-{'-'*r_w}"
    print(f"{'WATER BODY':<{w_w}} | {'MU':<{m_w}} | {'SYMBOLS':<{s_w}} | {'REGULATIONS'}\n{'='*len(sep)}")
    
    for row in rows:
        w_l = smart_wrap(row['water'], width=w_w) or [""]
        
        mu_val = row.get('mu', [])
        mu_str = ", ".join(mu_val) if isinstance(mu_val, list) else str(mu_val)
        m_l = smart_wrap(mu_str, width=m_w) or [""]
        
        s_l = smart_wrap(", ".join(row['symbols']), width=s_w) or [""]
        
        # --- CHANGED: HANGING INDENT + SPACING LOGIC ---
        reg_data = row['regs']
        r_l = []
        if isinstance(reg_data, list):
            for i, item in enumerate(reg_data):
                # Wrap each specific regulation item individually
                # initial_indent puts the bullet on the first line
                # subsequent_indent adds spaces to align the next lines (hanging indent)
                lines = textwrap.wrap(
                    item['details'], 
                    width=r_w, 
                    initial_indent="* ", 
                    subsequent_indent="  " 
                )
                r_l.extend(lines)
                
                # Add a blank line between points (but not after the last one)
                if i < len(reg_data) - 1:
                    r_l.append("")
        else:
            # Fallback if data is malformed
            r_l = smart_wrap(str(reg_data), width=r_w)
        
        if not r_l: r_l = [""]
        # -----------------------------------------------
        
        for i in range(max(len(w_l), len(m_l), len(r_l), len(s_l))):
            w = w_l[i] if i < len(w_l) else ""
            m = m_l[i] if i < len(m_l) else ""
            s = s_l[i] if i < len(s_l) else ""
            r = r_l[i] if i < len(r_l) else ""
            print(f"{w:<{w_w}} | {m:<{m_w}} | {s:<{s_w}} | {r}")
        print(sep)


# ==========================================
#      FULL PDF EXTRACTOR CLASS
# ==========================================

class SynopsisExtractor:
    """
    High-level class that handles downloading and extracting the entire
    BC Freshwater Fishing Synopsis PDF, organizing results by region.
    """
    
    SYNOPSIS_URL = "https://www2.gov.bc.ca/assets/gov/sports-recreation-arts-and-culture/outdoor-recreation/fishing-and-hunting/freshwater-fishing/fishing_synopsis.pdf"
    
    def __init__(self, output_dir="output"):
        self.parser = FishingSynopsisParser(output_dir=output_dir)
        self.output_dir = output_dir
        self.pdf_path = os.path.join(output_dir, "fishing_synopsis.pdf")
        
    def download_pdf(self, url=None, filename=None):
        """Download the PDF if it doesn't already exist."""
        if url is None:
            url = self.SYNOPSIS_URL
        if filename is None:
            filename = self.pdf_path
            
        if os.path.exists(filename):
            print(f"PDF already exists at {filename}")
            return
            
        print(f"Downloading PDF from {url}...")
        try:
            import requests
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, stream=True, headers=headers)
            response.raise_for_status()
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Download complete: {filename}")
        except Exception as e:
            print(f"Error downloading PDF: {e}")
            exit()
    
    def extract_all_pages(self):
        """
        Extract rows from all pages in the PDF and organize by region.
        Returns a dictionary with regions as keys and lists of waterbody entries as values.
        
        Structure:
        {
            "REGION 1 - Vancouver Island": {
                "alice lake": [row1, row2, ...],
                "campbell river": [row1],
                ...
            },
            "REGION 2 - Lower Mainland": {
                ...
            }
        }
        """
        if not os.path.exists(self.pdf_path):
            print(f"PDF not found at {self.pdf_path}. Downloading...")
            self.download_pdf()
        
        print(f"Extracting all pages from {self.pdf_path}...")
        
        regions_data = {}
        current_region = "Unknown Region"
        
        with pdfplumber.open(self.pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                # Extract rows from this page
                result = self.parser.extract_rows(page)
                
                # Update current region if metadata has it
                if result['metadata'].get('region'):
                    current_region = result['metadata']['region']
                
                # Skip pages with no rows
                if not result['rows']:
                    continue
                
                # Initialize region in output dict if needed
                if current_region not in regions_data:
                    regions_data[current_region] = {}
                
                # Add rows to the appropriate region
                for row in result['rows']:
                    # Normalize waterbody name for grouping (lowercase, strip whitespace)
                    water_name = row['water'].strip().lower()
                    
                    # Initialize array for this waterbody if needed
                    if water_name not in regions_data[current_region]:
                        regions_data[current_region][water_name] = []
                    
                    # Append this row to the array for this waterbody
                    regions_data[current_region][water_name].append(row)
                
                # Progress indicator
                if page_num % 10 == 0:
                    print(f"  Processed {page_num} pages...")
        
        print(f"Extraction complete. Found {len(regions_data)} regions.")
        return regions_data
    
    def save_to_json(self, data, filename="synopsis_data.json"):
        """Save the extracted data to a JSON file."""
        import json
        output_path = os.path.join(self.output_dir, filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Saved data to {output_path}")
        return output_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--page", type=int, default=37)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--extract-all", action="store_true", help="Extract all pages and save to JSON")
    args = parser.parse_args()
    
    if args.extract_all:
        # Use the new SynopsisExtractor class
        extractor = SynopsisExtractor()
        extractor.download_pdf()
        data = extractor.extract_all_pages()
        extractor.save_to_json(data)
    else:
        # Single page extraction (original behavior)
        p = FishingSynopsisParser()
        PDF_PATH = os.path.join(p.output_dir, "fishing_synopsis.pdf")
        
        with pdfplumber.open(PDF_PATH) as pdf:
            page_result = p.extract_rows(pdf.pages[args.page - 1], save_debug=args.debug)
            print_pretty_table(page_result)

if __name__ == "__main__":
    main()