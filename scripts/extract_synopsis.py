import pdfplumber
import os
import argparse
import re
import textwrap
import shutil
import json
import numpy as np
from typing import List, Optional, Dict, Any
from operator import itemgetter
from collections import namedtuple
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from PIL import Image, ImageDraw
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from concurrent.futures import ProcessPoolExecutor, as_completed

# Import shared data models
from synopsis_pipeline.models import (
    WaterbodyRow,
    PageMetadata,
    PageResult,
    ExtractionResults,
)

Sequence = namedtuple(
    "Sequence", ["bbox", "y_mid", "avg_render_idx", "text", "char_indices"]
)


class FishingSynopsisParser:
    def __init__(self, output_dir="output", debug_dir="debug", audit_dir="debug"):
        # Use extract_synopsis subfolder for all output
        self.base_output_dir = os.path.join(output_dir, "extract_synopsis")
        self.output_dir = self.base_output_dir
        self.debug_dir = os.path.join(self.base_output_dir, debug_dir)
        self.audit_dir = os.path.join(self.base_output_dir, audit_dir)

        # Ensure base directory exists
        os.makedirs(self.base_output_dir, exist_ok=True)

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

    def _get_bg_palette(self, img_pil):
        small = img_pil.resize((200, int(200 * img_pil.height / img_pil.width)))
        colors = (
            small.convert("P", palette=Image.ADAPTIVE, colors=256)
            .convert("RGB")
            .getcolors(maxcolors=256 * 256)
        )
        colors.sort(key=lambda x: x[0], reverse=True)
        backgrounds = []
        for count, rgb in colors:
            if sum(rgb) < 150:
                continue
            backgrounds.append(np.array(rgb))
            if len(backgrounds) >= 2:
                break
        if len(backgrounds) < 2:
            backgrounds.append(np.array([255, 255, 255]))
        return backgrounds

    def _check_char_sanity(self, char, np_img, scale, bg_pal):
        fs = char.get("size", 0)
        if fs < self.MIN_FONT_SIZE or fs > self.MAX_FONT_SIZE:
            return False
        if char["text"].strip() == "":
            return True
        x0, y0, x1, y1 = [
            int(v * scale)
            for v in [char["x0"], char["top"], char["x1"], char["bottom"]]
        ]
        crop = np_img[
            max(0, y0) : min(np_img.shape[0], y1), max(0, x0) : min(np_img.shape[1], x1)
        ]
        if crop.size == 0:
            return True
        pixels = crop.reshape(-1, 3)
        bg1, bg2 = bg_pal[0], bg_pal[1]
        is_distinct = (
            np.linalg.norm(pixels - bg1, axis=1) > self.COLOR_DIFF_THRESHOLD
        ) & (np.linalg.norm(pixels - bg2, axis=1) > self.COLOR_DIFF_THRESHOLD)
        is_dark = np.dot(pixels, [0.299, 0.587, 0.114]) < self.LUMINANCE_THRESHOLD
        ink_count = np.sum(is_distinct & is_dark)
        thresh = (
            1 if char["text"] in self.SYMBOLS_TO_RELAX else self.MIN_INK_PIXELS_STANDARD
        )
        return ink_count >= thresh

    def _check_group_sanity(self, chars, np_img, scale, bg_pal):
        if not "".join([c["text"] for c in chars]).strip():
            return False
        bad_chars = sum(
            1 for c in chars if not self._check_char_sanity(c, np_img, scale, bg_pal)
        )
        return (bad_chars / len(chars)) < 0.2

    def get_cleaned_page(self, page, save_debug=False, page_num=None):
        resolution = 400
        scale = resolution / 72
        img = page.to_image(resolution=resolution).original.convert("RGB")
        np_img = np.array(img)
        bg_pal = self._get_bg_palette(img)

        all_chars = page.chars
        for i, c in enumerate(all_chars):
            c["render_index"] = i

        chars_sorted = sorted(all_chars, key=lambda c: c["top"])
        lines = []
        for c in chars_sorted:
            placed = False
            for l in lines:
                overlap = max(
                    0, min(c["bottom"], l[-1]["bottom"]) - max(c["top"], l[-1]["top"])
                )
                if (c["bottom"] - c["top"]) > 0 and overlap / (
                    c["bottom"] - c["top"]
                ) > 0.4:
                    l.append(c)
                    placed = True
                    break
            if not placed:
                lines.append([c])

        sequences, raw_groups = [], []
        for row in lines:
            row.sort(key=lambda x: x["render_index"])
            if not row:
                continue
            active = [row[0]]
            for i in range(1, len(row)):
                if -3 < (row[i]["x0"] - active[-1]["x1"]) < 3:
                    active.append(row[i])
                else:
                    txt = "".join([c["text"] for c in active])
                    if txt.strip():
                        idx = [c["render_index"] for c in active]
                        bbox = (
                            min(c["x0"] for c in active),
                            min(c["top"] for c in active),
                            max(c["x1"] for c in active),
                            max(c["bottom"] for c in active),
                        )
                        sequences.append(
                            Sequence(
                                bbox,
                                (active[0]["top"] + active[0]["bottom"]) / 2,
                                sum(idx) / len(idx),
                                txt,
                                idx,
                            )
                        )
                        raw_groups.append(active)
                    active = [row[i]]
            if active:
                txt = "".join([c["text"] for c in active])
                if txt.strip():
                    idx = [c["render_index"] for c in active]
                    bbox = (
                        min(c["x0"] for c in active),
                        min(c["top"] for c in active),
                        max(c["x1"] for c in active),
                        max(c["bottom"] for c in active),
                    )
                    sequences.append(
                        Sequence(
                            bbox,
                            (active[0]["top"] + active[0]["bottom"]) / 2,
                            sum(idx) / len(idx),
                            txt,
                            idx,
                        )
                    )
                    raw_groups.append(active)

        if not sequences:
            return page
        X = np.array([[s.y_mid, s.avg_render_idx] for s in sequences])
        labels = (
            DBSCAN(eps=self.DBSCAN_EPS, min_samples=self.DBSCAN_MIN_SAMPLES)
            .fit(StandardScaler().fit_transform(X))
            .labels_
        )

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
                cluster_stats[k] = {
                    "total": len(indices),
                    "kept": kept,
                    "rate": kept / len(indices) if len(indices) > 0 else 0,
                }
            else:
                sane_count = sum(
                    1
                    for idx in indices
                    if self._check_group_sanity(raw_groups[idx], np_img, scale, bg_pal)
                )
                passthrough = (sane_count / len(indices)) >= self.CLUSTER_PASS_THRESHOLD
                if passthrough:
                    for idx in indices:
                        keep_indices.update(sequences[idx].char_indices)
                cluster_stats[k] = {
                    "total": len(indices),
                    "kept": len(indices) if passthrough else 0,
                    "rate": 1.0 if passthrough else 0.0,
                }

        # Save debug visualizations if requested
        if save_debug and page_num is not None:
            self._save_cluster_debug(
                page,
                page_num,
                sequences,
                labels,
                cluster_stats,
                keep_indices,
                np_img,
                scale,
            )

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
            first_line_chars = sorted(lines[0], key=lambda c: c.get("render_index", 0))
            first_line_text = "".join([c.get("text", "") for c in first_line_chars])
            render_indices = [c.get("render_index", -1) for c in first_line_chars]
            # Print characters with their render indices aligned
            char_display = " ".join(
                [f"{c.get('text', ' '):>4}" for c in first_line_chars]
            )
            idx_display = " ".join(
                [f"{c.get('render_index', -1):>4}" for c in first_line_chars]
            )
            print(f"  [Debug] First line chars (n={len(first_line_chars)}):")
            print(f"    Chars:   {char_display}")
            print(f"    Indices: {idx_display}")

        # Remove overlapping characters from first line only
        overlap_exists = False

        if lines and lines[0]:
            # Get only the characters from first line that are in keep_indices
            line_chars = [c for c in lines[0] if c["render_index"] in keep_indices]

            # Check each character against all others on the same line for any overlap
            for i, c1 in enumerate(line_chars):
                for j in range(i + 1, len(line_chars)):
                    c2 = line_chars[j]
                    # Shrink bboxes by 10% to avoid accidental overlap from font rendering
                    shrink = 0.1
                    c1_w, c1_h = c1["x1"] - c1["x0"], c1["bottom"] - c1["top"]
                    c2_w, c2_h = c2["x1"] - c2["x0"], c2["bottom"] - c2["top"]

                    c1_x0 = c1["x0"] + c1_w * shrink
                    c1_x1 = c1["x1"] - c1_w * shrink
                    c1_top = c1["top"] + c1_h * shrink
                    c1_bottom = c1["bottom"] - c1_h * shrink

                    c2_x0 = c2["x0"] + c2_w * shrink
                    c2_x1 = c2["x1"] - c2_w * shrink
                    c2_top = c2["top"] + c2_h * shrink
                    c2_bottom = c2["bottom"] - c2_h * shrink

                    x_overlap = max(0, min(c1_x1, c2_x1) - max(c1_x0, c2_x0))
                    y_overlap = max(0, min(c1_bottom, c2_bottom) - max(c1_top, c2_top))

                    if x_overlap * y_overlap > 0:
                        overlap_exists = True
                        break

        chars_to_remove = set()
        if overlap_exists:
            mean_render_idx = np.mean(
                [
                    c["render_index"]
                    for c in lines[0]
                    if c["render_index"] in keep_indices
                ]
            )
            chars_to_remove = set(
                [
                    c["render_index"]
                    for c in lines[0]
                    if c["render_index"] < mean_render_idx
                    and c["render_index"] in keep_indices
                ]
            )
        # Update keep_indices to exclude duplicates
        keep_indices -= chars_to_remove

        # Debug: Print what remains on first line
        if save_debug and lines and lines[0]:
            remaining = [
                c
                for c in sorted(lines[0], key=lambda c: c.get("render_index", 0))
                if c["render_index"] in keep_indices
            ]
            remaining_text = "".join([c.get("text", "") for c in remaining])
            print(
                f"  [Debug] After overlap removal (n={len(remaining)}): {remaining_text}"
            )

        # ===========================================================================
        # END OF BROKEN CODE
        # ==========================================================================

        return page.filter(
            lambda o: (
                o.get("render_index") in keep_indices
                if o.get("object_type") == "char"
                else True
            )
        )

    # --- 2. SYMBOL & DEBUG LOGIC ---

    def _save_cluster_debug(
        self,
        page,
        page_num,
        sequences,
        labels,
        cluster_stats,
        keep_indices,
        np_img,
        scale,
    ):
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

            rect = patches.Rectangle(
                (x0, y0),
                x1 - x0,
                y1 - y0,
                linewidth=1.5,
                edgecolor="black",
                facecolor=color,
                alpha=0.5,
            )
            ax.add_patch(rect)

        # Add colorbar
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        cbar = plt.colorbar(sm, ax=ax, fraction=0.046, pad=0.04)
        cbar.set_label("Average Render Index", fontsize=12)

        ax.set_title(
            f"Page {page_num} - All Groups Before Clustering (colored by render index)",
            fontsize=12,
        )
        ax.axis("off")

        pre_cluster_path = os.path.join(
            page_debug_dir, "pre_clustering_render_order.png"
        )
        plt.savefig(pre_cluster_path, dpi=150, bbox_inches="tight")
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
                col = "black"  # Noise points

            class_member_mask = labels == k
            xy = X[class_member_mask]

            stats = cluster_stats.get(k, {})
            rate = stats.get("rate", 0)
            total = stats.get("total", 0)
            kept = stats.get("kept", 0)

            label = f"Cluster {k}: {kept}/{total} ({rate*100:.1f}%)"
            ax.scatter(
                xy[:, 0],
                xy[:, 1],
                c=[col],
                s=100,
                alpha=0.6,
                edgecolors="black",
                label=label,
            )

        ax.set_xlabel("Y Position (mid)", fontsize=12)
        ax.set_ylabel("Average Render Index", fontsize=12)
        ax.set_title(
            f"Page {page_num} - DBSCAN Clusters (eps={self.DBSCAN_EPS}, min_samples={self.DBSCAN_MIN_SAMPLES})",
            fontsize=14,
        )
        ax.legend(loc="best", fontsize=9)
        ax.grid(True, alpha=0.3)

        scatter_path = os.path.join(page_debug_dir, "cluster_scatter.png")
        plt.savefig(scatter_path, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"  [Debug] Saved cluster scatter: {scatter_path}")

        # 2. Save character bbox images for each cluster
        for k in unique_labels:
            cluster_dir = os.path.join(
                page_debug_dir, f"cluster_{k if k != -1 else 'noise'}"
            )
            os.makedirs(cluster_dir, exist_ok=True)

            # Get sequences in this cluster
            cluster_mask = labels == k
            cluster_sequences = [
                seq for i, seq in enumerate(sequences) if cluster_mask[i]
            ]

            if not cluster_sequences:
                continue

            # Create visualization showing all characters in this cluster
            fig, ax = plt.subplots(figsize=(14, 10))
            ax.imshow(np_img)

            stats = cluster_stats.get(k, {})
            kept = stats.get("kept", 0)
            total = stats.get("total", 0)
            rate = stats.get("rate", 0)

            # Draw bounding boxes for each character in this cluster
            for seq in cluster_sequences:
                for char_idx in seq.char_indices:
                    # Find the character by render_index
                    char = next(
                        (c for c in page.chars if c.get("render_index") == char_idx),
                        None,
                    )
                    if char:
                        x0, y0, x1, y1 = (
                            char["x0"] * scale,
                            char["top"] * scale,
                            char["x1"] * scale,
                            char["bottom"] * scale,
                        )

                        # Color: green if kept, red if filtered out
                        color = "green" if char_idx in keep_indices else "red"
                        rect = patches.Rectangle(
                            (x0, y0),
                            x1 - x0,
                            y1 - y0,
                            linewidth=1,
                            edgecolor=color,
                            facecolor="none",
                            alpha=0.7,
                        )
                        ax.add_patch(rect)

            status = "KEPT" if kept == total else f"FILTERED ({kept}/{total})"
            ax.set_title(
                f"Cluster {k if k != -1 else 'Noise'} - {status} - Passthrough: {rate*100:.1f}%",
                fontsize=12,
            )
            ax.axis("off")

            cluster_img_path = os.path.join(cluster_dir, "character_bboxes.png")
            plt.savefig(cluster_img_path, dpi=150, bbox_inches="tight")
            plt.close()

        print(f"  [Debug] Saved cluster visualizations to: {page_debug_dir}")

    def detect_visual_symbols(self, section_context):
        """Identifies icons via vector curve geometry and rejects large map artifacts."""
        symbols = []
        for curve in section_context.curves:
            width = curve["x1"] - curve["x0"]
            height = curve["bottom"] - curve["top"]

            # If we find a massive vector object, trigger a rejection flag
            if width > self.MAP_REJECTION_DIM or height > self.MAP_REJECTION_DIM:
                return ["REJECT_REGION"]

            # Normal 'Stocked' symbol detection
            if (self.MIN_SYMBOL_DIM < width < self.MAX_SYMBOL_DIM) and (
                self.MIN_SYMBOL_DIM < height < self.MAX_SYMBOL_DIM
            ):
                if "Stocked" not in symbols:
                    symbols.append("Stocked")
        return symbols

    def _is_bold_font(self, char: Dict[str, Any]) -> bool:
        """
        Determine if a character uses a bold font.

        PDFs can indicate bold in several ways:
        1. Font name contains 'Bold', 'Heavy', 'Black', etc.
        2. Font weight > 400 (if available)
        3. Font name ends with '-B' or ',Bold'

        Args:
            char: Character dict from pdfplumber with 'fontname' key

        Returns:
            True if character is bold, False otherwise
        """
        fontname = char.get("fontname", "")
        if not fontname:
            return False

        # Check for common bold indicators in font name
        fontname_upper = fontname.upper()
        bold_indicators = ["BOLD", "HEAVY", "BLACK", "SEMIBOLD", "-B", ",BOLD"]

        return any(indicator in fontname_upper for indicator in bold_indicators)

    def extract_text_with_bold(self, page_section) -> str:
        """
        Extract text from a page section with bold formatting preserved using Markdown syntax.

        Args:
            page_section: A pdfplumber page or filtered page section

        Returns:
            Text string with bold portions wrapped in **
        """
        chars = page_section.chars
        if not chars:
            return ""

        # Group characters into lines based on vertical position
        result_lines = []
        current_line = []
        current_line_top = None
        line_height_threshold = (
            5  # Points - characters within this vertical distance are on same line
        )

        # Sort all characters by top position first
        chars_by_top = sorted(chars, key=lambda c: c.get("top", 0))

        for char in chars_by_top:
            char_top = char.get("top", 0)

            # Check if this character is on a new line
            if current_line_top is None:
                current_line_top = char_top
            elif abs(char_top - current_line_top) > line_height_threshold:
                # Process the completed line - IMPORTANT: sort by x0 before processing
                if current_line:
                    current_line.sort(key=lambda c: c.get("x0", 0))
                    result_lines.append(self._process_line_with_bold(current_line))
                current_line = []
                current_line_top = char_top

            current_line.append(char)

        # Process the last line - IMPORTANT: sort by x0 before processing
        if current_line:
            current_line.sort(key=lambda c: c.get("x0", 0))
            result_lines.append(self._process_line_with_bold(current_line))

        return "\n".join(result_lines)

    def _process_line_with_bold(self, chars: List[Dict[str, Any]]) -> str:
        """
        Process a line of characters, grouping them by bold status and building a string
        with ** markers around bold segments.

        Args:
            chars: List of character dicts on the same line, already sorted by x0

        Returns:
            String with bold text wrapped in **
        """
        if not chars:
            return ""

        result = []
        current_segment = []
        current_segment_chars = []  # Track actual char objects for spacing detection
        current_is_bold = None

        for i, char in enumerate(chars):
            text = char.get("text", "")
            is_bold = self._is_bold_font(char)

            # Detect if we need a space before this character
            # Check horizontal gap between this char and previous char in segment
            needs_space = False
            if current_segment_chars:
                prev_char = current_segment_chars[-1]
                # Calculate gap: current char's left edge - previous char's right edge
                gap = char.get("x0", 0) - prev_char.get("x1", 0)
                # If gap is larger than ~20% of the character width, insert space
                char_width = char.get("width", 0)
                if char_width > 0 and gap > char_width * 0.2:
                    needs_space = True

            # If bold status changed, flush current segment
            if current_is_bold is not None and is_bold != current_is_bold:
                segment_text = "".join(current_segment)
                if segment_text:  # Only add non-empty segments
                    if current_is_bold:
                        result.append(f"**{segment_text}**")
                    else:
                        result.append(segment_text)
                current_segment = []
                current_segment_chars = []
                needs_space = False  # Space will be handled between segments naturally

            # Add space if needed before adding the character
            if needs_space and current_segment:
                current_segment.append(" ")

            current_segment.append(text)
            current_segment_chars.append(char)
            current_is_bold = is_bold

        # Flush the last segment
        if current_segment:
            segment_text = "".join(current_segment)
            if segment_text:
                if current_is_bold:
                    result.append(f"**{segment_text}**")
                else:
                    result.append(segment_text)

        return "".join(result)

    def _generate_audit_image(self, clean_page, page_num):
        page_debug_dir = os.path.join(self.audit_dir, f"page_{page_num:03d}")
        os.makedirs(page_debug_dir, exist_ok=True)
        res = 150
        scale = res / 72
        im = clean_page.to_image(resolution=res).original.convert("RGBA")
        overlay = Image.new("RGBA", im.size, (255, 255, 255, 0))
        draw = ImageDraw.Draw(overlay)
        for char in clean_page.chars:
            draw.rectangle(
                [
                    char["x0"] * scale,
                    char["top"] * scale,
                    char["x1"] * scale,
                    char["bottom"] * scale,
                ],
                outline=(255, 0, 0, 80),
            )
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
            l, t = int(max(0, (x0 - 5) * scale)), int(max(0, sec["y0"] * scale))
            r, b = int(min(img_w, (x1 + 5) * scale)), int(min(img_h, sec["y1"] * scale))
            if r <= l or b <= t:
                continue
            crop = page_img.original.crop((l, t, r, b)).copy()
            draw = ImageDraw.Draw(crop)
            rel_div = int((div_x * scale) - l)
            draw.line([(rel_div, 0), (rel_div, crop.height)], fill="red", width=2)
            crop.save(os.path.join(row_crops_dir, f"row_{i:03d}.png"))
        print(f"  [Debug] Saved {len(sections)} row crops to: {row_crops_dir}")

    def save_row_crop(self, page, page_num, row_index, section, x0, x1, div_x):
        """
        Save a single row crop image and return the image key for linking to row data.

        Args:
            page: pdfplumber page object
            page_num: page number
            row_index: index of this row on the page
            section: section dict with y0, y1, color
            x0, x1: left and right bounds of the table
            div_x: dividing line between waterbody and regulation columns

        Returns:
            str: unique image key (e.g., "page_037_row_005.png")
        """
        # Create row_images directory in extract_synopsis subfolder
        row_images_dir = os.path.join(self.base_output_dir, "row_images")
        os.makedirs(row_images_dir, exist_ok=True)

        # Generate unique image filename
        image_key = f"page_{page_num:03d}_row_{row_index:03d}.png"
        image_path = os.path.join(row_images_dir, image_key)

        # Render and crop the image at high resolution for quality
        page_img = page.to_image(resolution=400)
        img_w, img_h = page_img.original.size
        scale = img_w / float(page.width)

        # Calculate crop bounds with vertical margin for better context
        vertical_margin = 5  # Points in PDF coordinates
        horizontal_margin = 5  # Points in PDF coordinates
        l = int(max(0, (x0 - horizontal_margin) * scale))
        t = int(max(0, (section["y0"] - vertical_margin) * scale))
        r = int(min(img_w, (x1 + horizontal_margin) * scale))
        b = int(min(img_h, (section["y1"] + vertical_margin) * scale))

        if r > l and b > t:
            crop = page_img.original.crop((l, t, r, b)).copy()

            # Draw a box around the actual row data (excluding the margins)
            draw = ImageDraw.Draw(crop)
            # Calculate the box bounds to outline the actual row without the margin
            box_left = int(horizontal_margin * scale)
            box_top = int(vertical_margin * scale)
            box_right = crop.width - int(horizontal_margin * scale)
            box_bottom = crop.height - int(vertical_margin * scale)

            # Draw rectangle with red outline around the actual row
            draw.rectangle(
                [(box_left, box_top), (box_right, box_bottom)], outline="red", width=2
            )

            crop.save(image_path)

        return image_key

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
            if word["bottom"] - word["top"] > 10:
                header_text.append(word["text"])

        full_text = " ".join(header_text)

        # If no large text found, try with a lower threshold (8pt)
        if not full_text.strip():
            header_text = []
            for word in words:
                if word["bottom"] - word["top"] > 8:
                    header_text.append(word["text"])
            full_text = " ".join(header_text)

        # Regex to find "REGION 4 - Kootenay" or "REGION 7A - Omineca" or "REGION 3 - Thompson-Nicola"
        # Handles region names with hyphens (Thompson-Nicola) and spaces (Lower Mainland)
        # Pattern: REGION + number + optional letter + dash + region name (words with hyphens/spaces)
        match = re.search(
            r"(REGION\s+\d+[A-Z]?\s*[-–]\s+[A-Za-z]+(?:[-\s][A-Za-z]+)*)",
            full_text,
            re.IGNORECASE,
        )

        if match:
            region = match.group(1).strip()
            # Remove common suffix words that aren't part of the region name
            region = re.sub(
                r"\s+(Water-Specific|Regional|Water|Regulations|Specific|EXCEPTIONS|BODY).*$",
                "",
                region,
                flags=re.IGNORECASE,
            )
            return self._normalize_region_name(region.strip())

        # Fallback: Look for just "REGION X -" pattern and be more flexible
        match = re.search(
            r"(REGION\s+\d+[A-Z]?\s*[-–]\s+[\w-]+(?:\s+[\w-]+)?)",
            full_text,
            re.IGNORECASE,
        )
        if match:
            region = match.group(1).strip()
            region = re.sub(
                r"\s+(Water-Specific|Regional|Water|Regulations|Specific|EXCEPTIONS|BODY).*$",
                "",
                region,
                flags=re.IGNORECASE,
            )
            return self._normalize_region_name(region.strip())

        return None

    def _normalize_region_name(self, region_name):
        """
        Normalize region name to consistent format: "REGION X - Title Case Name"
        Examples:
          "REGION 1 - VANCOUVER ISLAND" -> "REGION 1 - Vancouver Island"
          "region 2 - lower mainland" -> "REGION 2 - Lower Mainland"
          "REGION 3 - thompson-nicola" -> "REGION 3 - Thompson-Nicola"
        """
        if not region_name:
            return None

        # Split into "REGION X" and the name part
        match = re.match(
            r"(REGION\s+\d+[A-Z]?)\s*[-–]\s*(.+)", region_name, re.IGNORECASE
        )
        if not match:
            return region_name  # Return as-is if doesn't match expected pattern

        region_number = match.group(1).upper()  # Always uppercase "REGION X"
        region_name_part = match.group(2).strip()

        # Title case the region name, preserving hyphens
        region_name_part = region_name_part.title()

        return f"{region_number} - {region_name_part}"

    def _validate_table_header(
        self, page: Any, sections: List[Dict], x0: float, div_x: float, x1: float
    ) -> bool:
        """
        Validate that one of the first few rows contains the expected header.

        Args:
            page: Cleaned pdfplumber page
            sections: List of color sections
            x0, div_x, x1: Table column boundaries

        Returns:
            True if valid header found, False otherwise
        """
        h_buf, v_buf = 2.0, 1.0

        if not sections:
            return False

        # Check first 2-3 sections for header
        for first_sec in sections[: min(3, len(sections))]:
            y0, y1 = first_sec["y0"], first_sec["y1"]

            def is_centered(obj):
                mid = (obj.get("top", 0) + obj.get("bottom", 0)) / 2
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

            first_right = first_row.within_bbox(
                (right_x0, right_y0, right_x1, right_y1)
            )
            right_text = (first_right.extract_text(layout=True) or "").upper()

            if ("WATER BODY" in left_text or "MGMT UNIT" in left_text) and (
                "REGULATION" in right_text or "EXCEPTION" in right_text
            ):
                return True

        return False

    def extract_rows(self, raw_page, save_debug=False, page_num=None) -> PageResult:
        if page_num is None:
            page_num = raw_page.page_number

        # 1. Clean the page (removes background noise)
        page = self.get_cleaned_page(raw_page, save_debug=save_debug, page_num=page_num)

        # 2. Extract Metadata (Region Name)
        region_header = self._extract_region_header(page)

        metadata = PageMetadata(page_number=page_num, region=region_header)

        tables = page.find_tables(
            table_settings={
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
            }
        )
        if not tables:
            return PageResult(metadata=metadata, rows=[])

        main_t = max(
            tables, key=lambda t: (t.bbox[2] - t.bbox[0]) * (t.bbox[3] - t.bbox[1])
        )
        x0, top, x1, bottom = main_t.bbox

        # Try to find the column divider - if not found, this isn't a regulation table
        try:
            div_x = next(
                c[2]
                for c in sorted(main_t.cells, key=itemgetter(1, 0))
                if abs(c[0] - x0) < 2 and abs(c[2] - x1) > 5
            )
        except StopIteration:
            return PageResult(metadata=metadata, rows=[])

        h_buf, v_buf = 2.0, 1.0

        sections = self.get_color_sections(page, x0, top, bottom)

        # Validate table header using private method
        if not self._validate_table_header(page, sections, x0, div_x, x1):
            return PageResult(metadata=metadata, rows=[])

        if save_debug:
            self._generate_audit_image(page, page_num)
            self._save_row_crops(page, page_num, sections, div_x, x0, x1)

        structured_data = []

        for sec in sections:
            y0, y1 = sec["y0"], sec["y1"]

            def is_centered(obj):
                mid = (obj.get("top", 0) + obj.get("bottom", 0)) / 2
                return y0 <= mid <= y1

            row_context = page.filter(is_centered)

            # Clamp bbox coordinates to page boundaries
            bbox_x0 = max(0, x0 - h_buf)
            bbox_y0 = max(0, y0 - v_buf)
            bbox_x1 = min(page.width, x1 + h_buf)
            bbox_y1 = min(page.height, y1 + v_buf)

            # --- Symbol Detection & Map Rejection ---
            # Run this first to see if we should skip the row entirely
            v_sym_raw = self.detect_visual_symbols(
                row_context.within_bbox((bbox_x0, bbox_y0, bbox_x1, bbox_y1))
            )

            if "REJECT_REGION" in v_sym_raw:
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

            # Extract text with bold formatting preserved (using ** Markdown syntax)
            water_raw = self.extract_text_with_bold(left) or ""
            regs_raw = self.extract_text_with_bold(right) or ""

            # Debug output: show raw column data before processing
            if save_debug and (water_raw or regs_raw):
                print(f"\n{'='*80}")
                print(f"Row at y={y0:.1f}-{y1:.1f}")
                print(f"{'='*80}")
                print(f"WATER COLUMN (raw):\n{repr(water_raw)}")
                print(f"\nREGS COLUMN (raw):\n{repr(regs_raw)}")
                print(f"{'='*80}\n")

            # Process waterbody column (name, symbols, MUs)
            w_txt, w_sym, mus = self.process_waterbody_column(water_raw)

            if "WATER BODY" in w_txt.upper() or "MGMT UNIT" in w_txt.upper():
                continue

            all_syms = list(set(v_sym_raw + w_sym))

            # Normalize unicode characters to preserve context while standardizing format
            regs_raw = regs_raw.replace(
                "\xa0", " "
            )  # Non-breaking space → regular space
            regs_raw = regs_raw.replace("\u2013", "-")  # En-dash → hyphen
            regs_raw = regs_raw.replace("\u2014", "-")  # Em-dash → hyphen
            regs_raw = regs_raw.replace("\u2018", "'").replace(
                "\u2019", "'"
            )  # Curly single quotes → straight
            regs_raw = regs_raw.replace("\u201c", '"').replace(
                "\u201d", '"'
            )  # Curly double quotes → straight

            # Replace tributaries symbols with standardized placeholder text
            trib_pattern = r"[\uf0dc\uf02a]"
            regs_raw = re.sub(trib_pattern, "[Includes Tributaries]", regs_raw)
            regs_raw = regs_raw.replace(
                "Includes tributaries", "[Includes Tributaries]"
            )
            regs_raw = regs_raw.replace("Incl. Tribs", "[Includes Tributaries]")

            # Fix dates/numbers split across lines with bold markers
            # Step 1: Remove bolded whitespace (spaces/tabs only) and replace with single space
            # "**   **" → " ", "**\t\t**" → " "
            regs_raw = re.sub(r"\*\*[ \t]+\*\*", " ", regs_raw)

            # Step 2: Remove bold markers around newlines (keep the newline)
            # "June **\n**1" → "June \n1"
            regs_raw = re.sub(r"\*\*\s*\n\s*\*\*", "\n", regs_raw)

            # Step 2: Fix date ranges split by newlines
            # Pattern matches: Month Day-Month\nDay or Month\nDay-Month Day
            # Examples: "Apr 1-June\n14" → "Apr 1-June 14", "June\n15-Mar 31" → "June 15-Mar 31"
            month_pattern = r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|Aug(?:ust)?|Sept?(?:ember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
            # Match: (month + optional spaces/digits/hyphens) + newline + (digits)
            date_newline_pattern = (
                rf"({month_pattern}(?:\s+\d+)?\s*-\s*{month_pattern}?)\s*\n\s*(\d+)"
            )
            regs_raw = re.sub(
                date_newline_pattern, r"\1 \2", regs_raw, flags=re.IGNORECASE
            )

            # Also handle: Month\nDay (at start of date range)
            date_start_pattern = rf"({month_pattern})\s*\n\s*(\d+)"
            regs_raw = re.sub(
                date_start_pattern, r"\1 \2", regs_raw, flags=re.IGNORECASE
            )

            # Normalize whitespace: replace multiple spaces/newlines with single space/newline
            regs_raw = re.sub(
                r"[ \t]+", " ", regs_raw
            )  # Multiple spaces/tabs → single space
            regs_raw = re.sub(
                r"\n+", "\n", regs_raw
            )  # Multiple newlines → single newline
            regs_raw = re.sub(
                r" *\n *", "\n", regs_raw
            )  # Remove all spaces around newlines
            regs_raw = regs_raw.strip()

            # Only include rows that have a water body name OR management units (real regulation data)
            if (w_txt.strip() or mus) and (
                w_txt or regs_raw.strip() or mus or all_syms
            ):
                # Validate: warn if waterbody has no MU
                if w_txt.strip() and not mus:
                    print(
                        f"  [Warning] Page {page_num}: '{w_txt}' has no management unit (MU)"
                    )

                # Save row crop image and get image key
                image_key = self.save_row_crop(
                    page, page_num, len(structured_data), sec, x0, x1, div_x
                )

                structured_data.append(
                    WaterbodyRow(
                        water=w_txt,
                        mu=mus,
                        raw_regs=regs_raw.strip(),
                        symbols=all_syms,
                        page=page_num,
                        image=image_key,
                        region=region_header,
                    )
                )

        result = PageResult(metadata=metadata, rows=structured_data)
        if structured_data:
            print(
                f"Page {page_num}: {len(structured_data)} rows ({region_header or 'Unknown'})"
            )
        return result

    def process_waterbody_column(self, text):
        """
        Process waterbody column text to extract name, symbols, and management units.
        Does NOT parse regulations - this is for the left column only.

        Returns: (waterbody_name, symbols, management_units)
        """
        symbols, mu_list = [], []

        if not text:
            return "", symbols, mu_list

        # Normalize unicode characters first
        text = text.replace("\xa0", " ")  # Non-breaking space → regular space
        text = text.replace("\u2013", "-")  # En-dash → hyphen
        text = text.replace("\u2014", "-")  # Em-dash → hyphen
        text = text.replace("\u2018", "'").replace(
            "\u2019", "'"
        )  # Curly single quotes → straight
        text = text.replace("\u201c", '"').replace(
            "\u201d", '"'
        )  # Curly double quotes → straight

        # Remove bold markers before processing (they interfere with MU extraction)
        text = text.replace("**", "")

        # Normalize whitespace BEFORE extracting MUs to handle cases like "CREEK\n6-12"
        text = re.sub(r"\s+", " ", text).strip()

        # Extract CW (Classified Waters) - do this BEFORE MU extraction
        # to handle cases like "CW6-12" where CW is directly attached to the MU
        if re.search(r"\bCW\b", text):
            symbols.append("Classified")
            text = re.sub(r"\bCW\b", "", text)

        # Also handle CW directly attached to MU (e.g., "CW6-12")
        cw_mu_pattern = r"\bCW(\d{1,2}-\d{1,2})\b"
        cw_mu_matches = re.findall(cw_mu_pattern, text)
        if cw_mu_matches:
            if "Classified" not in symbols:
                symbols.append("Classified")
            # Remove the CW prefix but keep the MU for next step
            text = re.sub(r"\bCW(?=\d{1,2}-\d{1,2})", "", text)

        # Extract Management Units (MUs)
        # Updated pattern to handle MUs:
        # - At the start of text (^)
        # - After whitespace (\s)
        # - Directly attached to letters (e.g., "LAKE1-11")
        # But NOT after opening paren or "M.U. "
        mu_pattern = (
            r"(?<!\()(?<!M\.U\. )(?:(?<=\s)|(?<=[A-Za-z])|^)(\d{1,2}-\d{1,2})\b"
        )
        found_mus = re.findall(mu_pattern, text, re.MULTILINE)
        if found_mus:
            mu_list = list(dict.fromkeys(found_mus))
            for mu in mu_list:
                # Use regex with word boundaries to avoid partial matches (e.g., "4-2" in "4-22")
                text = re.sub(r"\b" + re.escape(mu) + r"\b", "", text)

        # Extract Tributaries Symbols
        trib_pattern = r"[\uf0dc\uf02a\*]"
        if (
            re.search(trib_pattern, text)
            or "Includes tributaries" in text
            or "Incl. Tribs" in text
        ):
            if "Incl. Tribs" not in symbols:
                symbols.append("Incl. Tribs")
            text = re.sub(trib_pattern, "", text)
            text = text.replace("Includes tributaries", "").replace("Incl. Tribs", "")

        # Final cleanup - normalize any remaining multiple spaces
        text = re.sub(r"\s+", " ", text).strip()

        return text, symbols, mu_list

    def get_color_sections(self, page, x0, top, bottom):
        img = page.to_image(resolution=150).original
        scale = img.width / float(page.width)
        px_x = int((x0 + 2) * scale)
        sections, last_color, start_y = [], None, top
        for py in range(int(top * scale), int(bottom * scale)):
            color = img.getpixel((px_x, py))
            if all(c < 50 for c in color[:3]):
                continue
            if last_color is None:
                last_color = color
                continue
            if color != last_color:
                sections.append({"y0": start_y, "y1": py / scale, "color": last_color})
                start_y, last_color = py / scale, color
        sections.append(
            {"y0": start_y, "y1": bottom, "color": last_color or (255, 255, 255)}
        )
        return sections

    def process_column_text(self, text, is_regs=False):
        symbols, mu_list = [], []

        # 1. Handle Empty Input
        if not text:
            return ([] if is_regs else ""), symbols, []

        # 2. Extract Management Units (MUs)
        # Finds patterns like '4-8', '4-15' but ignores '(5-15)' or 'M.U. 5-15'
        mu_pattern = r"(?<!\()(?<!M\.U\. )\b\d{1,2}-\d{1,2}\b"

        if not is_regs:
            found_mus = re.findall(mu_pattern, text)
            if found_mus:
                # Store unique MUs preserving order
                mu_list = list(dict.fromkeys(found_mus))
                for mu in mu_list:
                    text = text.replace(mu, "")

        # 3. Extract CW (Classified Waters)
        if re.search(r"\bCW\b", text):
            symbols.append("Classified")
            if not is_regs:
                text = re.sub(r"\bCW\b", "", text)

        # 4. Extract Tributaries Symbols
        trib_pattern = r"[\uf0dc\uf02a\*]"
        if (
            re.search(trib_pattern, text)
            or "Includes tributaries" in text
            or "Incl. Tribs" in text
        ):
            if "Incl. Tribs" not in symbols:
                symbols.append("Incl. Tribs")
            text = re.sub(
                trib_pattern, " [Includes Tributaries] " if is_regs else "", text
            )
            if not is_regs:
                text = text.replace("Includes tributaries", "").replace(
                    "Incl. Tribs", ""
                )

        # 5. Clean Text
        lines = text.split("\n")
        cleaned_lines = [re.sub(r"[ \t]+", " ", l).strip() for l in lines]
        cleaned_text = "\n".join(cleaned_lines).strip()

        # 6. Parse Regulations if needed
        if is_regs:
            if cleaned_text:
                return self.RegParser.parse_reg(cleaned_text), symbols, cleaned_text
            else:
                return [], symbols, cleaned_text

        return cleaned_text, symbols, mu_list


# --- 4. PRESENTATION ---


def smart_wrap(text, width):
    if not text:
        return []
    paragraphs = text.split("\n")
    wrapped_lines = []
    for para in paragraphs:
        if not para.strip():
            wrapped_lines.append("")
            continue
        wrapped_lines.extend(textwrap.wrap(para, width=width))
    return wrapped_lines


def print_pretty_table(page_result: PageResult):
    """Print a formatted table of extraction results."""
    if not page_result or not page_result.rows:
        return

    meta = page_result.metadata
    rows = page_result.rows

    # Print Page Metadata Header
    print("\n" + "#" * 60)
    print(f"  PAGE: {meta.page_number}  |  REGION: {meta.region or 'N/A'}")
    print("#" * 60)

    avail = shutil.get_terminal_size((80, 20)).columns - 15
    w_w, m_w, s_w = int(avail * 0.25), int(avail * 0.10), int(avail * 0.15)
    r_w = avail - w_w - m_w - s_w

    sep = f"{'-'*w_w}-+-{'-'*m_w}-+-{'-'*s_w}-+-{'-'*r_w}"
    print(
        f"{'WATER BODY':<{w_w}} | {'MU':<{m_w}} | {'SYMBOLS':<{s_w}} | {'REGULATIONS'}\n{'='*len(sep)}"
    )

    for row in rows:
        w_l = smart_wrap(row.water, width=w_w) or [""]

        mu_str = ", ".join(row.mu) if isinstance(row.mu, list) else str(row.mu)
        m_l = smart_wrap(mu_str, width=m_w) or [""]

        s_l = smart_wrap(", ".join(row.symbols), width=s_w) or [""]

        # Display raw regulations text
        r_l = smart_wrap(row.raw_regs, width=r_w) or [""]

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


def _process_single_page(args):
    """Helper function for parallel page processing."""
    pdf_path, page_num, output_dir, save_debug = args
    parser = FishingSynopsisParser(output_dir=output_dir)

    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[page_num - 1]
        result = parser.extract_rows(page, save_debug=save_debug, page_num=page_num)

    # Only return pages with actual data rows
    if result.rows:
        return result
    return None


class SynopsisExtractor:
    """
    High-level class that handles downloading and extracting the entire
    BC Freshwater Fishing Synopsis PDF, organizing results by region.
    """

    SYNOPSIS_URL = "https://www2.gov.bc.ca/assets/gov/sports-recreation-arts-and-culture/outdoor-recreation/fishing-and-hunting/freshwater-fishing/fishing_synopsis.pdf"

    def __init__(self, output_dir="output"):
        self.parser = FishingSynopsisParser(output_dir=output_dir)
        self.base_output_dir = output_dir
        # Use extract_synopsis subfolder for JSON files
        self.output_dir = os.path.join(output_dir, "extract_synopsis")
        # PDF stays in base output directory for shared access
        self.pdf_path = os.path.join(self.base_output_dir, "fishing_synopsis.pdf")

        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

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

            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(url, stream=True, headers=headers)
            response.raise_for_status()
            with open(filename, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Download complete: {filename}")
        except Exception as e:
            print(f"Error downloading PDF: {e}")
            exit()

    def extract_all_pages(self, save_debug=False):
        """
        Extract raw rows from all pages in the PDF.
        Returns a list of page results with metadata and raw row data.

        Structure:
        [
            {
                "metadata": {"page_number": 37, "region": "REGION 4 - Kootenay"},
                "rows": [
                    {"water": "...", "mu": [...], "raw_regs": "...", "symbols": [...], "page": 37, "image": "page_037_row_000.png"},
                    ...
                ]
            },
            ...
        ]
        """
        if not os.path.exists(self.pdf_path):
            print(f"PDF not found at {self.pdf_path}. Downloading...")
            self.download_pdf()

        print(f"Processing {os.path.basename(self.pdf_path)}...")

        # Get total page count
        with pdfplumber.open(self.pdf_path) as pdf:
            total_pages = len(pdf.pages)

        # Prepare arguments for parallel processing
        page_args = [
            (self.pdf_path, page_num, self.base_output_dir, save_debug)
            for page_num in range(1, total_pages + 1)
        ]

        all_pages = []
        completed_count = 0

        # Process pages in parallel
        with ProcessPoolExecutor() as executor:
            futures = {
                executor.submit(_process_single_page, args): args[1]
                for args in page_args
            }

            for future in as_completed(futures):
                page_num = futures[future]
                try:
                    result = future.result()
                    if result:
                        all_pages.append(result)

                    completed_count += 1
                    # Progress indicator
                    if completed_count % 20 == 0:
                        print(f"Progress: {completed_count}/{total_pages}")
                except Exception as exc:
                    print(f"  Error on page {page_num}: {exc}")
                    completed_count += 1

        # Sort pages by page number to maintain order
        all_pages.sort(key=lambda x: x.metadata.page_number)

        print(f"\nComplete: {len(all_pages)} pages with regulation tables")
        return ExtractionResults(pages=all_pages)

    def save_to_json(self, data, filename: str = "synopsis_raw_data.json") -> str:
        """Save the extracted data to a JSON file."""
        output_path = os.path.join(self.output_dir, filename)

        # Convert dataclasses to dicts for JSON serialization
        json_data = data.to_dict()

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        print(f"Saved data to {output_path}")
        return output_path


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("pdf_path", nargs="?", help="Path to PDF file to extract")
    parser.add_argument("output_path", nargs="?", help="Output JSON file path")
    parser.add_argument("--page", type=int, help="Single page to extract (for testing)")
    parser.add_argument(
        "--debug", action="store_true", help="Save debug visualizations"
    )
    args = parser.parse_args(argv)

    if args.pdf_path and args.output_path:
        # Extract raw data from specified PDF
        output_dir = os.path.dirname(args.output_path) or "output"
        extractor = SynopsisExtractor(output_dir=os.path.dirname(output_dir))

        # Copy PDF to expected location if it's different
        if not os.path.exists(extractor.pdf_path) or os.path.abspath(
            args.pdf_path
        ) != os.path.abspath(extractor.pdf_path):
            os.makedirs(extractor.base_output_dir, exist_ok=True)
            import shutil

            shutil.copy2(args.pdf_path, extractor.pdf_path)
            print(f"Copied PDF to {extractor.pdf_path}")

        raw_data = extractor.extract_all_pages(save_debug=args.debug)
        output_file = os.path.basename(args.output_path)
        extractor.save_to_json(raw_data, filename=output_file)
        print("\nRaw extraction complete.")

    elif args.page:
        # Single page extraction (for testing)
        p = FishingSynopsisParser()
        PDF_PATH = os.path.join("output", "fishing_synopsis.pdf")

        with pdfplumber.open(PDF_PATH) as pdf:
            page_result = p.extract_rows(
                pdf.pages[args.page - 1], save_debug=args.debug, page_num=args.page
            )
            print_pretty_table(page_result)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
