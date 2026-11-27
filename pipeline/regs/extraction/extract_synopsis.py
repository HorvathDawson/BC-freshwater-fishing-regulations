import pdfplumber
import os
import argparse
import logging
import re
import textwrap
import shutil
import json
import sys
from typing import List, Optional, Dict, Any, Tuple
from operator import itemgetter
from PIL import Image, ImageDraw
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

try:
    import requests
except ImportError:
    requests = None  # type: ignore[assignment]

# Import shared data models
from .models import (
    WaterbodyRow,
    PageMetadata,
    PageResult,
    ExtractionResults,
)
from .visible import PageNotJudged, PageVerdict, page_verdict
from pipeline.common.curated import SOURCE

logger = logging.getLogger(__name__)



def _normalize_unicode(text: str) -> str:
    """Replace common Unicode characters with their ASCII equivalents.

    Standardises non-breaking spaces, dashes, and curly quotes so that
    downstream processing only needs to handle plain ASCII punctuation.
    """
    text = text.replace("\xa0", " ")       # Non-breaking space → regular space
    text = text.replace("\u2013", "-")     # En-dash → hyphen
    text = text.replace("\u2014", "-")     # Em-dash → hyphen
    text = text.replace("\u2018", "'").replace("\u2019", "'")   # Curly single quotes
    text = text.replace("\u201c", '"').replace("\u201d", '"')   # Curly double quotes
    return text


# The page number PRINTED in the footer (the number a row's `page` carries) is read by the one
# reader in `pipeline.common.synopsis_pages`, which also derives the printed -> PDF page map.
from pipeline.common.synopsis_pages import _FOOTER_PAGE, printed_page_number  # noqa: E402,F401


def _stacked_under(char: Dict[str, Any], line: List[Dict[str, Any]]) -> bool:
    """True when `char` sits under a glyph already on `line`: more than half its own size lower AND
    overlapping it horizontally. Two glyphs stacked like that are on two lines, however close.

    The 5 pt line rule alone missed this: a cell's last line set in tight leading (Heber River,
    PDF p.18, "...confluence when" / "open" 5.0 pt lower, not > 5) joined the line above, and the x
    sort interleaved it — "Folpye fnishing only ... when". Glyphs of one line do not overlap at
    different heights (faux-bold doubles share their top). Every garble back to 2026-09-03 had this
    cause; the 09-24 file's clean Heber text was a hand fix. Verified on PDF pp.17-19, 21, 24, 26, 60:
    only Heber changes (T415, in T239)."""
    half = char.get("size", 0) * 0.5
    if half <= 0 or char.get("top", 0) - min(c.get("top", 0) for c in line) <= half:
        return False
    return any(o.get("x0", 0) < char.get("x1", 0) and char.get("x0", 0) < o.get("x1", 0)
               and abs(o.get("top", 0) - char.get("top", 0)) > half for o in line)


class FishingSynopsisParser:
    def __init__(self, output_dir: str = "output", debug_dir: str = "debug", audit_dir: str = "debug") -> None:
        # Use extraction subfolder for all output
        self.base_output_dir = os.path.join(output_dir, "extraction")
        self.output_dir = self.base_output_dir
        self.debug_dir = os.path.join(self.base_output_dir, debug_dir)
        self.audit_dir = os.path.join(self.base_output_dir, audit_dir)

        # Ensure base directory exists
        os.makedirs(self.base_output_dir, exist_ok=True)

        # Symbol Rejection Thresholds
        self.MIN_SYMBOL_DIM = 4.0
        self.MAX_SYMBOL_DIM = 25.0
        self.MAP_REJECTION_DIM = 35.0

        # Tributaries Text Constant
        self.INCLUDES_TRIBUTARIES = "[Includes Tributaries]"

    def get_cleaned_page(self, page) -> Tuple[Any, PageVerdict]:
        """The page with only the characters the PDF's own painting rules make visible, and the verdict.

        T1190: `pipeline.regs.extraction.visible` decides (painter's model on MuPDF's display list: covered by a
        later opaque fill or image, the same glyph drawn again, render mode, opacity, layer, crop box, clip, same
        colour as the fill under it). It replaced a 400 dpi render, a two-colour background palette, an ink count
        per character, DBSCAN over (line, paint order) and a first-line overlap cut, every number of them tuned to
        this book's colours. Each char keeps `render_index` (its place in content order) and gains `struck` (a
        strike-through line is drawn through it). Only chars are filtered; lines, rects and curves are untouched."""
        verdict = page_verdict(page)
        for i, c in enumerate(page.chars):
            c["render_index"] = i
            c["struck"] = i in verdict.struck
        keep = verdict.visible
        clean = page.filter(
            lambda o: o.get("render_index") in keep if o.get("object_type") == "char" else True
        )
        return clean, verdict

    # --- 2. SYMBOL & DEBUG LOGIC ---

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

    def extract_text_with_bold(self, page_section, mark_struck: bool = False) -> str:
        """
        Extract text from a page section with bold formatting preserved using Markdown syntax.

        Args:
            page_section: A pdfplumber page or filtered page section
            mark_struck: wrap each run of struck-through characters (`char["struck"]`, T1190) in ~~

        Returns:
            Text string with bold portions wrapped in ** (and, with mark_struck, struck runs in ~~)
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
            elif (abs(char_top - current_line_top) > line_height_threshold
                  or _stacked_under(char, current_line)):
                # Process the completed line - IMPORTANT: sort by x0 before processing
                if current_line:
                    current_line.sort(key=lambda c: c.get("x0", 0))
                    result_lines.append(self._process_line_with_bold(current_line, mark_struck))
                current_line = []
                current_line_top = char_top

            current_line.append(char)

        # Process the last line - IMPORTANT: sort by x0 before processing
        if current_line:
            current_line.sort(key=lambda c: c.get("x0", 0))
            result_lines.append(self._process_line_with_bold(current_line, mark_struck))

        return "\n".join(result_lines)

    @staticmethod
    def _close_strike(segment: List[str]) -> None:
        """Close a ~~ run before the spaces that end it (`~~15~~ x`, never `~~15 ~~x`)."""
        tail = []
        while segment and not segment[-1].strip():
            tail.append(segment.pop())
        segment.append("~~")
        segment.extend(reversed(tail))

    def _process_line_with_bold(self, chars: List[Dict[str, Any]], mark_struck: bool = False) -> str:
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
        in_strike = False           # (T1190, mark_struck) a ~~ run is open in current_segment

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

            # (T1190) a struck run closes before the space that follows it; a space never flips it
            struck = (bool(char.get("struck")) if text.strip() else in_strike) if mark_struck else False
            if in_strike and not struck:
                self._close_strike(current_segment)
                in_strike = False

            # If bold status changed, flush current segment
            if current_is_bold is not None and is_bold != current_is_bold:
                if in_strike:
                    self._close_strike(current_segment)
                    in_strike = False
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

            if struck and not in_strike:
                current_segment.append("~~")
                in_strike = True
            current_segment.append(text)
            current_segment_chars.append(char)
            current_is_bold = is_bold

        # Flush the last segment
        if in_strike:
            self._close_strike(current_segment)
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
        # Create row_images directory in extraction subfolder
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

        # 1. Keep only the visible characters (T1190: the PDF's painting rules, `visible.py`)
        page, verdict = self.get_cleaned_page(raw_page)
        result = self._rows_of_page(page, raw_page, page_num, save_debug)
        if verdict.disagreements:
            # pdfplumber placed a glyph elsewhere than MuPDF (and both renderers) draw it: its verdict is sure, its
            # place in the layout is not. Refused where rows are read; said out loud where none are.
            what = "; ".join(f"{t!r} pdfplumber {a} vs MuPDF {b}" for _, t, a, b in verdict.disagreements[:5])
            n = len(verdict.disagreements)
            if result.rows:
                raise PageNotJudged(page_num, f"pdfplumber and MuPDF place {n} glyph(s) differently ({what}) on a "
                                              f"page with {len(result.rows)} row(s)")
            print(f"  [visible] PDF page {page_num}: pdfplumber and MuPDF place {n} glyph(s) differently ({what}); "
                  f"no rows are read from this page", file=sys.stderr)
        return result

    def _rows_of_page(self, page, raw_page, page_num, save_debug) -> PageResult:
        # 2. Extract Metadata (Region Name)
        region_header = self._extract_region_header(page)

        metadata = PageMetadata(page_number=page_num, region=region_header)
        # The number printed on the page, from the RAW page: cleaning may drop the footer's ink.
        printed = printed_page_number(raw_page)

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
            if any(c.get("struck") for c in left.chars):
                raise ValueError(f"PDF page {page_num}: a struck-through character in a water-name cell "
                                 f"({water_raw!r}): T1190 carries strikes in the regulation cell only")
            regs_struck = (self.extract_text_with_bold(right, mark_struck=True)
                           if any(c.get("struck") for c in right.chars) else None)

            # Debug output: show raw column data before processing
            if save_debug and (water_raw or regs_raw):
                print(f"\n{'='*80}")
                print(f"Row at y={y0:.1f}-{y1:.1f}")
                print(f"{'='*80}")
                print(f"WATER COLUMN (raw):\n{repr(water_raw)}")
                print(f"\nREGS COLUMN (raw):\n{repr(regs_raw)}")
                print(f"{'='*80}\n")

            cell = self.row_from_cells(water_raw, regs_raw, v_sym_raw)
            if cell is not None:
                w_txt, mus, regs_raw, all_syms = (cell["water"], cell["mu"], cell["raw_regs"],
                                                   cell["symbols"])
                marked = self._marked_regs(page_num, water_raw, regs_struck, v_sym_raw, regs_raw)
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
                        page=self._printed_or_refuse(printed, page_num),
                        image=image_key,
                        region=region_header,
                        pdf_page=page_num,
                        raw_regs_marked=marked,
                    )
                )

        result = PageResult(metadata=metadata, rows=structured_data)
        if structured_data:
            print(
                f"Page {page_num}: {len(structured_data)} rows ({region_header or 'Unknown'})"
            )
        return result

    def row_from_cells(self, water_raw: str, regs_raw: str, v_sym_raw: List[str]) -> Optional[Dict[str, Any]]:
        """One table row's fields from its two cells' text (`extract_text_with_bold` of each) and its
        visual symbols: the name column's name / MUs / symbols and the regulation column's cleaned
        text. None for the header row or a row with no name and no MU (not a regulation row).

        Split out of `extract_rows` (T426) so the fast tier replays a row from its characters with no
        PDF (`pipeline/tests/test_extract_fixtures.py`); `extract_rows` is unchanged in what it reads."""
        # Process waterbody column (name, symbols, MUs)
        w_txt, w_sym, mus = self.process_waterbody_column(water_raw)

        if "WATER BODY" in w_txt.upper() or "MGMT UNIT" in w_txt.upper():
            return None

        # First-seen order, never `set` order: a set made the order follow the hash seed, so two
        # extractions of one PDF differed on 30 rows' symbols (T239 dry run).
        all_syms = list(dict.fromkeys(v_sym_raw + w_sym))

        # Normalize unicode characters to preserve context while standardizing format
        regs_raw = _normalize_unicode(regs_raw)

        # Replace tributaries symbols with standardized placeholder text
        trib_pattern = r"[\uf0dc\uf02a]"
        regs_raw = re.sub(trib_pattern, self.INCLUDES_TRIBUTARIES, regs_raw)
        regs_raw = regs_raw.replace(
            "Includes tributaries", self.INCLUDES_TRIBUTARIES
        )
        regs_raw = regs_raw.replace("Incl. Tribs", self.INCLUDES_TRIBUTARIES)

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

        if not ((w_txt.strip() or mus) and (w_txt or regs_raw.strip() or mus or all_syms)):
            return None
        return {"water": w_txt, "mu": mus, "raw_regs": regs_raw.strip(), "symbols": all_syms}

    def _marked_regs(self, page_num: int, water_raw: str, regs_struck: Optional[str], v_sym_raw: List[str],
                     raw_regs: str) -> Optional[str]:
        """(T1190) The regulation cell with each struck-through run in ~~ (`Sept 1-Oct ~~15~~`), cleaned exactly as
        `raw_regs` is; None for a row with no strike. `raw_regs` itself never carries the mark: it is the text the
        parse batches, the entry ids' rows digest, the carry step and ingest all key on, and a strike leaves it as the
        book's text layer has it. Refuses a marked text that is not `raw_regs` plus the marks alone."""
        if regs_struck is None:
            return None
        cell = self.row_from_cells(water_raw, regs_struck, v_sym_raw)
        marked = cell["raw_regs"] if cell is not None else None
        if "~~" in raw_regs or marked is None or marked.replace("~~", "") != raw_regs:
            raise ValueError(f"PDF page {page_num}: the struck-through text of {water_raw!r} does not clean to its "
                             f"raw_regs plus ~~ marks ({marked!r} vs {raw_regs!r})")
        return marked

    @staticmethod
    def _printed_or_refuse(printed: Optional[int], page_num: int) -> int:
        """A row's page is the PRINTED number, and a table page without one is refused rather
        than given its PDF index: that fallback is exactly the wrong number this replaced."""
        if printed is None:
            raise ValueError(f"PDF page {page_num} holds regulation rows but no printed page "
                             f"number could be read from its footer")
        return printed

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
        text = _normalize_unicode(text)

        # Remove bold markers before processing (they interfere with MU extraction)
        text = text.replace("**", "")

        # Extract Tributaries Symbols BEFORE normalizing whitespace
        # (we need to detect newlines to identify start-of-line symbols)
        # But NOT if they appear within parentheses (those refer to other waterbodies)
        trib_pattern = r"[\uf0dc\uf02a\*]"

        # First, convert symbols within any parentheses to standard text
        parens_pattern = r"\(([^)]*)(" + trib_pattern + r")([^)]*)\)"
        text = re.sub(parens_pattern, r"(\1" + self.INCLUDES_TRIBUTARIES + r"\3)", text)

        # Check if symbol appears at start of line (after newline or at very start)
        # This indicates it applies to the whole waterbody
        start_of_line_pattern = r"(?:^|\n)\s*" + trib_pattern
        start_of_line_trib = re.search(start_of_line_pattern, text)
        if start_of_line_trib:
            # Convert to standard text and add to symbols
            if "Incl. Tribs" not in symbols:
                symbols.append("Incl. Tribs")
            text = re.sub(
                r"(?:^|\n)\s*" + trib_pattern + r"\s*",
                " " + self.INCLUDES_TRIBUTARIES + " ",
                text,
            )

        # Now check if there are any remaining symbols (outside parentheses and not at line start)
        if re.search(trib_pattern, text):
            if "Incl. Tribs" not in symbols:
                symbols.append("Incl. Tribs")
            text = re.sub(trib_pattern, "", text)

        # Normalize whitespace AFTER extracting tributaries symbols
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
                trib_pattern,
                " " + self.INCLUDES_TRIBUTARIES + " " if is_regs else "",
                text,
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

    return keep_page(result, page_num)


def keep_page(result: PageResult, page_num: int) -> Optional[PageResult]:
    """The page as the extraction keeps it, or None. One rule, read by the extraction and by the
    change gate (`baseline.page_rows`, T426), so the two never keep different pages."""
    # Only return pages with actual data rows
    if not result.rows:
        return None
    # A table on a page with no REGION header is the "how to read the tables" sample (printed p.4
    # of the 2025-27 book: ALICE LAKE / ALOUETTE LAKE), not a regulation. Dropped LOUDLY: a new
    # book whose header the reader no longer finds would lose every page here, and must say so.
    if not result.metadata.region:
        print(f"  [Skipped] PDF page {page_num}: {len(result.rows)} row(s) on a page with no REGION "
              f"header (the sample table) — if this is a real table, fix _extract_region_header",
              file=sys.stderr)
        return None
    return result


#: (T1190) the struck-through items already seen and recorded; a strike not on this list is NEW (owner 2026-11-26)
KNOWN_STRIKES = Path(__file__).resolve().parent / "known_strikes.json"
_STRUCK_RUN = re.compile(r"(?:[^\s~-]+ )?~~(.+?)~~")


def strike_items(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Every struck run of the rows (dicts), with the word before it: {water, pdf_page, page, text: 'Oct ~~15~~'}."""
    out = []
    for r in rows:
        for m in _STRUCK_RUN.finditer(r.get("raw_regs_marked") or ""):
            out.append({"water": r["water"], "pdf_page": r.get("pdf_page"), "page": r.get("page"), "text": m.group(0)})
    return out


def load_known_strikes(path: Path = KNOWN_STRIKES) -> List[Dict[str, Any]]:
    """The recorded strikes. A missing or malformed file raises naming it (a missing list is never 'none known')."""
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(doc.get("strikes"), list) or not all({"water", "text"} <= set(s) for s in doc["strikes"]):
        raise ValueError(f"{path}: 'strikes' must be a list of {{water, text, ...}}")
    return doc["strikes"]


def strike_report(rows: List[Dict[str, Any]], known: List[Dict[str, Any]]) -> Tuple[Optional[str], List[Dict]]:
    """ONE warning line for a run (owner 2026-11-26: keep struck text marked, no per-row flag, one line listing them),
    and the strikes that are NEW: not in the recorded list (matched on water + struck text, any page or book)."""
    items = strike_items(rows)
    if not items:
        return None, []
    seen = {(k["water"], k["text"]) for k in known}
    new = [i for i in items if (i["water"], i["text"]) not in seen]
    parts = [f"{i['water']} PDF p.{i['pdf_page']} {i['text']!r}" + (" NEW" if i in new else "") for i in items]
    line = (f"[struck] {len(items)} struck-through item(s) kept, marked in raw_regs_marked"
            + (f", {len(new)} NEW (not in {KNOWN_STRIKES.name})" if new else "") + ": " + "; ".join(parts))
    return line, new


class SynopsisExtractor:
    """
    High-level class that handles downloading and extracting the entire
    BC Freshwater Fishing Synopsis PDF, organizing results by region.
    """

    SYNOPSIS_URL = "https://www2.gov.bc.ca/assets/gov/sports-recreation-arts-and-culture/outdoor-recreation/fishing-and-hunting/freshwater-fishing/fishing_synopsis.pdf"

    def __init__(self, output_dir="output"):
        self.parser = FishingSynopsisParser(output_dir=output_dir)
        self.base_output_dir = output_dir
        # JSON output lives in the canonical ``extraction`` subfolder — the
        # single source of truth referenced by config.synopsis_raw_data_path,
        # the enrichment builder, and the debug-dir logic in main().
        self.output_dir = os.path.join(output_dir, "extraction")
        # PDF stays in base output directory for shared access
        self.pdf_path = os.path.join(self.base_output_dir, "fishing_synopsis.pdf")

        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

    def download_pdf(self, url: Optional[str] = None, filename: Optional[str] = None) -> None:
        """Download the PDF if it doesn't already exist."""
        if url is None:
            url = self.SYNOPSIS_URL
        if filename is None:
            filename = self.pdf_path

        if os.path.exists(filename):
            print(f"PDF already exists at {filename}")
            return

        if requests is None:
            raise ImportError("The 'requests' package is required to download PDFs. Install with: pip install requests")

        print(f"Downloading PDF from {url}...")
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, stream=True, headers=headers)
        response.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Download complete: {filename}")

    def extract_all_pages(self, save_debug: bool = False, workers: Optional[int] = 4) -> ExtractionResults:
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
        failed: Dict[int, str] = {}

        # Process pages in parallel
        # At most 4 processes by default: heavy jobs here run <= 4 workers (scripts/heavy.sh).
        with ProcessPoolExecutor(max_workers=workers) as executor:
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
                    # (T1190) every page is still tried, then the run FAILS naming each page it could not read
                    # (a page the visible-text method cannot judge included): never a book with pages missing
                    logger.warning("Error on page %d: %s", page_num, exc)
                    failed[page_num] = f"{type(exc).__name__}: {exc}"
                    completed_count += 1

        if failed:
            raise RuntimeError(f"{len(failed)} PDF page(s) could not be extracted, no rows file written: "
                               + "; ".join(f"p.{n}: {why}" for n, why in sorted(failed.items())))

        # Sort pages by page number to maintain order
        all_pages.sort(key=lambda x: x.metadata.page_number)
        line, _new = strike_report([r.to_dict() for p in all_pages for r in p.rows], load_known_strikes())
        if line:
            print(line, file=sys.stderr)

        print(f"\nComplete: {len(all_pages)} pages with regulation tables")
        return ExtractionResults(pages=all_pages)

    def save_to_json(self, data: ExtractionResults, filename: str = "synopsis_raw_data.json") -> str:
        """Save the extracted data to a JSON file."""
        output_path = os.path.join(self.output_dir, filename)

        # Convert dataclasses to dicts for JSON serialization
        json_data = data.to_dict()

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        print(f"Saved data to {output_path}")
        return output_path


def main(argv: Optional[List[str]] = None) -> None:
    default_pdf = str(SOURCE.synopsis_pdf)
    # `synopsis_raw_data_path` was retired with the `output:` tree and raises on access, so this
    # entry point could not start at all. The generated path comes from GENERATED (AGENTS 41).
    from pipeline.common.curated import GENERATED
    default_output = str(GENERATED.regs.extraction / "synopsis_raw_data.json")

    parser = argparse.ArgumentParser(
        description="Extract fishing regulations from BC synopsis PDF",
        epilog="If no arguments provided, uses paths from config.yaml",
    )
    parser.add_argument(
        "pdf_path",
        nargs="?",
        default=default_pdf,
        help=f"Path to PDF file to extract (default: {default_pdf})",
    )
    parser.add_argument(
        "output_path",
        nargs="?",
        default=default_output,
        help=f"Output JSON file path (default: {default_output})",
    )
    parser.add_argument("--page", type=int, help="Single page to extract (for testing)")
    parser.add_argument("--workers", type=int, default=4, help="page worker processes (default 4)")
    parser.add_argument(
        "--debug", action="store_true", help="Save debug visualizations"
    )
    # T426: the extractor change gate. Re-reads the CURRENT book (pdf_path) with this code and compares every row
    # with the committed baseline; exits 1 naming each row neither identical nor in the accepted diffs. Writes no
    # output file and no row images. A whole book is a heavy job: run it through scripts/heavy.sh.
    parser.add_argument("--check-against", type=Path, metavar="MANIFEST",
                        help="compare with a baseline manifest (pipeline/regs/extraction/baseline/<book>.json)")
    parser.add_argument("--accepted", type=Path, default=None,
                        help="the accepted diffs (default: accepted_diffs.json beside the manifest)")
    parser.add_argument("--pages", type=int, nargs="+", help="with --check-against: these PDF pages only")
    parser.add_argument("--report", type=Path, help="with --check-against: write the diffs as JSON")
    args = parser.parse_args(argv)

    if args.check_against:
        from .baseline import book_pdf, check
        accepted = args.accepted or args.check_against.parent / "accepted_diffs.json"
        # No PDF given: the baseline's own book, found by sha (after runbook step 1 the default is the NEW book).
        pdf = (book_pdf(json.loads(args.check_against.read_text(encoding="utf-8")))
               if args.pdf_path == default_pdf else Path(args.pdf_path))
        sys.exit(check(args.check_against, accepted, pdf, pages=args.pages,
                       workers=args.workers, report=args.report))

    # Print configuration
    print("=" * 80)
    print("BC FRESHWATER FISHING REGULATIONS - SYNOPSIS EXTRACTION")
    print("=" * 80)

    if args.pdf_path and args.output_path:
        print("\n📝 Input:")
        print(f"  PDF: {args.pdf_path}")
        print("\n💾 Output:")
        print(f"  Raw data JSON: {args.output_path}")
        if args.debug:
            output_dir = os.path.dirname(args.output_path) or "output"
            debug_dir = os.path.join(
                os.path.dirname(output_dir), "extraction", "debug"
            )
            print(f"  Debug visualizations: {debug_dir}")
        print()

    if args.pdf_path and args.output_path:
        # Extract raw data from specified PDF
        output_dir = os.path.dirname(args.output_path) or "output"
        extractor = SynopsisExtractor(output_dir=os.path.dirname(output_dir))

        # Copy PDF to expected location if it's different
        if not os.path.exists(extractor.pdf_path) or os.path.abspath(
            args.pdf_path
        ) != os.path.abspath(extractor.pdf_path):
            os.makedirs(extractor.base_output_dir, exist_ok=True)
            shutil.copy2(args.pdf_path, extractor.pdf_path)
            print(f"Copied PDF to {extractor.pdf_path}")

        raw_data = extractor.extract_all_pages(save_debug=args.debug, workers=args.workers)
        output_file = os.path.basename(args.output_path)
        extractor.save_to_json(raw_data, filename=output_file)
        print("\nRaw extraction complete.")

    elif args.page:
        # Single page extraction (for testing)
        p = FishingSynopsisParser()
        PDF_PATH = SOURCE.synopsis_pdf      # (T1021) was os.path.join("data", "source", …): the cwd's

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
