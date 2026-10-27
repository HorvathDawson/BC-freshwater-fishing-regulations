/**
 * T215 — the highlight an opened synopsis PDF carries. A source row's box is what the extractor records
 * (`row_boxes.json`: pdfplumber points, ORIGIN TOP-LEFT, y down, with the page size); a PDF highlight annotation wants
 * `/Rect` and `/QuadPoints` in PDF user space (ORIGIN BOTTOM-LEFT, y up). This file is that one conversion, pure, so the
 * web (pdf-lib in the page) and native (pdf-lib to a cache file) openers share it. Nothing here reads a PDF.
 */

/** A source passage on one PDF page: `box` [x0, top, x1, bottom] and `size` [width, height], both in points, top-left origin. */
export interface SourceBox { readonly pdf: number; readonly box: readonly [number, number, number, number]; readonly size: readonly [number, number] }

/** The /Highlight annotation's geometry in PDF user space. */
export interface HighlightGeometry {
  readonly rect: readonly [number, number, number, number];                       // [llx, lly, urx, ury]
  readonly quadPoints: readonly [number, number, number, number, number, number, number, number]; // TL TR BL BR (Acrobat order)
}

/** Flip a top-left box into PDF space, clamped to the page; null for an empty or off-page box (open the page unhighlighted). */
export function highlightGeometry(s: SourceBox): HighlightGeometry | null {
  const [w, h] = s.size;
  const x0 = Math.max(0, Math.min(s.box[0], s.box[2])), x1 = Math.min(w, Math.max(s.box[0], s.box[2]));
  const t = Math.max(0, Math.min(s.box[1], s.box[3])), b = Math.min(h, Math.max(s.box[1], s.box[3]));
  if (!(w > 0 && h > 0) || x1 - x0 <= 0 || b - t <= 0) return null;
  const top = h - t, bot = h - b;
  return { rect: [x0, bot, x1, top], quadPoints: [x0, top, x1, top, x0, bot, x1, bot] };
}
