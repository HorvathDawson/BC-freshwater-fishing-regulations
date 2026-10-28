/**
 * T215 — draw the entry's passage onto the synopsis: one PDF HIGHLIGHT ANNOTATION per box on its page (the owner's pick), so
 * any viewer — the browser's, the phone's — shows the page with the row marked. The geometry is core's `highlightGeometry`
 * (extractor points -> PDF user space); this file only writes it with pdf-lib. Pure bytes in, bytes out.
 */
import { PDFArray, PDFDocument, PDFName, PDFNumber, PDFString } from "pdf-lib";
import { highlightGeometry, type SourceBox } from "@app/core";

/** Yellow, like a highlighter; pdfium and PDFKit both draw a /Highlight without an appearance stream (multiplied). */
const COLOUR = [1, 0.9, 0.2];
/** How far above the mark (PDF points) the document opens, so the line before it shows too. */
const OPEN_ABOVE = 72;

/** `pdf` with a highlight on every box that fits its page. Returns the input untouched when nothing could be drawn. */
export async function highlightPdf(pdf: Uint8Array, boxes: readonly SourceBox[],
                                   opts: { entryFirst?: boolean } = {}): Promise<Uint8Array> {
  const doc = await PDFDocument.load(pdf, { updateMetadata: false });
  const pages = doc.getPages();
  let drawn = 0;
  /** The first drawn box's page and the top of its highlight (PDF space): where the document opens. */
  let opensAt: { page: (typeof pages)[number]; top: number } | null = null;
  for (const b of boxes) {
    const page = pages[b.pdf - 1];
    const g = highlightGeometry(b);
    if (!page || !g) continue;
    const ctx = doc.context;
    const nums = (xs: readonly number[]) => ctx.obj(xs.map((x) => PDFNumber.of(Math.round(x * 100) / 100)));
    const annot = ctx.obj({
      Type: "Annot", Subtype: "Highlight", Rect: nums(g.rect), QuadPoints: nums(g.quadPoints),
      C: nums(COLOUR), CA: 1, F: 4, Border: nums([0, 0, 0]), T: PDFString.of("Source"),
    });
    const ref = ctx.register(annot);
    const existing = page.node.lookupMaybe(PDFName.of("Annots"), PDFArray);
    if (existing) existing.push(ref); else page.node.set(PDFName.of("Annots"), ctx.obj([ref]));
    opensAt ??= { page, top: g.rect[3] };
    drawn++;
  }
  /*
   * (T50) OPEN AT THE ENTRY. A phone's viewer is handed a file, not a URL, so there is no `#page=`: the document's own
   * /OpenAction names the highlighted page, a little above the mark, so a viewer that honours it lands on the passage
   * (the web still sends `#page=`, which wins there).
   */
  if (opensAt && !opts.entryFirst) {
    const top = Math.min(opensAt.page.getHeight(), opensAt.top + OPEN_ABOVE);
    doc.catalog.set(PDFName.of("OpenAction"),
                    doc.context.obj([opensAt.page.ref, PDFName.of("XYZ"), null, PDFNumber.of(Math.round(top)), null]));
  }
  /*
   * (T50) ENTRY FIRST, for a viewer that honours neither `#page=` nor /OpenAction (Android's, Drive's): the highlighted
   * page(s) are copied to the FRONT of the document, the whole book following unchanged — it opens on the passage and
   * the rest of the rule book is still one scroll away.
   */
  if (drawn && opts.entryFirst) {
    const at = [...new Set(boxes.map((b) => b.pdf - 1).filter((i) => i >= 0 && i < pages.length))].sort((a, b) => a - b);
    const copies = await doc.copyPages(doc, at);
    copies.forEach((c, k) => doc.insertPage(k, c));
  }
  return drawn ? doc.save() : pdf;
}
