/**
 * T215 — draw the entry's passage onto the synopsis: one PDF HIGHLIGHT ANNOTATION per box on its page (the owner's pick), so
 * any viewer — the browser's, the phone's — shows the page with the row marked. The geometry is core's `highlightGeometry`
 * (extractor points -> PDF user space); this file only writes it with pdf-lib. Pure bytes in, bytes out.
 */
import { PDFArray, PDFDocument, PDFName, PDFNumber, PDFString } from "pdf-lib";
import { highlightGeometry, type SourceBox } from "@app/core";

/** Yellow, like a highlighter; pdfium and PDFKit both draw a /Highlight without an appearance stream (multiplied). */
const COLOUR = [1, 0.9, 0.2];

/** `pdf` with a highlight on every box that fits its page. Returns the input untouched when nothing could be drawn. */
export async function highlightPdf(pdf: Uint8Array, boxes: readonly SourceBox[]): Promise<Uint8Array> {
  const doc = await PDFDocument.load(pdf, { updateMetadata: false });
  const pages = doc.getPages();
  let drawn = 0;
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
    drawn++;
  }
  return drawn ? doc.save() : pdf;
}
