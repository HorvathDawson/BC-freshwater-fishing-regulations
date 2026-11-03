// @vitest-environment node
// T354: the row highlighted in place by an incremental update. The book here is made as pipeline/deliver/release/
// synopsis.py ships the real one (every page's /Annots an indirect array) and its index cut the same way; the real book
// is checked there (qpdf + pdfium).
import { describe, expect, it } from "vitest";
import { PDFArray, PDFDict, PDFDocument, PDFName, PDFNumber, PDFRef } from "pdf-lib";
import type { SynopsisAppend } from "@app/data";
import { APPEND_V, appendHighlight } from "./appendPdf";

async function book(pages = 3) {
  const d = await PDFDocument.create();
  for (let i = 0; i < pages; i++) {
    const p = d.addPage([576, 756]);
    p.drawText(`page ${i + 1}`);
    p.node.set(PDFName.of("Annots"), d.context.register(d.context.obj([])));
  }
  const bytes = await d.save({ useObjectStreams: false });
  const doc = await PDFDocument.load(bytes);
  const tail = new TextDecoder("latin1").decode(bytes.slice(-64));
  const ref = (r: PDFRef): [number, number] => [r.objectNumber, r.generationNumber];
  const ix: SynopsisAppend = {
    v: APPEND_V, startxref: Number(/startxref\s+(\d+)/.exec(tail)![1]), size: doc.context.largestObjectNumber + 1,
    root: ref(doc.context.trailerInfo.Root as PDFRef), info: null, id: null,
    page: doc.getPages().map((p) => [...ref(p.ref), ...ref(p.node.get(PDFName.of("Annots")) as PDFRef), ""]),
  };
  return { bytes, ix };
}
const joined = (a: Uint8Array, b: Uint8Array) => { const o = new Uint8Array(a.length + b.length); o.set(a); o.set(b, a.length); return o; };
const annots = (d: PDFDocument, i: number) => d.getPage(i).node.lookup(PDFName.of("Annots"), PDFArray).size();
const BOX = (pdf: number, top = 100) => ({ pdf, box: [36, top, 540, top + 20] as [number, number, number, number], size: [576, 756] as [number, number] });

describe("appendHighlight (T354)", () => {
  it("highlights the box's page in place; no page moves and no other page is touched", async () => {
    const { bytes, ix } = await book();
    const tail = appendHighlight(ix, bytes.length, [BOX(2), BOX(2, 130)])!;
    expect(tail.length).toBeLessThan(2048);
    const doc = await PDFDocument.load(joined(bytes, tail));
    expect(doc.getPageCount()).toBe(3);
    expect([0, 1, 2].map((i) => annots(doc, i))).toEqual([0, 2, 0]);
    const a = doc.getPage(1).node.lookup(PDFName.of("Annots"), PDFArray).lookup(0, PDFDict);
    expect(a.get(PDFName.of("Subtype"))?.toString()).toBe("/Highlight");
    expect(a.get(PDFName.of("P"))).toEqual(doc.getPage(1).ref);
    const rect = a.lookup(PDFName.of("Rect"), PDFArray).asArray().map((n) => (n as PDFNumber).asNumber());
    expect(rect).toEqual([36, 636, 540, 656]);
    // its own appearance: Android's pdfium draws no highlight without one
    const ap = a.lookup(PDFName.of("AP"), PDFDict).lookup(PDFName.of("N"));
    expect(ap?.constructor.name).toMatch(/Stream/);
  });
  it("a page's own annotations (its links) stay, the highlight after them", async () => {
    const { bytes, ix } = await book();
    const withLink = { ...ix, page: ix.page.map((p, i) => (i === 0 ? [p[0], p[1], p[2], p[3], "7 0 R"] : p)) as SynopsisAppend["page"] };
    const text = new TextDecoder("latin1").decode(appendHighlight(withLink, bytes.length, [BOX(1)])!);
    expect(text).toContain(`${ix.page[0]![2]} 0 obj\n[7 0 R ${ix.size} 0 R]`);
  });
  it("the trailer points back at the book's xref and covers every new object", async () => {
    const { bytes, ix } = await book();
    const text = new TextDecoder("latin1").decode(appendHighlight(ix, bytes.length, [BOX(3)])!);
    expect(text).toContain(`/Prev ${ix.startxref}`);
    const xrefAt = Number(/startxref\n(\d+)\n%%EOF\n$/.exec(text)![1]);
    expect(new TextDecoder("latin1").decode(joined(bytes, new TextEncoder().encode(text)).slice(xrefAt, xrefAt + 12))).toMatch(/^\d+ 0 obj/);
    // the annot, its appearance + the xref stream are new; page 3's /Annots array is overridden in place
    expect(text).toMatch(new RegExp(`/Size ${ix.size + 3}/`));
    const runs = /\/Index \[([\d ]+)\]/.exec(text)![1]!.split(" ").map(Number);
    const covered = runs.flatMap((v, i) => (i % 2 ? [] : Array.from({ length: runs[i + 1]! }, (_, k) => v + k)));
    expect(covered.sort((a, b) => a - b)).toEqual([ix.page[2]![2], ix.size, ix.size + 1, ix.size + 2].sort((a, b) => a - b));
  });
  it("nothing to mark (no box, a box off every page): null", async () => {
    const { bytes, ix } = await book();
    expect(appendHighlight(ix, bytes.length, [])).toBeNull();
    expect(appendHighlight(ix, bytes.length, [BOX(9)])).toBeNull();
  });
  it("another index version, or bytes it was not cut from: thrown, never the book quietly unmarked", async () => {
    const { bytes, ix } = await book();
    expect(() => appendHighlight({ ...ix, v: 99 }, bytes.length, [BOX(2)])).toThrow(/v99/);
    expect(() => appendHighlight(ix, ix.startxref, [BOX(2)])).toThrow(/xref/);
  });
});
