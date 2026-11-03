// @vitest-environment node
// T354: the cited page first by an incremental update. The index is cut here from a pdf-lib book the way
// pipeline/deliver/release/synopsis.py cuts it from the shipped one (the real book is checked there: qpdf + pdfium).
import { describe, expect, it } from "vitest";
import { PDFArray, PDFDict, PDFDocument, PDFName, PDFNumber, PDFRef } from "pdf-lib";
import type { SynopsisAppend } from "@app/data";
import { APPEND_V, appendHighlight } from "./appendPdf";

const KEYS = ["MediaBox", "Resources", "Contents"];

async function book(pages = 3) {
  const d = await PDFDocument.create();
  for (let i = 0; i < pages; i++) d.addPage([576, 756]).drawText(`page ${i + 1}`);
  const bytes = await d.save({ useObjectStreams: false });
  const doc = await PDFDocument.load(bytes);
  const tail = new TextDecoder("latin1").decode(bytes.slice(-64));
  const ref = (r: PDFRef): [number, number] => [r.objectNumber, r.generationNumber];
  const pagesRef = doc.catalog.get(PDFName.of("Pages")) as PDFRef;
  const root = doc.context.lookup(pagesRef, PDFDict);
  const ix: SynopsisAppend = {
    v: APPEND_V, startxref: Number(/startxref\s+(\d+)/.exec(tail)![1]), size: doc.context.largestObjectNumber + 1,
    root: ref(doc.context.trailerInfo.Root as PDFRef), info: null, id: null, pages: ref(pagesRef),
    kids: root.get(PDFName.of("Kids"))!.toString(), count: (root.get(PDFName.of("Count")) as PDFNumber).asNumber(), box: "",
    page: doc.getPages().map((p) => KEYS.map((k) => `/${k} ${p.node.get(PDFName.of(k))!.toString()}`).join("")),
  };
  return { bytes, ix };
}
const joined = (a: Uint8Array, b: Uint8Array) => { const o = new Uint8Array(a.length + b.length); o.set(a); o.set(b, a.length); return o; };
const annots = (d: PDFDocument, i: number) => d.getPage(i).node.lookupMaybe(PDFName.of("Annots"), PDFArray)?.size() ?? 0;

describe("appendHighlight (T354)", () => {
  it("the cited page is copied to the front, highlighted; the book follows unchanged", async () => {
    const { bytes, ix } = await book();
    const tail = appendHighlight(ix, bytes.length, [{ pdf: 2, box: [36, 100, 540, 120], size: [576, 756] },
                                                    { pdf: 2, box: [36, 130, 540, 150], size: [576, 756] }])!;
    expect(tail.length).toBeLessThan(2048);
    const doc = await PDFDocument.load(joined(bytes, tail));
    expect(doc.getPageCount()).toBe(4);
    expect([0, 1, 2, 3].map((i) => annots(doc, i))).toEqual([2, 0, 0, 0]);
    // the front page shows the cited page's own content stream
    expect(doc.getPage(0).node.get(PDFName.of("Contents"))).toEqual(doc.getPage(2).node.get(PDFName.of("Contents")));
    const a = doc.getPage(0).node.lookup(PDFName.of("Annots"), PDFArray).lookup(0, PDFDict);
    expect(a.get(PDFName.of("Subtype"))?.toString()).toBe("/Highlight");
    const rect = a.lookup(PDFName.of("Rect"), PDFArray).asArray().map((n) => (n as PDFNumber).asNumber());
    expect(rect).toEqual([36, 636, 540, 656]);
  });
  it("the trailer points back at the book's xref and covers every new object", async () => {
    const { bytes, ix } = await book();
    const text = new TextDecoder("latin1").decode(appendHighlight(ix, bytes.length, [{ pdf: 3, box: [0, 0, 100, 10], size: [576, 756] }])!);
    expect(text).toContain(`/Prev ${ix.startxref}`);
    const xrefAt = Number(/startxref\n(\d+)\n%%EOF\n$/.exec(text)![1]);
    expect(new TextDecoder("latin1").decode(joined(bytes, new TextEncoder().encode(text)).slice(xrefAt, xrefAt + 12))).toMatch(/^\d+ 0 obj/);
    // two pages + an annot + the xref stream are new; the root /Pages is overridden in place
    expect(text).toMatch(new RegExp(`/Size ${ix.size + 3}/`));
    expect(text).toMatch(new RegExp(`/Index \\[${ix.pages[0]} 1 ${ix.size} 3\\]`));
  });
  it("a link with no box opens at the page it names, unmarked; page 1 or nothing: no append", async () => {
    const { bytes, ix } = await book();
    const doc = await PDFDocument.load(joined(bytes, appendHighlight(ix, bytes.length, [], 3)!));
    expect(doc.getPageCount()).toBe(4);
    expect(annots(doc, 0)).toBe(0);
    expect(appendHighlight(ix, bytes.length, [], 1)).toBeNull();
    expect(appendHighlight(ix, bytes.length, [{ pdf: 9, box: [0, 0, 1, 1], size: [576, 756] }])).toBeNull();
  });
  it("an index of another version, or bytes that are not the book it was cut from: no append", async () => {
    const { bytes, ix } = await book();
    const box = [{ pdf: 2, box: [0, 0, 100, 10], size: [576, 756] }] as const;
    expect(appendHighlight({ ...ix, v: 99 }, bytes.length, box)).toBeNull();
    expect(appendHighlight(ix, ix.startxref, box)).toBeNull();
  });
});
