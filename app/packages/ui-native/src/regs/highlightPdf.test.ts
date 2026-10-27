// @vitest-environment node
import { describe, expect, it, vi } from "vitest";
import { PDFArray, PDFDict, PDFDocument, PDFName, PDFNumber } from "pdf-lib";
import { highlightPdf } from "./highlightPdf";
import { openBook } from "./openBook";
import { setSynopsisSource } from "./synopsisSource";
import { Linking } from "react-native";

async function twoPages() {
  const d = await PDFDocument.create();
  d.addPage([576, 756]); d.addPage([576, 756]);
  return d.save();
}

describe("highlightPdf (T215)", () => {
  it("adds one /Highlight on the box's page, in PDF space, and none elsewhere", async () => {
    const out = await highlightPdf(await twoPages(), [{ pdf: 2, box: [36, 100, 540, 120], size: [576, 756] }]);
    const doc = await PDFDocument.load(out);
    expect(doc.getPage(0).node.lookupMaybe(PDFName.of("Annots"), PDFArray)).toBeUndefined();
    const annots = doc.getPage(1).node.lookup(PDFName.of("Annots"), PDFArray);
    expect(annots.size()).toBe(1);
    const a = annots.lookup(0, PDFDict);
    expect(a.get(PDFName.of("Subtype"))?.toString()).toBe("/Highlight");
    const rect = (a.lookup(PDFName.of("Rect"), PDFArray)).asArray().map((n) => (n as PDFNumber).asNumber());
    expect(rect).toEqual([36, 636, 540, 656]);
  });
  it("a box off every page leaves the bytes as they were", async () => {
    const pdf = await twoPages();
    expect(await highlightPdf(pdf, [{ pdf: 9, box: [1, 1, 5, 5], size: [576, 756] }])).toBe(pdf);
  });
});

describe("openBook with a box (T215)", () => {
  const boxes = [{ pdf: 23, box: [1, 2, 3, 4] as [number, number, number, number], size: [576, 756] as [number, number] }];
  it("opens our highlighted copy when the release names one and the link has a box", async () => {
    setSynopsisSource({ url: "http://x/s.pdf", sha256: "a".repeat(64), bytes: 1 });
    const open = vi.fn().mockResolvedValue(undefined);
    await openBook(23, boxes, "web", open);
    expect(open).toHaveBeenCalledWith(expect.objectContaining({ url: "http://x/s.pdf" }), 23, boxes);
  });
  it("no box, no copy, or a failure: the plain link", async () => {
    const link = vi.spyOn(Linking, "openURL").mockResolvedValue(true);
    const open = vi.fn().mockRejectedValue(new Error("boom"));
    vi.spyOn(console, "warn").mockImplementation(() => {});
    await openBook(23, undefined, "web", open);
    await openBook(23, boxes, "web", open);
    setSynopsisSource(null);
    await openBook(23, boxes, "web", open);
    expect(open).toHaveBeenCalledTimes(1);
    expect(link).toHaveBeenCalledTimes(3);
  });
});
