// @vitest-environment node
import { describe, expect, it, vi } from "vitest";
import { PDFArray, PDFDict, PDFDocument, PDFName, PDFNumber } from "pdf-lib";
import { highlightPdf } from "./highlightPdf";
import { openBook } from "./openBook";
import { setSynopsisSource } from "./synopsisSource";
import { cancelSynopsisLoad, dismissSynopsisError, getSynopsisLoad, percentOf, type LoadHooks } from "./synopsisLoad";
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
    // (T50) and the document opens on that page, just above the mark (a phone's viewer has no `#page=`)
    const open = doc.catalog.lookup(PDFName.of("OpenAction"), PDFArray);
    expect(open.get(0)).toBe(doc.getPage(1).ref);
    expect(open.get(1)?.toString()).toBe("/XYZ");
    expect((open.get(3) as PDFNumber).asNumber()).toBe(728);
  });
  it("entryFirst (Android): the highlighted page is copied to the front, the whole book follows, no /OpenAction", async () => {
    const out = await highlightPdf(await twoPages(), [{ pdf: 2, box: [36, 100, 540, 120], size: [576, 756] }],
                                   { entryFirst: true });
    const doc = await PDFDocument.load(out);
    expect(doc.getPageCount()).toBe(3);
    const hl = (i: number) => doc.getPage(i).node.lookupMaybe(PDFName.of("Annots"), PDFArray)?.size() ?? 0;
    expect([hl(0), hl(1), hl(2)]).toEqual([1, 0, 1]);
    expect(doc.catalog.get(PDFName.of("OpenAction"))).toBeUndefined();
  });
  it("no box, a page asked: the copy opens at that page", async () => {
    const doc = await PDFDocument.load(await highlightPdf(await twoPages(), [], { page: 2 }));
    expect((doc.catalog.lookup(PDFName.of("OpenAction"), PDFArray)).get(0)).toBe(doc.getPage(1).ref);
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
    expect(open).toHaveBeenCalledWith(expect.objectContaining({ url: "http://x/s.pdf" }), 23, boxes, expect.objectContaining({ signal: expect.anything() }));
  });
  it("no box: still OUR copy, at its page; no copy in the release: the plain link", async () => {
    setSynopsisSource({ url: "http://x/s.pdf", sha256: "a".repeat(64), bytes: 1 });
    const link = vi.spyOn(Linking, "openURL").mockResolvedValue(true);
    const open = vi.fn().mockResolvedValue(undefined);
    await openBook(23, undefined, "web", open);
    expect(open).toHaveBeenCalledWith(expect.anything(), 23, [], expect.anything());
    setSynopsisSource(null);
    await openBook(23, boxes, "web", open);
    expect(open).toHaveBeenCalledTimes(1);
    expect(link).toHaveBeenCalledTimes(1);
  });
});

describe("the synopsis load (owner 2026-10-28)", () => {
  const boxes = [{ pdf: 23, box: [1, 2, 3, 4] as [number, number, number, number], size: [576, 756] as [number, number] }];
  it("one download at a time, progress, cancel, and a failure that keeps Retry's inputs", async () => {
    setSynopsisSource({ url: "http://x/s.pdf", sha256: "a".repeat(64), bytes: 100 });
    let release!: () => void, hooks!: LoadHooks;
    const open = vi.fn((_s, _p, _b, h: LoadHooks) => { hooks = h; return new Promise<void>((r) => { release = r; }); });
    const first = openBook(23, boxes, "web", open as never);
    expect(getSynopsisLoad()).toMatchObject({ phase: "loading", pdf: 23 });
    await openBook(23, boxes, "web", open as never);                 // a second press while loading
    expect(open).toHaveBeenCalledTimes(1);
    hooks.onProgress(25, 100);
    expect(percentOf(getSynopsisLoad() as never)).toBe(25);
    release(); await first;
    expect(getSynopsisLoad().phase).toBe("idle");

    const pending = openBook(23, boxes, "web", open as never);
    cancelSynopsisLoad();
    expect(hooks.signal.aborted).toBe(true);
    release(); await pending;
    expect(getSynopsisLoad().phase).toBe("idle");

    vi.spyOn(console, "warn").mockImplementation(() => {});
    await openBook(23, boxes, "web", vi.fn().mockRejectedValue(new Error("boom")));
    expect(getSynopsisLoad()).toMatchObject({ phase: "error", pdf: 23, boxes });
    dismissSynopsisError();
    expect(getSynopsisLoad().phase).toBe("idle");
    setSynopsisSource(null);
  });
  it("web loadSynopsis streams: progress against Content-Length", async () => {
    const { loadSynopsis } = await import("./synopsisPdf.web");
    const chunks = [new Uint8Array([1, 2]), new Uint8Array([3])];
    const res = { ok: true, status: 200, headers: new Headers({ "content-length": "3" }),
                  body: { getReader: () => ({ read: async () => (chunks.length ? { done: false, value: chunks.shift()! } : { done: true, value: undefined }) }) } };
    const seen: [number, number | null][] = [];
    const out = await loadSynopsis({ url: "u", sha256: "a", bytes: 3 }, (async () => res) as never, { onProgress: (l, t) => seen.push([l, t]) });
    expect([...out]).toEqual([1, 2, 3]);
    expect(seen).toEqual([[2, 3], [3, 3]]);
  });
});
