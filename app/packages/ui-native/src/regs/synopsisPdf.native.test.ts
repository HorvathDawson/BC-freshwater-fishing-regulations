// @vitest-environment node
// (T285) the phone's open of the synopsis: native download to disk (no fetch body on the Java heap); (T354) the marked
// copy made natively + the append, shown in the app's own viewer; a missing index or a failed write is THROWN.
import { beforeEach, describe, expect, it, vi } from "vitest";

const files = new Map<string, { bytes: Uint8Array }>();
let tasks = 0;
const urls: string[] = [];

vi.mock("expo-file-system", () => {
  class File {
    uri: string; constructor(dir: string, name: string) { this.uri = `${dir}/${name}`; }
    get exists() { return files.has(this.uri); }
    get size() { return files.get(this.uri)?.bytes.length ?? 0; }
    get contentUri() { return `content://ca.test.FileSystemFileProvider/${this.uri}`; }
    create() { files.set(this.uri, { bytes: new Uint8Array() }); }
    write(b: Uint8Array, o?: { append?: boolean }) { const was = o?.append ? files.get(this.uri)?.bytes ?? new Uint8Array() : new Uint8Array(); files.set(this.uri, { bytes: new Uint8Array([...was, ...b]) }); }
    async copy(to: File) { files.set(to.uri, { bytes: files.get(this.uri)!.bytes }); }
    delete() { files.delete(this.uri); }
    move(to: File) { files.set(to.uri, files.get(this.uri)!); files.delete(this.uri); }
    async bytes() { return files.get(this.uri)!.bytes; }
  }
  class DownloadTask {
    constructor(private url: string, private to: File, private o: { onProgress?: (p: { bytesWritten: number; totalBytes: number }) => void }) {}
    async downloadAsync() { tasks++; urls.push(this.url); if (this.url.includes("/gone/")) throw Object.assign(new Error("rejected"), { cause: "HTTP 404" }); const b = new Uint8Array([1, 2, 3, 4]); this.to.write(b); this.o.onProgress?.({ bytesWritten: 4, totalBytes: 4 }); return this.to; }
    release() {}
  }
  return { File, DownloadTask, Paths: { cache: "cache", document: "doc" } };
});

import { openHighlighted, synopsisHeld } from "./synopsisPdf.native";
import { closePdf, getPdfView } from "./pdfView";

/** an index for the 4-byte test book: 9 pages, page 9's /Annots array is object 20 and holds one link */
const INDEX = { v: 2, startxref: 0, size: 30, root: [1, 0], info: null, id: null,
                page: Array.from({ length: 9 }, (_, i) => [10 + i, 0, 20 + i, 0, i === 8 ? "5 0 R" : ""]) } as never;
const S = { url: "https://data.test/synopsis.pdf", sha256: "ab".repeat(32), bytes: 4, append: INDEX };
const BOX = [{ pdf: 9, box: [0, 0, 1, 1], size: [576, 756] }] as never;
const text = (uri: string) => new TextDecoder("latin1").decode(files.get(uri)!.bytes);

describe("synopsisPdf.native (T285, T354)", () => {
  beforeEach(() => { files.clear(); tasks = 0; urls.length = 0; closePdf(); vi.spyOn(console, "warn").mockImplementation(() => {}); });

  it("downloads natively once, keeps the file, marks a copy and shows it in the app at the page", async () => {
    expect(synopsisHeld(S)).toBe(false);
    await openHighlighted(S, 9, BOX);
    expect(tasks).toBe(1);
    expect(synopsisHeld(S)).toBe(true);
    expect(getPdfView()).toMatchObject({ uri: "cache/synopsis-open.pdf", page: 9, failed: null });
    const marked = files.get("cache/synopsis-open.pdf")!.bytes;
    expect([...marked.slice(0, 4)]).toEqual([1, 2, 3, 4]);
    // page 9's own /Annots array again, its link kept, the highlight added; the page itself is not rewritten
    expect(text("cache/synopsis-open.pdf")).toMatch(/\n28 0 obj\n\[5 0 R 30 0 R\]\nendobj/);
    expect(text("cache/synopsis-open.pdf")).not.toMatch(/\n18 0 obj/);
    // offline: the held copy opens with no download
    await openHighlighted(S, 9, BOX);
    expect(tasks).toBe(1);
  });
  it("no box: the held book itself, at the page", async () => {
    await openHighlighted(S, 1, []);
    expect(getPdfView()).toMatchObject({ uri: `doc/synopsis-${S.sha256.slice(0, 16)}.pdf`, page: 1 });
    expect(files.has("cache/synopsis-open.pdf")).toBe(false);
  });
  it("a release with no index, or a held copy of another size: thrown, never the book quietly unmarked", async () => {
    await expect(openHighlighted({ ...S, append: null }, 9, BOX)).rejects.toThrow(/no page index/);
    expect(getPdfView()).toBeNull();
    await expect(openHighlighted({ ...S, bytes: 99 }, 9, BOX)).rejects.toThrow();
    expect(getPdfView()).toBeNull();
  });
  it("a 404 on the release's own copy (the release was replaced): the same bytes from the live release", async () => {
    const gone = { ...S, url: "https://data.test/release/gone/synopsis.pdf", alternate: async () => "https://data.test/release/live/synopsis.pdf" };
    await openHighlighted(gone, 9, BOX);
    expect(urls).toEqual([gone.url, "https://data.test/release/live/synopsis.pdf"]);
    expect(getPdfView()?.page).toBe(9);
  });
});
