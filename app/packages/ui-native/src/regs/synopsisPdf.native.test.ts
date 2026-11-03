// @vitest-environment node
// (T285) the phone's open of the synopsis: native download to disk (no fetch body on the Java heap), a VIEW intent on the
// file's content:// URI, a clear "no PDF app" failure, and the highlight failing never failing the open.
import { beforeEach, describe, expect, it, vi } from "vitest";

const files = new Map<string, { bytes: Uint8Array }>();
const intents: unknown[][] = [];
let viewerPresent = true;
let tasks = 0;
const urls: string[] = [];

vi.mock("react-native", () => ({ Platform: { OS: "android" }, Linking: { openURL: vi.fn(async () => {}) }, Share: { share: vi.fn() } }));
vi.mock("expo-intent-launcher", () => ({
  startActivityAsync: vi.fn(async (...a: unknown[]) => { intents.push(a); if (!viewerPresent) throw new Error("ActivityNotFoundException"); }),
}));
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
const highlight = vi.fn(async (b: Uint8Array) => new Uint8Array([...b, 9]));
vi.mock("./highlightPdf", () => ({ highlightPdf: (...a: [Uint8Array]) => highlight(...a) }));

import { openHighlighted, synopsisHeld } from "./synopsisPdf.native";

const S = { url: "https://data.test/synopsis.pdf", sha256: "ab".repeat(32), bytes: 4 };
const BOX = [{ pdf: 9, box: [0, 0, 1, 1], size: [576, 756] }] as never;

describe("synopsisPdf.native (T285)", () => {
  beforeEach(() => { files.clear(); intents.length = 0; viewerPresent = true; tasks = 0; urls.length = 0; highlight.mockClear(); vi.spyOn(console, "warn").mockImplementation(() => {}); });

  it("downloads natively once, keeps the file, and views the highlighted copy by content:// URI with a read grant", async () => {
    expect(synopsisHeld(S)).toBe(false);
    await openHighlighted(S, 9, BOX, {}, "android");
    expect(tasks).toBe(1);
    expect(synopsisHeld(S)).toBe(true);
    expect(intents).toHaveLength(1);
    expect(intents[0]![0]).toBe("android.intent.action.VIEW");
    expect(intents[0]![1]).toMatchObject({ type: "application/pdf", flags: 1 });
    expect(String((intents[0]![1] as { data: string }).data)).toMatch(/^content:\/\//);
    // offline: the held copy opens with no download
    await openHighlighted(S, 9, BOX, {}, "android");
    expect(tasks).toBe(1);
    expect(intents).toHaveLength(2);
  });
  it("(T354) with the release's index: a native copy + the append, the book never read into JS", async () => {
    const append = { v: 1, startxref: 0, size: 10, root: [1, 0], info: null, id: null, pages: [2, 0], kids: "[3 0 R]", count: 9,
                     box: "", page: Array.from({ length: 9 }, () => "/Contents 4 0 R") } as const;
    await openHighlighted({ ...S, append: append as never }, 9, BOX, {}, "android");
    expect(highlight).not.toHaveBeenCalled();
    const opened = files.get("cache/synopsis-open.pdf")!.bytes;
    expect([...opened.slice(0, 4)]).toEqual([1, 2, 3, 4]);
    expect(new TextDecoder("latin1").decode(opened.slice(4))).toMatch(/\/Type\/Pages\/Kids \[10 0 R 3 0 R\]\/Count 10>>[\s\S]*%%EOF\n$/);
    expect(String((intents[0]![1] as { data: string }).data)).toMatch(/synopsis-open\.pdf$/);
  });
  it("a highlight that fails still opens the book itself", async () => {
    highlight.mockRejectedValueOnce(new Error("out of memory"));
    await openHighlighted(S, 9, BOX, {}, "android");
    expect(intents).toHaveLength(1);
  });
  it("the About link (no box, page 1) opens the held file as it is", async () => {
    await openHighlighted(S, 1, [], {}, "android");
    expect(highlight).not.toHaveBeenCalled();
    expect(intents).toHaveLength(1);
  });
  it("a 404 on the release's own copy (the release was replaced): the same bytes from the live release", async () => {
    const gone = { ...S, url: "https://data.test/release/gone/synopsis.pdf", alternate: async () => "https://data.test/release/live/synopsis.pdf" };
    await openHighlighted(gone, 9, BOX, {}, "android");
    expect(urls).toEqual([gone.url, "https://data.test/release/live/synopsis.pdf"]);
    expect(intents).toHaveLength(1);
  });
  it("no PDF app: a NoPdfViewer error (the card says so)", async () => {
    viewerPresent = false;
    await expect(openHighlighted(S, 9, BOX, {}, "android")).rejects.toMatchObject({ name: "NoPdfViewer" });
  });
});
