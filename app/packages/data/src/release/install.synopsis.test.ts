/**
 * (owner 2026-10-29) THE RULE BOOK IS PART OF THE FIRST DOWNLOAD: a phone's install fetches the release's synopsis PDF
 * with the map files (streamed to disk, sha-verified, counted in the progress), keeps it where the book opens it
 * (`synopsisPath`, T285's `synopsis-<sha16>.pdf`), and an update fetches it again only when its sha256 moves (the old
 * copy is pruned with the old set). A release with no synopsis block installs as before.
 */
import { describe, expect, it } from "vitest";
import { sha256Hex } from "../sha256";
import { parseManifest, type Manifest } from "../manifest";
import { install, prune, synopsisPath, type DownloadTo, type Progress, type ReleaseFs } from "./install";

const enc = (s: string) => new TextEncoder().encode(s);
const H = "d768d6e135e62b99";
const BOOK1 = enc("%PDF rule book one"), BOOK2 = enc("%PDF rule book two, amended");

function manifest(release: string, book: Uint8Array | null): Manifest {
  const core = enc("core");
  return parseManifest({
    format: "app-manifest/2", data_release: release,
    tiles: { compatible: [`${H}.*`], current: null, assets: null, mask: null }, basemap: null,
    files: { core: { url: `${release}/core-x.sqlite.gz`, bytes: core.length, gz: 0, sha256: sha256Hex(core),
                     format: "core/1", section_handles: H } },
    regulations: { valid_until: null, versions: null },
    ...(book ? { synopsis: { id: sha256Hex(book).slice(0, 12), url: `${release}/synopsis-${sha256Hex(book).slice(0, 12)}.pdf`,
                             bytes: book.length, sha256: sha256Hex(book) } } : {}),
  });
}

function memFs(): ReleaseFs & { files: Map<string, Uint8Array> } {
  const files = new Map<string, Uint8Array>();
  return {
    files,
    exists: async (p) => files.has(p) || [...files.keys()].some((k) => k.startsWith(`${p}/`)),
    read: async (p) => files.get(p)!,
    write: async (p, b) => { files.set(p, b); },
    rename: async (a, b) => { files.set(b, files.get(a)!); files.delete(a); },
    remove: async (p) => { for (const k of [...files.keys()]) if (k === p || k.startsWith(`${p}/`)) files.delete(k); },
  };
}

const serve = (books: Record<string, Uint8Array>, log: string[]) => async (url: string) => {
  log.push(url.replace(/^.*\//, ""));
  if (/synopsis-/.test(url)) return Object.values(books).find((b) => url.includes(sha256Hex(b).slice(0, 12)))!;
  return enc("core");
};
/** A streaming download the way a phone's is handed it: to a path, verified, with its bytes reported. */
const toDiskOf = (fs: ReturnType<typeof memFs>, books: Record<string, Uint8Array>, log: string[]): DownloadTo =>
  async (url, path, e, onBytes) => {
    const b = await serve(books, log)(url);
    if (sha256Hex(b) !== e.sha256) throw new Error("sha");
    onBytes?.(b.length);
    fs.files.set(path, b);
  };

describe("install: the rule book comes with the first download", () => {
  it("first install streams it to the book's own path, verified and counted; the pointer names it", async () => {
    const fs = memFs(), log: string[] = [], seen: Progress[] = [];
    const m = manifest("r-1", BOOK1);
    const held = await install(m, fs, serve({ a: BOOK1 }, log), "https://r2.test/", null, (p) => seen.push(p),
                               toDiskOf(fs, { a: BOOK1 }, log));
    expect(fs.files.get(synopsisPath(sha256Hex(BOOK1)))).toEqual(BOOK1);
    expect(synopsisPath(sha256Hex(BOOK1))).toBe(`synopsis-${sha256Hex(BOOK1).slice(0, 16)}.pdf`);
    expect(held.synopsis).toBe(sha256Hex(BOOK1));
    expect(seen.at(-1)!.of).toBe(4 + BOOK1.length);                       // core + the book, in the one bar
    expect(seen.some((p) => p.file === "synopsis")).toBe(true);
  });

  it("an update with the same book does not fetch it again; a new book replaces it and the old is pruned", async () => {
    const fs = memFs(), log: string[] = [];
    const books = { a: BOOK1, b: BOOK2 };
    const m1 = manifest("r-1", BOOK1);
    const h1 = await install(m1, fs, serve(books, log), "https://r2.test/", null, () => {}, toDiskOf(fs, books, log));
    log.length = 0;
    const m2 = manifest("r-2", BOOK1);
    const h2 = await install(m2, fs, serve(books, log), "https://r2.test/", h1, () => {}, toDiskOf(fs, books, log));
    expect(log.filter((u) => u.startsWith("synopsis"))).toEqual([]);
    log.length = 0;
    const m3 = manifest("r-3", BOOK2);
    const h3 = await install(m3, fs, serve(books, log), "https://r2.test/", h2, () => {}, toDiskOf(fs, books, log));
    expect(log.filter((u) => u.startsWith("synopsis"))).toEqual([`synopsis-${sha256Hex(BOOK2).slice(0, 12)}.pdf`]);
    expect(h3.synopsis).toBe(sha256Hex(BOOK2));
    await prune(fs, h2, m2, h3, m3);
    expect(fs.files.has(synopsisPath(sha256Hex(BOOK1)))).toBe(false);
    expect(fs.files.get(synopsisPath(sha256Hex(BOOK2)))).toEqual(BOOK2);
  });

  it("a set installed before the book (its pointer names none) gets it with the next update; no book, no fetch", async () => {
    const fs = memFs(), log: string[] = [];
    const h0 = await install(manifest("r-0", null), fs, serve({}, log), "https://r2.test/", null);
    expect(h0.synopsis).toBeNull();
    const h1 = await install(manifest("r-1", BOOK1), fs, serve({ a: BOOK1 }, log), "https://r2.test/", { ...h0, synopsis: undefined },
                             () => {}, toDiskOf(fs, { a: BOOK1 }, log));
    expect([h1.synopsis, fs.files.has(synopsisPath(sha256Hex(BOOK1)))]).toEqual([sha256Hex(BOOK1), true]);
  });
});
