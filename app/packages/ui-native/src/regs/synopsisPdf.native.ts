/**
 * T215 (NATIVE) — open our copy of the synopsis with the entry highlighted. The release's PDF is downloaded ONCE per release
 * (it is named by its sha256) into the app's document directory, the highlight is appended, and (T354) the result
 * opens in the app's own viewer at the page (`PdfViewer.native.tsx`).
 * `synopsisPdf.web.ts` is the browser's.
 */
import { DownloadTask, File, Paths } from "expo-file-system";
import type { SourceBox } from "@app/core";
import { synopsisPath } from "@app/data/release";
import { appendHighlight } from "./appendPdf";
import { showPdf } from "./pdfView";
import { getSynopsisNet, type SynopsisSource } from "./synopsisSource";
import type { LoadHooks } from "./synopsisLoad";

// (T285) the document directory, not the cache: the OS may empty the cache, and the rule book must open offline
// (owner 2026-10-29) the same file a first install streams it to (`synopsisPath`): the book opens offline from the start
const held = (s: SynopsisSource) => new File(Paths.document, synopsisPath(s.sha256));
/** (T254) whether the release's copy is already in the cache directory (nothing to offer: it opens at once) */
export const synopsisHeld = (s: SynopsisSource): boolean => { try { return held(s).exists; } catch { return false; } };

const wait = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * The release's PDF, on disk: the cached file when it is there, else downloaded (and kept for the release's life).
 * (T285) THROUGH expo-file-system's native DownloadTask straight to a `.part` — `fetch` held the whole 25 MB body on the
 * Java heap (the T262 OOM; a phone with a small heap failed the open as "couldn't load"). Tried again with backoff; an
 * offline spell is waited out; a file of the wrong size is thrown away, never kept.
 */
async function downloadSynopsis(s: SynopsisSource, hooks: Partial<LoadHooks> = {}, tries = 3): Promise<File> {
  const f = held(s);
  if (f.exists) return f;
  const net = getSynopsisNet();
  const part = new File(Paths.document, `synopsis-${s.sha256.slice(0, 16)}.part`);
  let last: unknown = null;
  let url = s.url;
  let moved = false;
  for (let i = 0; i < tries; i++) {
    if (hooks.signal?.aborted) throw new Error("cancelled");
    if (i > 0) await wait(1500 * 2 ** (i - 1));
    if (net?.online && !net.online()) await net.waitOnline?.(hooks.signal);
    if (part.exists) part.delete();
    const task = new DownloadTask(url, part, { signal: hooks.signal, sessionType: "foreground",   // (T313) iOS: as install.native.ts
      onProgress: (p) => hooks.onProgress?.(p.bytesWritten, p.totalBytes > 0 ? p.totalBytes : (s.bytes > 1 ? s.bytes : null)) });
    try {
      if (!(await task.downloadAsync())) throw new Error(`${url}: the download stopped`);
      if (s.bytes > 1 && part.size !== s.bytes) throw new Error(`${url}: ${part.size} bytes, expected ${s.bytes}`);
      part.move(f);
      hooks.onProgress?.(f.size, f.size);
      return f;
    } catch (e) {
      last = e;
      if (hooks.signal?.aborted) throw e;
      console.warn(`synopsis download: attempt ${i + 1} failed`, e);
      // (T285) 404: this release's copy is gone (a newer release replaced it); the same bytes are under the live one
      if (!moved && s.alternate && /404/.test(String((e as Error)?.message ?? e) + String((e as Error)?.cause ?? ""))) {
        moved = true;
        const alt = await s.alternate().catch(() => null);
        if (alt && alt !== url) { url = alt; i--; }
      }
    } finally { try { task.release(); } catch { /* already released */ } }
  }
  if (part.exists) part.delete();
  throw last instanceof Error ? last : new Error(String(last));
}

/** The release's PDF bytes (from the cached file; downloaded first when it is not there). */
export async function loadSynopsis(s: SynopsisSource, _fetchImpl: typeof fetch = fetch, hooks: Partial<LoadHooks> = {}): Promise<Uint8Array> {
  return (await downloadSynopsis(s, hooks)).bytes();
}

/**
 * The held book with `boxes` highlighted on their own pages, as a cache file; null: nothing to mark (the book itself
 * opens). (T354) The copy is made natively and a few hundred bytes are written on its end (`appendPdf.ts`): the book
 * never enters JS. A release without the index, a held file of another size or a failed write THROWS (the card says
 * so): never the book quietly unmarked.
 */
async function markedCopy(s: SynopsisSource, cached: File, boxes: readonly SourceBox[]): Promise<File | null> {
  if (!boxes.length) return null;
  if (!s.append) throw new Error("synopsis: this release ships no page index (synopsis.append)");
  if (cached.size !== s.bytes) throw new Error(`synopsis: the held copy is ${cached.size} bytes, the index is for ${s.bytes}`);
  const tail = appendHighlight(s.append, s.bytes, boxes);
  if (!tail) return null;                          // no box on a page of the book: the page opens unmarked
  const f = new File(Paths.cache, "synopsis-open.pdf");
  if (f.exists) f.delete();
  await cached.copy(f);
  f.write(tail, { append: true });
  return f;
}

/**
 * Open the synopsis at PDF page `pdf` with `boxes` highlighted, IN THE APP (T354, owner 2026-11-03): the viewer opens the
 * file at the page, the highlight on the page in its place. Throws when the book cannot be had or marked: the caller's
 * card says so (Retry, gov.bc.ca).
 */
export async function openHighlighted(s: SynopsisSource, pdf: number, boxes: readonly SourceBox[],
                                      hooks: Partial<LoadHooks> = {}): Promise<void> {
  const t0 = Date.now();
  const cached = await downloadSynopsis(s, hooks);
  const t1 = Date.now();
  const marked = await markedCopy(s, cached, boxes);
  // what opened, for the test bed (logcat) and a report: the page, the boxes it was handed, marked or plain, and how
  // long the book (held or fetched) and the marked copy took
  console.info(`[synopsis] open PDF p.${pdf}: ${boxes.length} box(es) → ${marked ? "highlighted copy" : "plain book"}`
    + ` (book ${t1 - t0} ms, copy ${Date.now() - t1} ms)`);
  showPdf((marked ?? cached).uri, Math.max(1, pdf));
}
