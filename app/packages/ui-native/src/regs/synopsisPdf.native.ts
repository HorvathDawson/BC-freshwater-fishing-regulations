/**
 * T215 (NATIVE) — open our copy of the synopsis with the entry highlighted. The release's PDF is downloaded ONCE per release
 * (it is named by its sha256) into the app's document directory, the highlight annotation is written, and the result is handed to the
 * system to show: on Android a VIEW intent on its content:// URI (verified on the emulator, T50); on iOS the file URL
 * first, the share sheet when no handler takes it (iOS not yet verified on a device).
 * `synopsisPdf.web.ts` is the browser's.
 */
import { Linking, Platform, Share } from "react-native";
import { DownloadTask, File, Paths } from "expo-file-system";
import { startActivityAsync } from "expo-intent-launcher";
import type { SourceBox } from "@app/core";
import { synopsisPath } from "@app/data/release";
import { highlightPdf } from "./highlightPdf";
import { getSynopsisNet, type SynopsisSource } from "./synopsisSource";
import type { LoadHooks } from "./synopsisLoad";

/** Android's `Intent.FLAG_GRANT_READ_URI_PERMISSION`: the viewer may read the one file it is handed. */
const GRANT_READ_URI = 0x1;

// (T285) the document directory, not the cache: the OS may empty the cache, and the rule book must open offline
// (owner 2026-10-29) the same file a first install streams it to (`synopsisPath`): the book opens offline from the start
const held = (s: SynopsisSource) => new File(Paths.document, synopsisPath(s.sha256));
/** (T254) whether the release's copy is already in the cache directory (nothing to offer: it opens at once) */
export const synopsisHeld = (s: SynopsisSource): boolean => { try { return held(s).exists; } catch { return false; } };

/** (T285) thrown when the device has no app that opens a PDF: the card says so (and still offers gov.bc.ca) */
class NoPdfViewerError extends Error { constructor(cause?: unknown) { super(`no app on this device opens a PDF${cause ? `: ${String(cause)}` : ""}`); this.name = "NoPdfViewer"; } }

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

/** Hand `f` to the system's PDF viewer (Android: a VIEW intent on its content:// URI with a read grant). */
async function viewAndroid(f: File): Promise<void> {
  try {
    await startActivityAsync("android.intent.action.VIEW", { data: f.contentUri, type: "application/pdf", flags: GRANT_READ_URI });
  } catch (e) { throw new NoPdfViewerError(e); }
}

const openUrl = (uri: string) => Linking.openURL(uri).catch(() => Share.share({ url: uri }).then(() => undefined));

/** Open the synopsis with `boxes` highlighted. Throws when it cannot: the caller falls back to the plain link. */
export async function openHighlighted(s: SynopsisSource, pdf: number, boxes: readonly SourceBox[],
                                      hooks: Partial<LoadHooks> = {}, os: string = Platform.OS): Promise<void> {
  // Android's viewers open a file at its first page whatever it asks (`#page=`, /OpenAction): the entry goes first there
  const cached = await downloadSynopsis(s, hooks);
  // (T285) the About link: the book from its first page, nothing to mark — the held file itself is what opens
  if (!boxes.length && pdf <= 1) return os === "android" ? viewAndroid(cached) : openUrl(cached.uri);
  const f = new File(Paths.cache, "synopsis-open.pdf");
  try {
    const out = await highlightPdf(await cached.bytes(), boxes, { entryFirst: os === "android", page: pdf });
    if (f.exists) f.delete();
    f.create();
    f.write(out);
  } catch (e) {
    // (T285) the highlight could not be drawn (a big file on a small heap): open the book itself rather than fail the open
    console.warn(`synopsis: the highlight failed (PDF page ${pdf}); opening the plain copy`, e);
    if (os === "android") return viewAndroid(cached);
    return openUrl(cached.uri);
  }
  // the page is the first the boxes name: a native viewer ignores `#page=`, so the highlight is what marks the place
  if (os === "android") {
    // (T50) ANDROID: `Linking` refuses a file:// URI and sends no read grant with a content:// one, and `Share` drops
    // `url` there (the reader got an EMPTY share sheet). A VIEW intent on the file's content:// URI (expo-file-system's
    // FileProvider) with FLAG_GRANT_READ_URI_PERMISSION opens it in the reader's PDF app; with no PDF app it throws
    // NoPdfViewerError (the card says so).
    return viewAndroid(f);
  }
  await openUrl(f.uri);
}
