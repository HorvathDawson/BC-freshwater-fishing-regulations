/**
 * T215 (NATIVE) — open our copy of the synopsis with the entry highlighted. The release's PDF is downloaded ONCE per release
 * (it is named by its sha256) into the cache directory, the highlight annotation is written, and the result is handed to the
 * system to show: on Android a VIEW intent on its content:// URI (verified on the emulator, T50); on iOS the file URL
 * first, the share sheet when no handler takes it (iOS not yet verified on a device).
 * `synopsisPdf.web.ts` is the browser's.
 */
import { Linking, Platform, Share } from "react-native";
import { File, Paths } from "expo-file-system";
import { startActivityAsync } from "expo-intent-launcher";
import type { SourceBox } from "@app/core";
import { highlightPdf } from "./highlightPdf";
import type { SynopsisSource } from "./synopsisSource";
import type { LoadHooks } from "./synopsisLoad";

/** Android's `Intent.FLAG_GRANT_READ_URI_PERMISSION`: the viewer may read the one file it is handed. */
const GRANT_READ_URI = 0x1;

const held = (s: SynopsisSource) => new File(Paths.cache, `synopsis-${s.sha256.slice(0, 16)}.pdf`);

/** The release's PDF bytes: the cached file when it is there, else downloaded (and kept for the release's life). */
export async function loadSynopsis(s: SynopsisSource, fetchImpl: typeof fetch = fetch, hooks: Partial<LoadHooks> = {}): Promise<Uint8Array> {
  const f = held(s);
  if (f.exists) return f.bytes();
  // (owner 2026-10-28) the loading card shows on the phone too; React Native's fetch gives no stream, so the bar is
  // indeterminate there (the percent shows where `res.body` streams) and Cancel aborts the request.
  const res = await fetchImpl(s.url, { signal: hooks.signal });
  if (!res.ok) throw new Error(`synopsis: HTTP ${res.status} for ${s.url}`);
  const bytes = new Uint8Array(await res.arrayBuffer());
  hooks.onProgress?.(bytes.length, bytes.length);
  f.create();
  f.write(bytes);
  return bytes;
}

/** Open the synopsis with `boxes` highlighted. Throws when it cannot: the caller falls back to the plain link. */
export async function openHighlighted(s: SynopsisSource, pdf: number, boxes: readonly SourceBox[],
                                      hooks: Partial<LoadHooks> = {}, os: string = Platform.OS): Promise<void> {
  // Android's viewers open a file at its first page whatever it asks (`#page=`, /OpenAction): the entry goes first there
  const out = await highlightPdf(await loadSynopsis(s, fetch, hooks), boxes, { entryFirst: os === "android", page: pdf });
  const f = new File(Paths.cache, "synopsis-open.pdf");
  if (f.exists) f.delete();
  f.create();
  f.write(out);
  // the page is the first the boxes name: a native viewer ignores `#page=`, so the highlight is what marks the place
  if (os === "android") {
    // (T50) ANDROID: `Linking` refuses a file:// URI and sends no read grant with a content:// one, and `Share` drops
    // `url` there (the reader got an EMPTY share sheet). A VIEW intent on the file's content:// URI (expo-file-system's
    // FileProvider) with FLAG_GRANT_READ_URI_PERMISSION opens it in the reader's PDF app; no PDF app rejects, and
    // `openBook` falls back to the plain link.
    await startActivityAsync("android.intent.action.VIEW",
                             { data: f.contentUri, type: "application/pdf", flags: GRANT_READ_URI });
    return;
  }
  await Linking.openURL(f.uri).catch(() => Share.share({ url: f.uri }).then(() => undefined));
}
