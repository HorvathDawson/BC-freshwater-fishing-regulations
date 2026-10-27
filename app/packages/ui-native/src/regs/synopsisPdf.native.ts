/**
 * T215 (NATIVE) — open our copy of the synopsis with the entry highlighted. The release's PDF is downloaded ONCE per release
 * (it is named by its sha256) into the cache directory, the highlight annotation is written, and the result is handed to the
 * system to show: the file URL first, the share sheet when no handler takes it. NOT VERIFIED ON A DEVICE (like N1/N2).
 * `synopsisPdf.web.ts` is the browser's.
 */
import { Linking, Share } from "react-native";
import { File, Paths } from "expo-file-system";
import type { SourceBox } from "@app/core";
import { highlightPdf } from "./highlightPdf";
import type { SynopsisSource } from "./synopsisSource";

const held = (s: SynopsisSource) => new File(Paths.cache, `synopsis-${s.sha256.slice(0, 16)}.pdf`);

/** The release's PDF bytes: the cached file when it is there, else downloaded (and kept for the release's life). */
export async function loadSynopsis(s: SynopsisSource, fetchImpl: typeof fetch = fetch): Promise<Uint8Array> {
  const f = held(s);
  if (f.exists) return f.bytes();
  const res = await fetchImpl(s.url);
  if (!res.ok) throw new Error(`synopsis: HTTP ${res.status} for ${s.url}`);
  const bytes = new Uint8Array(await res.arrayBuffer());
  f.create();
  f.write(bytes);
  return bytes;
}

/** Open the synopsis with `boxes` highlighted. Throws when it cannot: the caller falls back to the plain link. */
export async function openHighlighted(s: SynopsisSource, _pdf: number, boxes: readonly SourceBox[]): Promise<void> {
  const out = await highlightPdf(await loadSynopsis(s), boxes);
  const f = new File(Paths.cache, "synopsis-open.pdf");
  if (f.exists) f.delete();
  f.create();
  f.write(out);
  // the page is the first the boxes name: a native viewer ignores `#page=`, so the highlight is what marks the place
  await Linking.openURL(f.uri).catch(() => Share.share({ url: f.uri }).then(() => undefined));
}
