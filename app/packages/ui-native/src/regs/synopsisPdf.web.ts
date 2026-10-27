/**
 * T215 (WEB) — open our copy of the synopsis with the entry highlighted, at its page. The PDF is fetched from the release
 * (immutable, named by content: the browser's HTTP cache keeps it after the first open), the highlight annotation is
 * written, and the bytes open as a blob URL at `#page=<pdf>`. The window is opened BEFORE the async work, inside the press,
 * so a popup blocker allows it. `synopsisPdf.ts` is the phone's.
 */
import type { SourceBox } from "@app/core";
import { highlightPdf } from "./highlightPdf";
import type { SynopsisSource } from "./synopsisSource";

export async function loadSynopsis(s: SynopsisSource, fetchImpl: typeof fetch = fetch): Promise<Uint8Array> {
  const res = await fetchImpl(s.url, { cache: "force-cache" });
  if (!res.ok) throw new Error(`synopsis: HTTP ${res.status} for ${s.url}`);
  return new Uint8Array(await res.arrayBuffer());
}

/** Open the synopsis with `boxes` highlighted. Throws when it cannot: the caller falls back to the plain link. */
export async function openHighlighted(s: SynopsisSource, pdf: number, boxes: readonly SourceBox[]): Promise<void> {
  const win = window.open("", "_blank");
  try {
    const out = await highlightPdf(await loadSynopsis(s), boxes);
    const url = `${URL.createObjectURL(new Blob([out as BlobPart], { type: "application/pdf" }))}#page=${pdf}`;
    if (win) win.location.href = url; else window.open(url, "_blank");
  } catch (e) {
    win?.close();
    throw e;
  }
}
