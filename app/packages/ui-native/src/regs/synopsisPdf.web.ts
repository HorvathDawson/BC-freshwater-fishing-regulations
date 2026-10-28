/**
 * T215 (WEB) — open our copy of the synopsis with the entry highlighted, at its page. The PDF is fetched from the release
 * (immutable, named by content: the browser's HTTP cache keeps it after the first open), the highlight annotation is
 * written, and the bytes open as a blob URL at `#page=<pdf>`. The window is opened BEFORE the async work, inside the press,
 * so a popup blocker allows it. `synopsisPdf.ts` is the phone's.
 */
import type { SourceBox } from "@app/core";
import { highlightPdf } from "./highlightPdf";
import type { SynopsisSource } from "./synopsisSource";
import type { LoadHooks } from "./synopsisLoad";

/** The PDF bytes, read as a stream so `onProgress` can say how far (Content-Length, else the manifest's byte count; null total when neither). */
export async function loadSynopsis(s: SynopsisSource, fetchImpl: typeof fetch = fetch, hooks: Partial<LoadHooks> = {}): Promise<Uint8Array> {
  const res = await fetchImpl(s.url, { cache: "force-cache", signal: hooks.signal });
  if (!res.ok) throw new Error(`synopsis: HTTP ${res.status} for ${s.url}`);
  const len = Number(res.headers?.get?.("content-length"));
  const total = len > 0 ? len : s.bytes > 1 ? s.bytes : null;
  const reader = res.body?.getReader?.();
  if (!reader) return new Uint8Array(await res.arrayBuffer());
  const parts: Uint8Array[] = [];
  let got = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    parts.push(value); got += value.length;
    hooks.onProgress?.(got, total);
  }
  const out = new Uint8Array(got);
  let at = 0;
  for (const p of parts) { out.set(p, at); at += p.length; }
  return out;
}

/** Open the synopsis with `boxes` highlighted. Throws when it cannot: the caller falls back to the plain link. */
export async function openHighlighted(s: SynopsisSource, pdf: number, boxes: readonly SourceBox[],
                                      hooks: Partial<LoadHooks> = {}): Promise<void> {
  const win = window.open("", "_blank");
  // the new tab is blank until the bytes arrive: say so in it
  try { if (win) win.document.body.textContent = "Loading the synopsis…"; } catch { /* a blocked write is only a missing label */ }
  try {
    const out = await highlightPdf(await loadSynopsis(s, fetch, hooks), boxes);
    const url = `${URL.createObjectURL(new Blob([out as BlobPart], { type: "application/pdf" }))}#page=${pdf}`;
    if (win) win.location.href = url; else window.open(url, "_blank");
  } catch (e) {
    win?.close();
    throw e;
  }
}
