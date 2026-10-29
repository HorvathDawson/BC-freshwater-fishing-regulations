/**
 * T215 (WEB) — open our copy of the synopsis with the entry highlighted, at its page. The PDF is fetched from the release
 * (immutable, named by content: the browser's HTTP cache keeps it after the first open), the highlight annotation is
 * written, and the bytes open as a blob URL at `#page=<pdf>`. The window is opened BEFORE the async work, inside the press,
 * so a popup blocker allows it. `synopsisPdf.ts` is the phone's.
 */
import type { SourceBox } from "@app/core";
import { fetchResilient } from "@app/data";
import { highlightPdf } from "./highlightPdf";
import { getSynopsisNet, type SynopsisSource } from "./synopsisSource";
import type { LoadHooks } from "./synopsisLoad";

/** (T254) the copies opened whole this session (the HTTP cache holds them: a second open asks nothing) */
const opened = new Set<string>();
export const synopsisHeld = (s: SynopsisSource): boolean => opened.has(s.sha256);

/** "%PDF": the body is the file itself, so a Range into it means what it says (a cut download resumes) */
const isPdf = (h: Uint8Array) => h.length >= 4 && h[0] === 0x25 && h[1] === 0x50 && h[2] === 0x44 && h[3] === 0x46;

/**
 * The PDF bytes, read as a stream so `onProgress` can say how far (Content-Length, else the manifest's byte count; null
 * total when neither). (T254) Through `fetchResilient`: a stalled download is cut and RESUMED from where it stopped
 * (`Range`), tried again with backoff, and waits out an offline spell — 25 MB on one bar is never started over.
 */
export async function loadSynopsis(s: SynopsisSource, fetchImpl: typeof fetch = fetch, hooks: Partial<LoadHooks> = {}): Promise<Uint8Array> {
  const net = getSynopsisNet();
  const total = s.bytes > 1 ? s.bytes : null;
  const out = await fetchResilient(s.url, {
    fetchImpl, signal: hooks.signal, wireBytes: total, init: { cache: "force-cache" }, resumable: isPdf,
    meter: net?.meter ?? null, isOnline: net?.online, waitOnline: net?.waitOnline,
    onProgress: (got, len) => hooks.onProgress?.(got, len ?? total),
  });
  opened.add(s.sha256);
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
