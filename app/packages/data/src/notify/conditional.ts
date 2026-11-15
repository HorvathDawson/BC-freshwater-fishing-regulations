/**
 * (T741) A SMALL JSON FILE BY CONDITIONAL GET, WITH THE LAST COPY KEPT: the background check's release manifest and
 * version file (the gauges' `obs.json` is `fetchObs` with its own kept copy, the same rules). Exactly as fresh as a
 * whole read:
 *   - a copy and its ETag kept → `If-None-Match` in the STRONG form (as `update/check.ts`: the edge gzips and weakens
 *     R2's ETag, the data worker compares strong validators against the R2 object), and only a 304 answers the copy;
 *   - a 200 is read whole, `check`ed (it throws on a body the reader refuses), then kept with its ETag;
 *   - no ETag in the answer → nothing kept, every run a whole read, and that is said (`console.info`);
 *   - a kept copy that is unreadable or that `check` refuses is reported and read whole;
 *   - a 304 with no copy kept (a header never sent) is a failure, never an empty answer.
 * Errors are the fetch's own (offline stays the environment's) or `<url>: HTTP <status>`, as the plain reads were.
 */
import { reportFailure } from "@app/core";

/** Where a kept copy lives (a cache: losing it costs one whole read). */
export interface HeldCopy { read(): string | null; write(text: string): void }

export async function conditionalJson(url: string, fetchImpl: typeof fetch, held: HeldCopy,
                                      check: (value: unknown) => void = () => {}): Promise<unknown> {
  const kept = readKept(url, held, check);
  const headers: Record<string, string> = kept ? { "If-None-Match": kept.etag.replace(/^W\//, "") } : {};
  const res = await fetchImpl(url, { cache: "no-store", headers });
  if (res.status === 304 && kept) return kept.value;
  if (!res.ok) throw new Error(`${url}: HTTP ${res.status}`);
  const value: unknown = await res.json();
  check(value);
  const etag = res.headers?.get?.("ETag") ?? null;
  if (etag) held.write(JSON.stringify({ url, etag, value }));
  else {
    held.write("");
    console.info(`[held] ${url}: no ETag in the answer; the next check reads the whole file again`);
  }
  return value;
}

function readKept(url: string, held: HeldCopy, check: (value: unknown) => void): { etag: string; value: unknown } | null {
  const text = held.read();
  if (!text) return null;
  let k: { url?: unknown; etag?: unknown; value?: unknown };
  try { k = JSON.parse(text) as typeof k; } catch (e) {
    reportFailure(new Error(`kept copy of ${url} is not JSON (${e instanceof Error ? e.message : String(e)}); reading it whole`));
    return null;
  }
  if (k.url !== url || typeof k.etag !== "string" || !k.etag) return null;
  try { check(k.value); } catch (e) {
    reportFailure(new Error(`kept copy of ${url} is refused (${e instanceof Error ? e.message : String(e)}); reading it whole`));
    return null;
  }
  return { etag: k.etag, value: k.value };
}
