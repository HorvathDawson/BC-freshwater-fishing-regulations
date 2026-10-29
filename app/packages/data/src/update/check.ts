/**
 * (T231) ONE CHEAP LOOK AT ONE OBJECT ON THE DATA ORIGIN: a conditional GET (`If-None-Match` with the ETag last seen), so
 * an object that has not moved costs a 304 and no body; a 200 is hashed (sha256 of the bytes) and compared with the
 * hash last seen, so an origin that rewrites the same bytes (T225 rewrites `index.json` every 24 h even unchanged) is
 * still "unchanged". Never throws: offline and HTTP failures are answers, told apart (`offline`), because an offline
 * phone is the ordinary case for this app and not an error to shout about.
 *
 * `cache: "no-store"`: the browser's HTTP cache must not answer "has this moved?" (the feeds carry max-age=300); the
 * validator is ours, sent by hand, and a 304 reaches this code (the Fetch standard hands a 304 to the caller whenever
 * the request carries its own conditional header).
 */
import { sha256Hex } from "../sha256";

/** What was last seen of an object: its ETag (the origin's validator) and the sha256 of its bytes. */
export interface Seen { etag: string | null; sha256: string | null }

export type Checked =
  | { kind: "unchanged"; etag: string | null }
  | { kind: "changed"; bytes: Uint8Array; etag: string | null; sha256: string }
  | { kind: "failed"; error: string; offline: boolean };

const said = (e: unknown) => (e instanceof Error ? e.message : String(e));

export async function checkObject(url: string, seen: Seen | null, fetchImpl: typeof fetch = fetch,
                                  signal?: AbortSignal): Promise<Checked> {
  let res: Response;
  try {
    // sent STRONG: the edge gzips and weakens the R2 ETag (`W/"…"`), and the data worker compares strong validators
    // only (measured on staging 2026-10-28: a weak one is answered 200, the same value strong 304)
    const headers: Record<string, string> = seen?.etag ? { "If-None-Match": seen.etag.replace(/^W\//, "") } : {};
    res = await fetchImpl(url, { headers, cache: "no-store", ...(signal ? { signal } : {}) });
  } catch (e) {
    return { kind: "failed", error: `${url}: ${said(e)}`, offline: true };
  }
  if (res.status === 304) return { kind: "unchanged", etag: seen?.etag ?? null };
  if (!res.ok) return { kind: "failed", error: `${url}: HTTP ${res.status}`, offline: false };
  let bytes: Uint8Array;
  try {
    bytes = new Uint8Array(await res.arrayBuffer());
  } catch (e) {
    // the connection dropped mid-body: nothing half-read is ever kept
    return { kind: "failed", error: `${url}: ${said(e)}`, offline: true };
  }
  const etag = res.headers.get("ETag");
  const sha256 = sha256Hex(bytes);
  if (seen?.sha256 === sha256) return { kind: "unchanged", etag };
  return { kind: "changed", bytes, etag, sha256 };
}
