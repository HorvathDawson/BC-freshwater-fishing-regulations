/**
 * `fetchVerified` (P10): fetch a release file and prove it is the one the manifest names, or throw.
 *
 * THE BYTES ARE CHECKED, NEVER A HEADER. `Content-Length` says what the server meant to send, not what arrived (and it is
 * absent or wrong under transparent gzip). The manifest states the raw length and sha256; a body is accepted only when its
 * DECODED bytes match both. Release files are stored gzipped: a server that sends `Content-Encoding: gzip` (the dev
 * server, R2 with the header set) has the client decode it; one that sends the `.gz` bytes as they are leaves the gzip
 * magic on them, and this decodes them. Either way the check is on the raw file.
 */
import { gunzipSync } from "fflate";
import { sha256Hex } from "./sha256";

export class VerifyError extends Error {}

/** Decode a gzip member if `bytes` is one, else return them unchanged. */
export function ungzip(bytes: Uint8Array): Uint8Array {
  return bytes.length >= 2 && bytes[0] === 0x1f && bytes[1] === 0x8b ? gunzipSync(bytes) : bytes;
}

/** Hold fetched bytes to the manifest's `{bytes, sha256}`: the raw file, or throw naming the difference. */
export function verify(url: string, body: Uint8Array, expect: { bytes: number; sha256: string }): Uint8Array {
  const raw = ungzip(body);
  if (raw.length !== expect.bytes)
    throw new VerifyError(`${url}: ${raw.length} bytes, the manifest says ${expect.bytes}`);
  const got = sha256Hex(raw);
  if (got !== expect.sha256)
    throw new VerifyError(`${url}: sha256 ${got.slice(0, 12)}…, the manifest says ${expect.sha256.slice(0, 12)}…`);
  return raw;
}

export async function fetchVerified(url: string, expect: { bytes: number; sha256: string },
                                    signal?: AbortSignal, fetchImpl: typeof fetch = fetch): Promise<Uint8Array> {
  const res = await fetchImpl(url, { signal });
  if (!res.ok) throw new VerifyError(`${url}: HTTP ${res.status}`);
  return verify(url, new Uint8Array(await res.arrayBuffer()), expect);
}
