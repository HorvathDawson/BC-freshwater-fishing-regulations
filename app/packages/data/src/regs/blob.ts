/**
 * The regs store's blob codec (`meta.blob_codec`): a port of `pipeline/deliver/store/common.py` `unpack` + `json.loads`
 * that runs the same on Hermes, the web and node.
 *
 * Byte 0 says how a blob is stored: 0x01 raw DEFLATE (zlib wbits -15); 0x02 raw DEFLATE with the table's preset
 * dictionary (`zdict.d`); anything else is the JSON text itself (a JSON text never starts with 0x01/0x02). A preset
 * dictionary is why this is fflate and not `DecompressionStream`, which has no dictionary option and is absent on
 * Hermes. `inflateSync` is RAW inflate — no zlib header, so not `unzlibSync`.
 */
import { inflateSync } from "fflate";

export class RegsBlobError extends Error {
  override name = "RegsBlobError";
}

/** The inflated bytes (exported for the codec test and T04's diagnostics). A raw blob comes back as is. */
export function inflateBlob(b: Uint8Array, dict: Uint8Array | null): Uint8Array {
  if (b.length === 0) throw new RegsBlobError("regs store: an empty blob");
  const head = b[0];
  if (head === 0x01) return inflateSync(b.subarray(1));
  if (head === 0x02) {
    if (!dict) throw new RegsBlobError("regs store: a dictionary-deflated blob in a table with no dictionary");
    return inflateSync(b.subarray(1), { dictionary: dict });
  }
  return b;
}

/** A blob's JSON value. Throws RegsBlobError on a 0x02 blob with no dictionary, an empty blob, or bytes that are not JSON. */
export function unpackBlob(b: Uint8Array, dict: Uint8Array | null): unknown {
  let bytes: Uint8Array;
  try {
    bytes = inflateBlob(b, dict);
  } catch (e) {
    if (e instanceof RegsBlobError) throw e;
    throw new RegsBlobError("regs store: a blob is not JSON");   // corrupt deflate data cannot be its JSON either
  }
  try {
    return JSON.parse(utf8(bytes));
  } catch {
    throw new RegsBlobError("regs store: a blob is not JSON");
  }
}

/** Code units per String.fromCharCode call: well under every engine's argument limit. */
const CHUNK = 8192;

/**
 * UTF-8 → string without TextDecoder (Hermes has none). Chunked, never `String.fromCharCode(...hugeArray)`: the store's
 * statics inflate to ~200 KB. Same decoding as core statusIndex's private `utf8`, which stays core's own.
 */
export function utf8(b: Uint8Array): string {
  const parts: string[] = [];
  let units: number[] = [];
  for (let i = 0; i < b.length;) {
    const c = b[i++]!;
    let cp: number;
    if (c < 0x80) cp = c;
    else if (c < 0xe0) cp = ((c & 0x1f) << 6) | (b[i++]! & 0x3f);
    else if (c < 0xf0) cp = ((c & 0x0f) << 12) | ((b[i++]! & 0x3f) << 6) | (b[i++]! & 0x3f);
    else cp = ((c & 0x07) << 18) | ((b[i++]! & 0x3f) << 12) | ((b[i++]! & 0x3f) << 6) | (b[i++]! & 0x3f);
    if (cp > 0xffff) {
      cp -= 0x10000;
      units.push(0xd800 + (cp >> 10), 0xdc00 + (cp & 0x3ff));
    } else units.push(cp);
    if (units.length >= CHUNK) {
      parts.push(String.fromCharCode(...units));
      units = [];
    }
  }
  parts.push(String.fromCharCode(...units));
  return parts.join("");
}
