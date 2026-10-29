/**
 * (T254) A RELEASE FILE OVER A BAD LINE: `fetchVerified`'s promise (the bytes are the manifest's, or it throws) through
 * the gate (its turn in low-bandwidth mode), the resilient download (timeouts, retries, resume) and the activity store
 * (what the screens say while it comes). A file whose bytes do not verify is fetched once more WHOLE (a resume that
 * joined two different files can never be accepted: the sha says so), then refused.
 */
import { verify, VerifyError } from "../fetchVerified";
import { fetchResilient } from "./fetch";
import type { FetchPriority } from "./policy";
import type { Net } from "./index";

/** gzip's first two bytes: the body is the stored `.gz` itself (not decoded on the way), so a Range means what it says */
export const isGzip = (head: Uint8Array): boolean => head.length >= 2 && head[0] === 0x1f && head[1] === 0x8b;
/** A typical release file's gzip ratio, for the meter when the server does not say the encoded length. */
const GZ_RATIO = 0.35;

export interface VerifiedVia {
  net: Net;
  /** the activity / gate key (the release file's name) */
  key: string;
  priority: FetchPriority;
  fetchImpl?: typeof fetch;
  signal?: AbortSignal;
}

export async function fetchVerifiedVia(url: string, expect: { bytes: number; sha256: string }, o: VerifiedVia): Promise<Uint8Array> {
  const { net, key } = o;
  net.activity.queued(key, o.priority, null);
  return net.gate.run(key, o.priority, async () => {
    net.activity.started(key);
    const once = async (resumable: boolean) => {
      let decoded = false, wireLen: number | null = null;
      const body = await fetchResilient(url, {
        fetchImpl: o.fetchImpl, signal: o.signal, meter: net.meter, wireBytes: Math.round(expect.bytes * GZ_RATIO),
        isOnline: net.online, waitOnline: net.waitOnline,
        resumable: (head) => resumable && isGzip(head),
        wireScale: (res, head) => {
          const len = Number(res.headers?.get?.("content-length"));
          wireLen = len > 0 ? len : null;
          decoded = !isGzip(head);
          // the browser decoded a gzip body: the wire carried (encoded / decoded) of each byte handed over
          return decoded ? (wireLen !== null && wireLen < expect.bytes ? wireLen / expect.bytes : GZ_RATIO) : 1;
        },
        onProgress: (loaded) => net.activity.progress(key, loaded, decoded ? expect.bytes : wireLen),
        onRetry: (e) => net.activity.retry(key, e.attempt),
      });
      return verify(url, body, expect);
    };
    try {
      let raw: Uint8Array;
      try { raw = await once(true); }
      catch (e) { if (!(e instanceof VerifyError)) throw e; raw = await once(false); }
      net.activity.done(key);
      return raw;
    } catch (e) {
      net.activity.failed(key, e instanceof Error ? e.message : String(e));
      throw e;
    }
  });
}
