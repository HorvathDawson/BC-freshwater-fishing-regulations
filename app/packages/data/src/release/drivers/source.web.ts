/**
 * THE WEB SOURCE (P11): a release file's bytes from the cache, else the network — verified either way. See `source.d.ts`.
 *
 * One fetch per file per page: concurrent asks for the same file share the request (the loader already holds what it
 * opened; this covers `paint` and `flow-area`, which are read as bytes).
 *
 * (T254) With the app's `net`, a network fetch takes its turn (the gate: in low-bandwidth mode the tapped water's files
 * first, then the page's extras, then any prefetch — a prefetch the reader then asks for is raised to the reader's turn),
 * survives a bad line (timeouts per size, retries with backoff, waiting out an offline spell) and says how far it got.
 * The cache is read first and never waits its turn.
 *
 * (T409) THE CHECK RUNS IN THE BROWSER'S OWN CODE: sha256 by WebCrypto (`crypto.subtle.digest`) and a stored `.gz` body
 * (a host that does not send `Content-Encoding`) ungzipped by `DecompressionStream`, both off the page's main thread. The
 * JS hash held the main thread ~0.5 s (4x CPU) on a water page's files — most of its blocking time. Same checks (the
 * decoded length, then the sha256), same refusals (`VerifyError`, the same words), the same resilient download
 * (`fetchVerifiedVia`'s gate, retries, resume and one whole refetch). A browser without either API is an error, said and
 * reported (`reportFailure`) — never a silent fall back to the JS hash. The phone keeps `fetchVerified` (Hermes has
 * neither API).
 */
import { reportFailure } from "@app/core";
import { VerifyError } from "../../fetchVerified";
import type { Manifest } from "../../manifest";
import { fetchResilient, PRIORITY, priorityOf, type FetchPriority, type Net } from "../../net";
import type { FileName, ReleaseSource } from "../loader";
import type { SourceAt } from "./source";

export type { SourceAt } from "./source";

export function releaseSource(m: Manifest, at: SourceAt): ReleaseSource {
  const inflight = new Map<FileName, Promise<Uint8Array>>();
  const net = at.net ?? null;
  const read = async (name: FileName, ahead: boolean): Promise<Uint8Array> => {
    const spec = m.files[name];
    if (!spec) throw new Error(`release ${m.data_release} has no file ${name}`);
    const url = `${at.base}${spec.url}`;
    const hit = await at.cache.get(url);
    if (hit) {
      try { return await verifyWeb(url, hit, spec); } catch (e) { if (!(e instanceof VerifyError)) throw e; /* not the manifest's bytes: fetch them again */ }
    }
    const raw = net
      ? await fetchVerifiedWeb(url, spec, { net, key: name, priority: ahead ? PRIORITY.ahead : priorityOf(name), fetchImpl: at.fetch ?? fetch })
      : await fetchOnceWeb(url, spec, at.fetch ?? fetch);
    await at.cache.put(url, raw);
    return raw;
  };
  const urlOf = (name: FileName) => { const spec = m.files[name]; return spec ? `${at.base}${spec.url}` : null; };
  return {
    // (T238) what the Offline maps page asks: is a file kept in the release cache
    async stored(name) {
      const url = urlOf(name);
      return !!url && !!at.cache.has && at.cache.kind === "cache-storage" && await at.cache.has(url);
    },
    bytes(name, o) {
      const ahead = o?.ahead === true;
      let p = inflight.get(name);
      if (!p) {
        p = read(name, ahead).finally(() => inflight.delete(name));
        inflight.set(name, p);
      } else if (!ahead && net) {
        // a file queued ahead that the reader now needs: raise it to the reader's turn (joins the same download)
        void net.gate.run(name, priorityOf(name), () => p!).catch(() => {});
        net.activity.raise(name, priorityOf(name));
      }
      return p;
    },
  };
}

type Expect = { bytes: number; sha256: string };

/** gzip's first two bytes: the stored `.gz` itself came (the host did not decode it) */
const isGzip = (b: Uint8Array): boolean => b.length >= 2 && b[0] === 0x1f && b[1] === 0x8b;

/** (T409) A browser API the check needs and does not have: an error, reported — the JS hash is not a quiet second way. */
class WebVerifyUnsupported extends Error {}
function needs<T>(what: string, v: T | undefined): T {
  if (v) return v;
  const e = new WebVerifyUnsupported(`release check: this browser has no ${what}; the release files cannot be checked`);
  reportFailure(e);
  throw e;
}

const hex = (b: ArrayBuffer): string => Array.from(new Uint8Array(b), (x) => x.toString(16).padStart(2, "0")).join("");

/** (T409) `verify` in the browser's own code: the raw file, or `VerifyError` naming the difference (the same words). */
async function verifyWeb(url: string, body: Uint8Array, expect: Expect): Promise<Uint8Array> {
  const subtle = needs("WebCrypto (crypto.subtle)", (globalThis as { crypto?: Crypto }).crypto?.subtle);
  let raw = body;
  if (isGzip(body)) {
    const DS = needs("DecompressionStream", (globalThis as { DecompressionStream?: typeof DecompressionStream }).DecompressionStream);
    raw = new Uint8Array(await new Response(new Blob([body as BlobPart]).stream().pipeThrough(new DS("gzip"))).arrayBuffer());
  }
  if (raw.length !== expect.bytes)
    throw new VerifyError(`${url}: ${raw.length} bytes, the manifest says ${expect.bytes}`);
  const got = hex(await subtle.digest("SHA-256", raw as BufferSource));
  if (got !== expect.sha256)
    throw new VerifyError(`${url}: sha256 ${got.slice(0, 12)}…, the manifest says ${expect.sha256.slice(0, 12)}…`);
  return raw;
}

/** `fetchVerified` with the browser's check (no app `Net`: one plain fetch). */
async function fetchOnceWeb(url: string, expect: Expect, fetchImpl: typeof fetch): Promise<Uint8Array> {
  const res = await fetchImpl(url);
  if (!res.ok) throw new VerifyError(`${url}: HTTP ${res.status}`);
  return verifyWeb(url, new Uint8Array(await res.arrayBuffer()), expect);
}

/** A typical release file's gzip ratio, for the meter when the server does not say the encoded length (as `net/verified`). */
const GZ_RATIO = 0.35;

/**
 * `fetchVerifiedVia` (net/verified.ts) with the browser's check: the same gate turn, activity, resilient download, Range
 * resume of a stored gzip body, and one whole refetch of bytes that do not verify, then the refusal. Kept beside the web
 * source so the phone's bundle is untouched (T409: web-only).
 */
async function fetchVerifiedWeb(url: string, expect: Expect,
                                o: { net: Net; key: string; priority: FetchPriority; fetchImpl?: typeof fetch }): Promise<Uint8Array> {
  const { net, key } = o;
  net.activity.queued(key, o.priority, null);
  return net.gate.run(key, o.priority, async () => {
    net.activity.started(key);
    const once = async (resumable: boolean) => {
      let decoded = false, wireLen: number | null = null;
      const body = await fetchResilient(url, {
        fetchImpl: o.fetchImpl, meter: net.meter, wireBytes: Math.round(expect.bytes * GZ_RATIO),
        isOnline: net.online, waitOnline: net.waitOnline,
        resumable: (head) => resumable && isGzip(head),
        wireScale: (res, head) => {
          const len = Number(res.headers?.get?.("content-length"));
          wireLen = len > 0 ? len : null;
          decoded = !isGzip(head);
          return decoded ? (wireLen !== null && wireLen < expect.bytes ? wireLen / expect.bytes : GZ_RATIO) : 1;
        },
        onProgress: (loaded) => net.activity.progress(key, loaded, decoded ? expect.bytes : wireLen),
        onRetry: (e) => net.activity.retry(key, e.attempt),
      });
      return verifyWeb(url, body, expect);
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
