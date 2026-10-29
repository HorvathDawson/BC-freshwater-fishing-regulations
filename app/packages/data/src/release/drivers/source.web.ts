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
 */
import { fetchVerified, verify } from "../../fetchVerified";
import type { Manifest } from "../../manifest";
import { fetchVerifiedVia, PRIORITY, priorityOf } from "../../net";
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
      try { return verify(url, hit, spec); } catch { /* not the manifest's bytes: fetch them again */ }
    }
    const raw = net
      ? await fetchVerifiedVia(url, spec, { net, key: name, priority: ahead ? PRIORITY.ahead : priorityOf(name), fetchImpl: at.fetch ?? fetch })
      : await fetchVerified(url, spec, undefined, at.fetch ?? fetch);
    await at.cache.put(url, raw);
    return raw;
  };
  return {
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
