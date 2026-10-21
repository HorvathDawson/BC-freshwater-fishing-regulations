/**
 * THE WEB SOURCE (P11): a release file's bytes from the cache, else the network — verified either way. See `source.d.ts`.
 *
 * One fetch per file per page: concurrent asks for the same file share the request (the loader already holds what it
 * opened; this covers `paint` and `flow-area`, which are read as bytes).
 */
import { fetchVerified, verify } from "../../fetchVerified";
import type { Manifest } from "../../manifest";
import type { FileName, ReleaseSource } from "../loader";
import type { SourceAt } from "./source";

export type { SourceAt } from "./source";

export function releaseSource(m: Manifest, at: SourceAt): ReleaseSource {
  const inflight = new Map<FileName, Promise<Uint8Array>>();
  const read = async (name: FileName): Promise<Uint8Array> => {
    const spec = m.files[name];
    if (!spec) throw new Error(`release ${m.data_release} has no file ${name}`);
    const url = `${at.base}${spec.url}`;
    const hit = await at.cache.get(url);
    if (hit) {
      try { return verify(url, hit, spec); } catch { /* not the manifest's bytes: fetch them again */ }
    }
    const raw = await fetchVerified(url, spec, undefined, at.fetch ?? fetch);
    await at.cache.put(url, raw);
    return raw;
  };
  return {
    bytes(name) {
      let p = inflight.get(name);
      if (!p) {
        p = read(name).finally(() => inflight.delete(name));
        inflight.set(name, p);
      }
      return p;
    },
  };
}
