/**
 * THE WEB CACHE (P11, MINIMAL-PACKAGING §5): Cache Storage by immutable url, else memory. See `cache.d.ts`.
 *
 * Every failure of Cache Storage degrades to memory rather than failing the read: a cache that cannot store is a
 * slower app, never a broken one.
 */
import type { ByteCache } from "./cache";

export type { ByteCache } from "./cache";

function memory(): ByteCache {
  const held = new Map<string, Uint8Array>();
  return {
    kind: "memory",
    get: async (url) => held.get(url) ?? null,
    put: async (url, bytes) => { held.set(url, bytes); },
  };
}

export async function releaseCache(name = "release-v1"): Promise<ByteCache> {
  const storage = (globalThis as { caches?: CacheStorage }).caches;
  if (!storage) return memory();
  let cache: Cache;
  try {
    cache = await storage.open(name);
  } catch {
    return memory();
  }
  const fallback = memory();
  return {
    kind: "cache-storage",
    async get(url) {
      try {
        const hit = await cache.match(url);
        return hit ? new Uint8Array(await hit.arrayBuffer()) : await fallback.get(url);
      } catch {
        return fallback.get(url);
      }
    },
    async put(url, bytes) {
      try {
        await cache.put(url, new Response(bytes.slice().buffer, {
          headers: { "Content-Type": "application/octet-stream", "Content-Length": String(bytes.length) } }));
      } catch {
        await fallback.put(url, bytes);            // quota or a refused origin: keep it for this session
      }
    },
  };
}
