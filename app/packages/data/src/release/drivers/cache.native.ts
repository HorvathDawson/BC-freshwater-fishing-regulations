/**
 * THE NATIVE CACHE (P11): none. The installed release folder is the store (`install`), and a file the phone reads is
 * read from there; caching it again would be a second copy of the same bytes. See `cache.d.ts`.
 */
import type { ByteCache } from "./cache";

export type { ByteCache } from "./cache";

export async function releaseCache(_name = "release-v1"): Promise<ByteCache> {
  return { kind: "none", get: async () => null, put: async () => {} };
}
