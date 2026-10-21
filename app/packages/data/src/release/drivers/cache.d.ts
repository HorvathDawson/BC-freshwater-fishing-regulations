/**
 * The shared declaration for `./cache` (P11): where a fetched release file is kept between visits.
 *
 *   cache.web.ts      Cache Storage, keyed by the file's IMMUTABLE url (every url carries its sha: `core-20548c7ea6ec…`),
 *                     so an entry never goes stale and a new release is new keys. Where Cache Storage is unavailable
 *                     (an insecure origin, a private window, a test) it falls back to memory for the session.
 *   cache.native.ts   none: the installed release folder IS the store (`install`), so there is nothing to cache.
 *
 * What comes out is checked again by the caller (`source.web.ts` re-verifies length and sha256): a cache is a place
 * bytes were put, not a proof of what they are.
 */
export interface ByteCache {
  /** Where it keeps them: "cache-storage", "memory", or "none". */
  readonly kind: "cache-storage" | "memory" | "none";
  get(url: string): Promise<Uint8Array | null>;
  put(url: string, bytes: Uint8Array): Promise<void>;
}

export declare function releaseCache(name?: string): Promise<ByteCache>;
