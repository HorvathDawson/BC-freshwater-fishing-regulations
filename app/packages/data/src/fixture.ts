/** An in-memory RegsSource so `core` and the conformance suite can be built and
 *  tested before any bundle exists. Never shipped. */
import type { BundleInfo, ItemId, RegsSource } from "./index.js";

export function fixtureSource(items: string[], info: BundleInfo): RegsSource {
  const set = new Set(items);
  return {
    async info() { return info; },
    async itemExists(id: ItemId) { return set.has(id); },
  };
}
