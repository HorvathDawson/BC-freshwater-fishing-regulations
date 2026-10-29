/**
 * (T269) THE WEB'S WATER ADDRESSES: a named water's page is `/water/<slug>` (the SEO build's committed slug map). The app
 * learns a water's slug from the shard of the map that holds it (`@app/ui` `slugShardPath`, written by the SEO build
 * beside the pages), fetched once on the first water of that shard. A site with no shards (a development server) answers
 * null and the address stays the start link, as before. Only the web's address bar reads this (`systemBack.web.ts`).
 */
import { slugShardPath } from "@app/ui";

const shards = new Map<string, Promise<Readonly<Record<string, string>> | null>>();
const known = new Map<string, string>();

/** A slug already known, synchronously (the page's own water, or a shard already in). */
export const slugKnown = (item: string): string | null => known.get(item) ?? null;

/** Tell what the page itself said: the water a `/water/<slug>` page is about (its `cift:item` meta). */
export function slugSeen(item: string, slug: string): void { known.set(item, slug); }

/** The water's slug, or null (an unnamed reach, a water with no page, a site with no shards, a failed fetch). */
export function slugOf(item: string): Promise<string | null> {
  const hit = known.get(item);
  if (hit) return Promise.resolve(hit);
  const path = slugShardPath(item);
  let p = shards.get(path);
  if (!p) {
    p = fetch(path).then((r) => (r.ok ? r.json() as Promise<Record<string, string>> : null)).catch(() => null);
    shards.set(path, p);
  }
  return p.then((m) => {
    const s = m?.[item] ?? null;
    if (s) known.set(item, s);
    return s;
  });
}
