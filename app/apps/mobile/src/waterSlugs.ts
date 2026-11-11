/**
 * (T269) THE WEB'S WATER ADDRESSES: a named water's page is `/water/<slug>` (the SEO build's committed slug map). The app
 * learns a water's slug from the shard of the map that holds it (`@app/ui` `slugShardPath`, written by the SEO build
 * beside the pages), fetched once on the first water of that shard. A site with no shards (a development server) answers
 * null and the address stays the start link, as before. The web's address bar (`systemBack.web.ts`) and its Share
 * (`share.web.ts`) read this. (T624) The reader itself is `@app/ui` `slugReader`, shared with the phone's Share.
 */
import { slugReader } from "@app/ui";

const reader = slugReader("");

/** A slug already known, synchronously (the page's own water, or a shard already in). */
export const slugKnown = (item: string): string | null => reader.known(item);

/** Tell what the page itself said: the water a `/water/<slug>` page is about (its `cift:item` meta). */
export const slugSeen = (item: string, slug: string): void => reader.seen(item, slug);

/** The water's slug, or null (an unnamed reach, a water with no page, a site with no shards, a failed fetch). */
export const slugOf = (item: string): Promise<string | null> => reader.of(item);
