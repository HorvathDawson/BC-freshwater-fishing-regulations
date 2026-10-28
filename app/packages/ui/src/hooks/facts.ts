/**
 * WHAT THE DATA SAYS ABOUT ITSELF: the bundle's and the feed's counts (read, never stated), and the regulations read of
 * the water that is open.
 */
import { useState } from "react";
import type { BundleCounts, GaugeFeed, ItemId, WaterSource } from "@app/data";
import { useAsync, type Async } from "../async";

/**
 * What the Layers sheet says about the data it is sitting on.
 *
 * THE POINT OF THIS HOOK IS THAT THE NUMBERS ARE READ. `App.tsx` used to pass
 * `waters={255} reaches={6967} surveyed={35} stations={5}` and a `fetchedAt` string, all
 *  five transcribed by hand from a design mock-up — whose fixture is one valley. Against
 * the shipped province bundle the sheet therefore reported 5 stations where there are
 * 2,324, and an age three days older than the feed it was showing, under a heading that
 * reads "EVERY VALUE HAS AN AGE".
 *
 * The bundle's counts come from the bundle; the live count and the age come from the feed's
 * own index. Anything unavailable stays `null` and the sheet omits it — a fabricated figure
 * is worse than a missing one, because a missing one is visibly missing.
 */
export function useDataFacts(source: WaterSource, feed?: GaugeFeed, wanted = true): Async<{
  counts: BundleCounts | null;
  /** Stations the LIVE FEED is publishing — not the same as the bundle's roster. */
  liveStations: number | null;
  /** When the feed was last built, ISO. Null when it could not be reached. */
  fetchedAt: string | null;
}> {
  // READ ON DEMAND (P11): on the release `counts()` opens core, which a cold start does not fetch before the first map.
  // So nothing is asked until a screen that shows the figures is up (`wanted`), and once asked they are kept.
  const [asked, setAsked] = useState(wanted);
  if (wanted && !asked) setAsked(true);
  return useAsync(async () => {
    // The bundle is local and always answers; the feed is a network call that may not. Each
    // is caught on its own, so a feed that is down cannot blank the bundle's counts.
    const [counts, index] = await Promise.all([
      source.counts().catch(() => null),
      feed ? feed.index().catch(() => null) : Promise.resolve(null),
    ]);
    return {
      counts,
      liveStations: index ? Object.keys(index.stations).length : null,
      fetchedAt: index?.fetchedAt ?? null,
    };
  }, `datafacts:${feed ? "feed" : "nofeed"}`, asked);
}

/**
 * THE REGULATIONS READ OF THE WATER THAT IS OPEN (P11): its answer keys, through the source — on the release that opens
 * core, water and the region file holding them (`WaterSource.regulationKeys`), so opening a water in another region
 * fetches that region's file. Nothing is asked while no water is open. T01/T03 draw the page from what this opened.
 */
export function useRegulationKeys(source: WaterSource, item: ItemId | null): Async<readonly number[]> {
  return useAsync(async () => (item ? source.regulationKeys(item) : []), `regulation-keys:${item ?? ""}`, item !== null);
}
