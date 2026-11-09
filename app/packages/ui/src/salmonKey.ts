/**
 * (T228, T517) THE LIVE SALMON KEY of a water — the ONE reading every screen joins a water's parts by (`@app/core`
 * `mergeNav`): the water map (`waterMap.ts` `useSalmonKey`), the header and Parts sheet's names (`useMapEntries`), the
 * receiving water kept in colour, and whether a water is solo (`regs.ts` `regsSoloOf`). Its own module so both sides
 * import it without a cycle.
 */
import { salmonTimelineKey } from "@app/core";
import type { ItemId } from "@app/data";
import type { RegsStore } from "@app/data/regs";
import type { ReleaseLoader } from "@app/data/release";

/** What a salmon reading is read through: the release loader's salmon slices (live DFO feed included). */
export type SalmonLoader = Pick<ReleaseLoader, "salmon" | "regionsOf">;

/**
 * The salmon reading of water `item`'s sections: the release's salmon slices of the regions its parts' answers are in
 * (as the Salmon card reads them), through the loader (live feed included), compared by `@app/core` `salmonTimelineKey`.
 * Null where the release ships no salmon: then nothing joins.
 */
export async function salmonKeyOf(loader: SalmonLoader, store: Pick<RegsStore, "itemOrd" | "parts">,
                                  item: ItemId): Promise<((sid: number) => string) | null> {
  const ord = await store.itemOrd(item);
  const akeys = ord === null ? [] : [...new Set((await store.parts(ord)).map((p) => p.akey))]
    .filter((a): a is number => a !== null);
  // a water with no key at all (wholly outside B.C.) has no salmon to compare: no reading, nothing joins
  if (!akeys.length) return null;
  const regions = [...new Set((await Promise.all(akeys.map((a) => loader.regionsOf(a)))).flat())];
  const f = await loader.salmon(regions);
  if (!f) return null;
  // (T517) every region of the water's keys that HAS a salmon slice must be READ: a section of a region not loaded would
  // read the default (unlisted) chain and join pieces nobody compared — said, never joined. A region with no slice (no
  // DFO place in it) is every section on the unlisted chain: that IS its reading
  const sliced = new Set(f.payload.shards ?? []), loaded = new Set(f.loaded ?? []);
  const unread = regions.filter((r) => sliced.has(r) && !loaded.has(r));
  if (unread.length) throw new Error(`salmon key: ${item}'s region${unread.length > 1 ? "s" : ""} ${unread.join(", ")} not loaded`);
  return (sid: number) => salmonTimelineKey(f, sid);
}
