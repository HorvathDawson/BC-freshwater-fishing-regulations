/**
 * (T555 item 5) WHAT THE READER KEEPS BY ITEM ID FOLLOWS `item_alias`, AND IS REWRITTEN ONCE.
 *
 * An item id is durable (AGENTS 5) but not eternal: a later build can absorb a water into another (a slough folded into
 * its river), and the release then ships `core.item_alias` (old id → the id it became). Every by-id READ follows the
 * alias already (`WaterSource.currentItem`'s rule: `water`, `locate`, `waterBox`, the regulations). But what is KEPT on
 * the device — a spot's water, a gauge alert's water — would go on holding the old id, and everything that compares ids
 * (a water's own spots, its in-season notices, its alerts) would miss it. So after each boot the kept ids are asked
 * once and the absorbed ones rewritten to the id in use.
 *
 * Recent searches hold the TEXT the reader typed, never an id (`@app/core` recentSearches): nothing to follow there.
 * An id that is gone with NO alias is left as it is: the spot keeps its coordinate and its name, and nothing is guessed.
 */
import type { ItemId } from "./types";
import type { WaterSource } from "./source";
import type { Spot } from "./spots/model";
import type { NotifyState } from "./notify/settings";

/** Of `ids`, those this release names by ANOTHER id now: old → current. Unknown ids (gone, no alias) are not in it. */
export async function savedItemRenames(source: Pick<WaterSource, "currentItem">,
                                       ids: Iterable<string | null | undefined>): Promise<Map<string, ItemId>> {
  const out = new Map<string, ItemId>();
  if (!source.currentItem) return out;
  for (const id of new Set([...ids].filter((i): i is string => !!i))) {
    const now = await source.currentItem(id as ItemId);
    if (now !== null && now !== id) out.set(id, now);
  }
  return out;
}

/** The spot with its water's id as this release names it — THE SAME OBJECT when nothing moved (`useSpots.refresh`'s rule). */
export function spotWithCurrentItem(spot: Spot, renames: ReadonlyMap<string, ItemId>): Spot {
  const now = spot.item ? renames.get(spot.item) : undefined;
  return now ? { ...spot, item: now } : spot;
}

/** The alert settings with every alert's water id as this release names it — the same object when nothing moved. */
export const alertsWithCurrentItems = (renames: ReadonlyMap<string, ItemId>) => (s: NotifyState): NotifyState => {
  if (!s.rules.some((r) => r.water?.item && renames.has(r.water.item))) return s;
  return { ...s, rules: s.rules.map((r) => {
    const now = r.water?.item ? renames.get(r.water.item) : undefined;
    return now && r.water ? { ...r, water: { ...r.water, item: now } } : r;
  }) };
};

/** What `followSavedItems` reads and writes: the spots (the app's serialised writer, `useSpots.refresh`) and the alert settings. */
export interface SavedItemStores {
  spots: { list(): Promise<readonly Spot[]>; rewrite(fill: (s: Spot) => Promise<Spot>): Promise<void> };
  /** null: no alerts on this platform (the web) */
  alerts: { read(): NotifyState; update(fn: (s: NotifyState) => NotifyState): unknown } | null;
}

/**
 * ONCE PER RELEASE OPENED (a cold start and an applied update alike, the one path): every kept item id is asked of the
 * release in use, and the absorbed ones are rewritten. Resolves to how many ids moved (0: nothing was written). A failed
 * read or write throws — the caller reports it (`reportFailure`); the kept ids stay as they were and the reads still
 * follow the alias, so nothing opens wrong meanwhile.
 */
export async function followSavedItems(source: Pick<WaterSource, "currentItem">, kept: SavedItemStores): Promise<number> {
  if (!source.currentItem) return 0;
  const spots = await kept.spots.list();
  const rules = kept.alerts?.read().rules ?? [];
  const renames = await savedItemRenames(source, [...spots.map((s) => s.item), ...rules.map((r) => r.water?.item)]);
  if (!renames.size) return 0;
  if (spots.some((s) => s.item && renames.has(s.item))) await kept.spots.rewrite(async (s) => spotWithCurrentItem(s, renames));
  kept.alerts?.update(alertsWithCurrentItems(renames));
  return renames.size;
}
