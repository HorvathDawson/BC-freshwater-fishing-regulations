/**
 * (T286) THE WATERS ON SCREEN: which water each of a viewport's sections belongs to (core `item_section`, one read per
 * 500 sections), for the regulations prefetch when the map settles (`@app/ui` `useRegsPrefetch`). A section no water
 * holds (an unnamed reach) is left out.
 */
import type { RegsStore } from "./store";

const CHUNK = 500;

export async function itemsOfSections(store: Pick<RegsStore, "core">, sids: readonly number[]): Promise<ReadonlyMap<number, string>> {
  const core = await store.core(), out = new Map<number, string>();
  for (let i = 0; i < sids.length; i += CHUNK) {
    const some = sids.slice(i, i + CHUNK);
    const rows = await core.all(
      `SELECT s.sid AS sid, i.item_id AS item FROM item_section s JOIN item i ON i.ord = s.ord WHERE s.sid IN (${some.map(() => "?").join(", ")})`,
      ...some);
    for (const r of rows) if (!out.has(Number(r.sid))) out.set(Number(r.sid), String(r.item));
  }
  return out;
}
