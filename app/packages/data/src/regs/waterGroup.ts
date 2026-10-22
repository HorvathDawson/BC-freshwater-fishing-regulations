/**
 * THE WATER MAP'S GROUP (P15): a water's parts, boundaries, section ids and mouth, read from the release through the
 * regs store's keyed reads (`navRows` — with `water_continue`, the stream it goes on as or from —, `siblingOrds`, `item`,
 * `parts`) and put in order by `@app/core` `waterGroup` —
 * the navigation is core's, over this stored graph. No geometry is read: the tiles draw the water; the release says
 * which sections are which part, and where each part's box is.
 */
import { waterGroup, type ItemNavRows, type WaterGroup } from "@app/core";
import type { RegsReads } from "./reads";
import { RegsInputsError } from "./inputs";

export type GroupStore = Pick<RegsReads, "itemOrd" | "siblingOrds" | "item" | "parts" | "navRows">;

/** One item's rows for the group. */
async function rowsOf(store: GroupStore, ord: number): Promise<ItemNavRows> {
  const [item, parts, nav] = await Promise.all([store.item(ord), store.parts(ord), store.navRows(ord)]);
  return {
    ord, item: item.id, name: item.name, kind: item.kind,
    parts: parts.map((p) => ({ partIx: p.partIx, label: p.facts?.label ?? item.name, short: p.facts?.short ?? item.name })),
    geo: nav.geo, marks: nav.marks, sections: nav.sections, exit: nav.exit,
    continuesAs: nav.continuesAs, continuesFrom: nav.continuesFrom,
  };
}

/** The group of the water `item` (its durable id): itself first, then a split lake's other closure areas. */
export async function loadWaterGroup(store: GroupStore, item: string): Promise<WaterGroup> {
  const ord = await store.itemOrd(item);
  if (ord === null) throw new RegsInputsError(`water map: no water ${item} in the regs store`);
  const ords = await store.siblingOrds(ord);
  const all = await Promise.all([ord, ...ords.filter((o) => o !== ord)].map((o) => rowsOf(store, o)));
  return waterGroup(all);
}
