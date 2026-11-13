/**
 * THE WATER MAP'S GROUP (P15): a water's parts, boundaries, section ids and mouth, read from the release through the
 * regs store's keyed reads (`navRows` — with `water_continue`, the stream it goes on as or from —, `siblingOrds`, `item`,
 * `parts`) and put in order by `@app/core` `waterGroup` —
 * the navigation is core's, over this stored graph. No geometry is read: the tiles draw the water; the release says
 * which sections are which part, and where each part's box is.
 */
import { geoBounds, mergeNav, waterGroup, type Bbox, type ItemNavRows, type WaterGroup } from "@app/core";
import type { RegsReads } from "./reads";
import { RegsInputsError } from "./inputs";

export type GroupStore = Pick<RegsReads, "itemOrd" | "siblingOrds" | "item" | "parts" | "navRows" | "statics">;

/** One item's rows for the group. */
async function rowsOf(store: GroupStore, ord: number): Promise<ItemNavRows> {
  const [item, parts, nav] = await Promise.all([store.item(ord), store.parts(ord), store.navRows(ord)]);
  // (T345) the part the mouth meets, when it is TIDAL (the receiving water's tidal cut piece, never a listed part): the
  // exit says so, and the map says the line instead of opening the receiver at a part its map does not have (the
  // Coquitlam into the Fraser's part 16 opened the Fraser on its first listed part)
  const ex = nav.exit;
  const tidal = !!ex && ex.part !== null && (await store.parts(ex.ord)).some((p) => p.partIx === ex.part && p.tidal);
  return {
    ord, item: item.id, name: item.name, kind: item.kind,
    // (T278; owner T166 2026-10-28: no parts on tidal water) a tidal piece is never listed; `waterGroup` drops it and the
    // marks that reach it (`tidal`)
    // (T517, owner 2026-11-09) a part is named from its two end tokens (`spanName`); with none — a lake, a whole water,
    // and AN UNKEYED PART (outside B.C.: no key, no facts, no rules) — as its water ("Blanchard River"): navReads.test.ts
    parts: parts.filter((p) => !p.tidal).map((p) => ({ partIx: p.partIx, grp: p.grp, run: p.run,
                                                       ...(p.facts ? { label: p.facts.label, lo: p.facts.lo, hi: p.facts.hi }
                                                         : { label: item.name, lo: null, hi: null }),
                                                       // (T460) a side channel has no km: a fork's branch, never a skip
                                                       ...(p.facts && p.facts.km === null ? { side: true } : {}) })),
    tidal: parts.filter((p) => p.tidal).map((p) => p.partIx),
    geo: nav.geo, marks: nav.marks, sections: nav.sections, exit: ex && tidal ? { ...ex, tidal: true } : ex,
    continuesAs: nav.continuesAs, continuesFrom: nav.continuesFrom,
  };
}

/** The group of the water `item` (its durable id): itself first, then a split lake's other closure areas. (T517) The
 *  parts of one run the release cut where the salmon chain changes are joined where `salmonKey` — the LIVE salmon
 *  reading, the one the map uses (`@app/ui` `useSalmonKey` / `salmonKeyOf`) — reads them alike (`@app/core` `mergeNav`);
 *  with none (null: not read, or no salmon in the release) nothing joins: every part is its own entry. */
export async function loadWaterGroup(store: GroupStore, item: string,
                                     salmonKey: ((sid: number) => string) | null = null): Promise<WaterGroup> {
  return mergeNav(await loadRawWaterGroup(store, item), salmonKey);
}

/** (T228) The group as the release cut it: every part apart (`mergeNav` joins them). */
export async function loadRawWaterGroup(store: GroupStore, item: string): Promise<WaterGroup> {
  const ord = await store.itemOrd(item);
  if (ord === null) throw new RegsInputsError(`water map: no water ${item} in the regs store`);
  const ords = await store.siblingOrds(ord);
  // (T345, one load path) the group's items in ONE order whichever of them was opened: a split lake's areas read from
  // any area are the same group, part for part and index for index (it was the opened item first, so a reload on one
  // area listed and stepped the parts in another order than a step into it from its sibling)
  const [all, statics] = await Promise.all([
    Promise.all([...new Set([ord, ...ords])].sort((a, b) => a - b).map((o) => rowsOf(store, o))), store.statics()]);
  return waterGroup(all, statics.ends);
}

/**
 * (T58) The ids of the water `item`'s split-lake group — a closure area's lake's areas, or the lake's own and its areas
 * — itself among them; a water with no closure areas is alone. A water the store does not hold: none. The route keeps
 * this on a frame so a water opened from inside water mode in ANOTHER area of a lake already held is that lake gone back
 * to, never stacked twice (`@app/ui` route `enter`).
 */
export async function siblingItems(store: Pick<RegsReads, "itemOrd" | "siblingOrds" | "item">,
                                   item: string): Promise<readonly string[]> {
  const ord = await store.itemOrd(item);
  if (ord === null) return [];
  const ords = await store.siblingOrds(ord);
  return Promise.all(ords.map(async (o) => (o === ord ? item : (await store.item(o)).id)));
}

/** (T688) A WHOLLY TIDAL water, as its map needs it: the water has no listed part (every piece is tidal, T278), so no group
 *  holds its frame; the release's own rows for its tidal pieces do. */
export interface TidalWater {
  readonly item: string;
  readonly name: string;
  /** the part the Regulations view opens its tidal card on: the first tidal piece */
  readonly partIx: number;
  /** the camera's frame: the union of its tidal pieces' boxes (`geoBounds`), null when none is stored */
  readonly bounds: Bbox | null;
  /** its tidal pieces' sections */
  readonly sids: readonly number[];
}

/** The water `item` as a wholly tidal water, or null: not in the store, or it lists a part, or it has no tidal piece. */
export async function loadTidalWater(store: GroupStore, item: string): Promise<TidalWater | null> {
  const ord = await store.itemOrd(item);
  if (ord === null) throw new RegsInputsError(`water map: no water ${item} in the regs store`);
  const [it, parts, nav] = await Promise.all([store.item(ord), store.parts(ord), store.navRows(ord)]);
  const tidal = parts.filter((p) => p.tidal);
  if (!tidal.length || tidal.length !== parts.length) return null;
  const ix = new Set(tidal.map((p) => p.partIx));
  return { item: it.id, name: it.name, partIx: Math.min(...ix),
           bounds: geoBounds(nav.geo.filter((g) => ix.has(g.partIx))),
           sids: nav.sections.filter((s) => ix.has(s.partIx)).map((s) => s.sid) };
}
