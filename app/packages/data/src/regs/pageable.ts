/**
 * (T530, on T192/T487) WHICH WATERS HAVE A PAGE ON THE SITE, as one rule for the SEO build and the app: a water in the book
 * (shown, in B.C., with a keyed part), with at least one keyed part with facts, a region 1-8, and one part that is not outside
 * B.C. on the day. The site's build (`tools/seo/model.ts` `listWaterOrds` / `waterModel`) and the app's Share on a gauge both
 * read it, so a gauge is offered only where the site has a page for a water it serves.
 */
import { buildRegsPage, type Day, type RegsPage, type Weekday } from "@app/core";
import { loadRegsInputs } from "./inputs";
import type { PartRow } from "./reads";
import type { RegsStore } from "./store";

const ADULT_RESIDENT = { age: "adult", residency: "resident" } as const;

/** The waters in the book a page is made for (before their parts are read): ords, in order. */
export const PAGE_ORDS_SQL = "SELECT i.item_ord AS ord FROM item i WHERE i.shown = 1 AND i.outside_bc = 0 " +
  "AND EXISTS (SELECT 1 FROM part p WHERE p.item_ord = i.item_ord AND p.akey IS NOT NULL AND p.ord IS NOT NULL) ORDER BY i.item_ord";

export interface PageDay { readonly day: Day; readonly weekday: Weekday; readonly year: number }

/** The page the app shows for part `partIx` of a water, for an adult resident (the site's own reader). */
export async function regsPage(store: RegsStore, itemId: string, partIx: number, on: PageDay, today: Day = on.day): Promise<RegsPage> {
  const inp = await loadRegsInputs(store, { kind: "part", item: itemId, partIx }, on.day, on.weekday, on.year);
  return buildRegsPage(inp, { today, who: ADULT_RESIDENT });
}

/** B.C.'s regions (1-8) of the answer keys. */
export async function regionsOfKeys(store: RegsStore, akeys: readonly number[]): Promise<number[]> {
  const w = await store.water();
  const out = new Set<number>();
  for (const a of akeys) for (const r of await w.all("SELECT region FROM akey_region WHERE akey = ?", a)) out.add(parseInt(String(r.region), 10));
  return [...out].filter((n) => n >= 1 && n <= 8).sort((a, b) => a - b);
}

/** A water's parts that carry rules (a key and facts); empty: no page. */
export const liveParts = (parts: readonly PartRow[]): readonly PartRow[] => parts.filter((p) => p.akey !== null && p.facts !== null);

/** Whether the site has a page for the water `item` (the build's rule: in the book, live parts, a region, a part inside B.C.). */
export async function waterHasPage(store: RegsStore, item: string, on: PageDay): Promise<boolean> {
  const ord = await store.itemOrd(item);
  if (ord === null) return false;
  const w = await store.water();
  if (!(await w.all(PAGE_ORDS_SQL)).some((r) => Number(r.ord) === ord)) return false;
  const live = liveParts(await store.parts(ord));
  if (!live.length) return false;
  if (!(await regionsOfKeys(store, live.map((p) => p.akey!))).length) return false;
  for (const p of live) if ((await regsPage(store, item, p.partIx, on)).status !== "outside") return true;
  return false;
}
