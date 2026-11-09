/**
 * (T369, owner 2026-11-03) A LINK ADDRESSES A PLACE ON THE WATER, NOT A PART NUMBER. `part=N` is an index into one
 * release's parts, and a re-split moves it; a place — the FWA stream key and a distance along it (`@app/data`
 * `StreamPlace`) — is FWA's and does not. So a link carries the place the frame is on, made here from the release the
 * app holds, and a link opened is resolved here, in the release the app holds then: the section whose span holds the
 * distance → its water (named or not) → its part. Pure and offline: release reads only, no tiles, no
 * geometry. Inside the app the frame keeps its part index (one release); only the address carries the place.
 *
 * `@app/ui/place`, never the `@app/ui` barrel. Links are the web's (its address bar, `.web.ts`); the phone reads the same
 * places to carry an open unnamed reach across a data update (T386, owner 2026-11-04: `carry.ts`; `lines` phone-held).
 *
 * UNIVERSAL: a named stream's part (the middle of its longest run), an UNNAMED reach (the tapped section's
 * middle; no item), a stream water of one part (no place: the item says it all), a lake and a split lake's closure
 * area (no place: the area's item id is the address — lakes have no route measure).
 *
 * (T388, owner 2026-11-04) A NAMED WATER NEVER FETCHES `lines` (1.2 MB gz). Its places come from the water file's own
 * `part_line` (`@app/data` `PartLine`: each part's stream as runs on its blue lines), which rides in the `water-<region>`
 * slice the page already reads:
 *
 *   writing    the place of a part is the middle of its LONGEST run — a real point on the part's own stream (a side
 *              channel is shorter than the main stem), strictly inside it, so this release resolves it back to the
 *              same part. A tapped frame on a named water says its part's place too (the tap's own point is `here=`,
 *              unchanged). One part: no place (the item is enough).
 *   opening    a named link (`regs=<item>` + blk + m) opens the part of THAT water whose run holds (blk, m); none does:
 *              the "place not on this map" notice (T369 D9) — never a guess, never another water. A tapped link
 *              reopens the tap on the part's midpoint section, at the link's `here=` point.
 *   unnamed    unchanged: the tapped section's middle from `lines`, fetched only when an unnamed reach is on screen or a
 *              link to one is opened.
 */
import { reported, bcNow } from "@app/core";
import type { ItemId, SectionId, WaterSource } from "@app/data";
import type { PartLine, RegsReads } from "@app/data/regs";
import type { ReleaseLoader } from "@app/data/release";
import { lineMid, openLineReads, type LineReads, type StreamPlace } from "@app/data/regs/place";
import { openWater, waterOfSection } from "./openWater";
import { HOME, reduce, waterFrame, type Route, type WaterFrame } from "./route";
import { routeSearch, type AddressState, type StartLink } from "./startLink";

export type { StreamPlace };
/** The regs reads a place is made from: the water's parts and (T388) its part runs. */
type WaterReads = Pick<RegsReads, "itemOrd" | "parts" | "navRows" | "sectionPart" | "partLines">;
/** The open release's regs reads + its `lines` reads (`@app/data/regs/place` `openLineReads`). */
export type PlaceReads = WaterReads & LineReads;

/** The place reads of an open release: its regs store's, and its `lines` file (opened the first time a place needs it:
 *  an unnamed reach). */
export function placeReads(store: WaterReads, loader: Pick<ReleaseLoader, "manifest" | "db">): PlaceReads {
  return { itemOrd: (i) => store.itemOrd(i), parts: (o) => store.parts(o), navRows: (o) => store.navRows(o),
           sectionPart: (o, h) => store.sectionPart(o, h), partLines: (o) => store.partLines(o),
           ...openLineReads(loader) };
}
/** What a section's water is asked of (`openWater.ts` `waterOfSection`, the map tap's own resolver: one rule for both). */
export type ItemFor = Pick<WaterSource, "itemForSection">;

/** The middle of the first of `sids` that is a stream section (a lake's handle has no line); null: none is. Throws when
 *  the release has no lines at all: the frame needs a place the release cannot give (said, never hidden). */
async function midOf(store: PlaceReads, sids: readonly number[]): Promise<StreamPlace | null> {
  if (!(await store.hasLines()))
    throw new Error("place: this release has no section_line, so the part on screen cannot be put in its address");
  for (const s of sids) {
    const l = await store.sectionLine(s);
    if (l) return lineMid(l);
  }
  return null;
}

/** (T388) The middle of the longest of `runs` (the first, of equal ones), in whole metres: inside it (m1 > m0 always:
 *  the pipeline writes no empty run). null: no run (a part with no stream section — a lake). */
function runsMid(runs: readonly PartLine[]): StreamPlace | null {
  let best: PartLine | null = null;
  for (const r of runs) if (!best || r.m1 - r.m0 > best.m1 - best.m0) best = r;
  return best ? { blk: best.blk, m: Math.floor((best.m0 + best.m1) / 2) } : null;
}

/**
 * THE PLACE A FRAME IS ON, for its address; null: the frame needs none (browsing, a whole water, a water of one part,
 * a lake) or the release cannot say (cut before `section_line`). A named water's comes from its `part_line` (T388: no
 * `lines` read); an unnamed reach's from `lines`.
 */
export async function placeOf(store: PlaceReads, f: WaterFrame | null): Promise<StreamPlace | null> {
  if (!f) return null;
  // an UNNAMED reach (always a tap): the tapped section's middle, from `lines`
  if (f.item === null) return f.tapSid !== null ? midOf(store, [f.tapSid]) : null;
  const ord = await store.itemOrd(f.item);
  if (ord === null) return null;
  const runs = await store.partLines(ord);
  // the frame's part: its own, or (a tap whose part is not set) the tapped section's
  const part = f.part ?? (f.tapSid !== null ? await store.sectionPart(ord, f.tapSid) : null);
  if (part === null) return null;
  if ((await store.navRows(ord)).geo.length <= 1) return null;          // one part: the item is enough
  return runsMid(runs.filter((r) => r.partIx === part));
}

/** Where a place falls in this release: the section, its water (null: an unnamed reach) and that water's part. */
export interface PlaceAt { sid: SectionId; item: ItemId | null; part: number | null }

/** THE PLACE RESOLVED in the release held, through `lines` (an unnamed reach's link, and a data update carrying one,
 *  T386); null: no section of this release spans it (or the release has no lines). */
export async function resolvePlace(store: PlaceReads, itemFor: ItemFor, p: StreamPlace): Promise<PlaceAt | null> {
  const h = await store.sectionAt(p.blk, p.m);
  if (h === null) return null;
  const sid = h as SectionId;
  const item = await waterOfSection(itemFor, sid);
  if (item === null) return { sid, item: null, part: null };
  const ord = await store.itemOrd(item);
  const part = ord === null ? null : await store.sectionPart(ord, h);
  return { sid, item, part };
}

/**
 * (T388) A NAMED LINK'S PLACE, RESOLVED in the release held without `lines`: the part of water `item` whose `part_line`
 * run holds it, and that part's midpoint section — the section a tapped link reopens on. null: no run of THAT water
 * holds it (the link's "place not on this map"), or the water is not in this release. `runs`: the water's part runs
 * (`partLines`).
 */
export async function resolvePartPlace(store: PlaceReads, item: ItemId, ord: number, runs: readonly PartLine[],
                                       p: StreamPlace): Promise<PlaceAt | null> {
  const run = runs.find((r) => r.blk === p.blk && r.m0 <= p.m && p.m < r.m1);
  if (!run) return null;
  const mid = (await store.navRows(ord)).geo.find((g) => g.partIx === run.partIx)?.midSid;
  if (mid === undefined) throw new Error(`place: ${item}'s part ${run.partIx} has no part_geo row`);
  return { sid: mid as SectionId, item, part: run.partIx };
}

/** Where a link's place falls: on the link's own water through its `part_line` (T388), or — an unnamed reach's link —
 *  through `lines` (`resolvePlace`). A named link whose water this release does not hold: nowhere (its notice), never
 *  another water. */
async function placeAt(store: PlaceReads, itemFor: ItemFor, item: ItemId | null, p: StreamPlace): Promise<PlaceAt | null> {
  if (item === null) return resolvePlace(store, itemFor, p);
  const ord = await store.itemOrd(item);
  if (ord === null) return null;
  return resolvePartPlace(store, item, ord, await store.partLines(ord), p);
}

/**
 * (T369) THE ADDRESS, MADE FROM THE RELEASE: `routeSearch` with the frame's place. The one function the web's address
 * bar and the tests' reload both call. A frame that needs a place the release cannot give (a release cut before
 * `section_line`) is reported, not hidden (`onMissing`), and addressed without one (its reload opens the whole water).
 */
export async function linkSearch(store: PlaceReads | null, r: Route, current = "",
                                 onMissing?: (e: Error) => void, extra: AddressState | null = null,
                                 now: Date = bcNow()): Promise<string> {
  let place: StreamPlace | null = null;
  if (store) {
    try { place = await placeOf(store, waterFrame(r)); }
    catch (e) { onMissing?.(e instanceof Error ? e : new Error(String(e))); }
  }
  return routeSearch(r, current, place, extra, now);
}

/**
 * (T369) THE LINK'S PLACE, RESOLVED in the release held (`resolvePlace`): the water and part it falls on there, opened
 * the one way (`openWater`) at the link's view. A place this release does not hold opens the link's water whole (or,
 * for a place alone, the map) and says so. A water named with no place, no part and not whole is a water whose address
 * needs no place — one of one part (a lake, a split lake's closure area): it opens at that part.
 */
export async function resolveStartLink(link: StartLink, store: PlaceReads, itemFor: ItemFor,
                                       solo?: (item: ItemId) => Promise<boolean>): Promise<StartLink> {
  if (!link.place) return ownPart(link, store, solo);
  const view = link.place.view;
  const at = await placeAt(store, itemFor, waterFrame(link.route)?.item ?? null, link.place);
  if (!at) {
    const f = waterFrame(link.route);
    return { ...link, place: null, tap: null, notice: "place-gone",
             route: f?.item ? reduce(HOME, openWater({ item: f.item, view, whole: view === "map" })) : link.route };
  }
  // (T369 D5) a tapped frame's link reopens the tap: the section the place falls on, and the point it named
  const tap = link.tap ? { tapSid: at.sid, at: link.tap.at } : {};
  const isSolo = at.item !== null && view === "map" && !!solo && await solo(at.item).catch(reported(false));
  const route = reduce(HOME, openWater(at.item === null
    ? { item: null, tapSid: at.sid, at: link.tap?.at ?? null, view }
    : { item: at.item, part: at.part, view: isSolo ? "regs" : view,
        // (T369 D7) ⤢ is in the address (`whole=1`): the frame comes back whole, on its part
        ...tap, ...(isSolo ? { solo: true } : {}), whole: view === "map" && (at.part === null || !!waterFrame(link.route)?.whole) }));
  return { ...link, route, place: null, tap: null, arrive: at.item ?? link.arrive };
}


/** (T369) A link naming a water alone: a water of one part opens at it; any other — a multi-part water whose link has
 *  no place (a release cut before `section_line` made it) — on its Map, whole. */
async function ownPart(link: StartLink, store: PlaceReads, solo?: (item: ItemId) => Promise<boolean>): Promise<StartLink> {
  const f = waterFrame(link.route);
  if (!f || f.item === null || f.part !== null || f.tapSid !== null) return link;
  // (T369 D13) a Map asked of a solo water opens its Regulations at once (no Map mounted, then dropped)
  const isSolo = f.view === "map" && !!solo && await solo(f.item).catch(reported(false));
  const view = isSolo ? "regs" : f.view;
  if (f.whole && !isSolo) return link;
  const ord = await store.itemOrd(f.item);
  const parts = ord === null ? [] : (await store.parts(ord)).filter((p) => !p.tidal);
  if (parts.length === 1) {
    // (T369 D5) a tapped lake (no line, no place): its one part's section, when it has one, is the tap; (T388) a tapped
    // stream of one part carries no place either: the tap reopens on its midpoint section, at `here=`
    const nav = link.tap && ord !== null ? await store.navRows(ord) : null;
    const sid = nav?.geo.find((g) => g.partIx === parts[0]!.partIx)?.midSid;
    if (nav && sid === undefined) throw new Error(`place: ${f.item}'s part ${parts[0]!.partIx} has no part_geo row`);
    const tap = sid !== undefined ? { tapSid: sid as SectionId, at: link.tap!.at } : {};
    return { ...link, tap: null, route: reduce(HOME, openWater({ item: f.item, part: parts[0]!.partIx, view, ...tap,
                                                                  ...(isSolo ? { solo: true } : {}) })) };
  }
  return f.view === "map" ? { ...link, route: reduce(HOME, openWater({ item: f.item, view: "map", whole: true })) } : link;
}
