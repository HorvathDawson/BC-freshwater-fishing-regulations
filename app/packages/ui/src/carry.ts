/**
 * (T369 D8, owner 2026-11-04) A DATA UPDATE APPLIED WHILE A WATER IS OPEN: "apply right away and keep the same place on
 * the water". A frame's part is an index into ONE release's parts (a re-split moves them), so the open
 * stack is carried into the new release by PLACE, from release reads alone (pure, offline, both platforms):
 *
 *   a named water   its item id (durable, AGENTS rule 5) → the part of the NEW release holding the point the frame
 *                   was on: the tap's point, else its part's middle (`part_geo` `mid`) — the new part whose box holds
 *                   it, the nearest middle among them (none holds it: the nearest)
 *   the tap         dropped (a section handle is not durable): the frame is on its part, and its middle is the reach
 *   a whole water   stays whole (no part to carry)
 *   an unnamed reach (T386, owner 2026-11-04: "yes we want this") by its PLACE: the tapped section's stream key and
 *                   middle measure in the old release (`lines`, phone-held since T386) → the section of the new release
 *                   spanning it (`resolvePlace`) — still unnamed, or now a named water's part (opened on it); a place
 *                   no section spans any more: dropped
 *   a water the new release does not hold: dropped; an empty stack leaves for the map beneath
 */
import type { NavRows, RegsStore } from "@app/data/regs";
import { placeOf, resolvePlace, type ItemFor, type PlaceReads } from "./place";
import type { LatLon, RouteAction, WaterFrame, WaterRoute } from "./route";

type Reads = Pick<RegsStore, "itemOrd" | "navRows">;
type Entry = NavRows["geo"][number];

/** The point a frame is on in the release it was opened in: its tap's, else its part's middle. */
function pointOf(f: WaterFrame, nav: Pick<NavRows, "geo">): LatLon | null {
  if (f.at) return f.at;
  const e = nav.geo.find((x) => x.partIx === f.part);
  return e?.mid ? { lon: e.mid[0], lat: e.mid[1] } : null;
}

/** The part of `nav` at point `p`: the ones whose box holds it, the nearest middle of those (else of all). */
export function entryAt(nav: Pick<NavRows, "geo">, p: LatLon): { part: number } | null {
  const all = nav.geo.filter((e) => e.mid);
  const holds = all.filter((e) => e.box && p.lon >= e.box[0]! && p.lat >= e.box[1]! && p.lon <= e.box[2]! && p.lat <= e.box[3]!);
  const k = Math.cos((p.lat * Math.PI) / 180);
  const d = (e: Entry) => ((e.mid![0] - p.lon) * k) ** 2 + (e.mid![1] - p.lat) ** 2;
  const pool = holds.length ? holds : all;
  if (!pool.length) return null;
  const best = pool.reduce((a, b) => (d(b) < d(a) ? b : a));
  return { part: best.partIx };
}

/** The place reads of both releases and the new one's section → water, for an unnamed reach (T386). */
export interface CarryPlaces { was: PlaceReads; now: PlaceReads; itemFor: ItemFor }

/** An unnamed reach carried by its place: the new release's section there (unnamed, or a named water's part); null:
 *  no section of it spans the place. */
async function carryUnnamed(f: WaterFrame, p: CarryPlaces): Promise<WaterFrame | null> {
  const place = await placeOf(p.was, f);
  const at = place && (await resolvePlace(p.now, p.itemFor, place));
  if (!at) return null;
  return { ...f, item: at.item, part: at.part, tapSid: at.sid as WaterFrame["tapSid"] };
}

/** One frame carried from `was` into `now`; null: it cannot be (a place or a water `now` does not hold). */
export async function carryFrame(f: WaterFrame, was: Reads, now: Reads, places?: CarryPlaces): Promise<WaterFrame | null> {
  if (f.item === null) return f.tapSid !== null && places ? carryUnnamed(f, places) : null;
  const ord = await now.itemOrd(f.item);
  if (ord === null) return null;
  const base: WaterFrame = { ...f, tapSid: null, at: null };
  if (f.part === null) return { ...base, part: null };
  const oldOrd = await was.itemOrd(f.item);
  const p = oldOrd === null ? null : pointOf(f, await was.navRows(oldOrd));
  const at = p ? entryAt(await now.navRows(ord), p) : null;
  if (!at) return { ...base, part: null, ...(f.view === "map" ? { whole: true } : {}) };
  return { ...base, part: at.part };
}

/** THE OPEN STACK CARRIED into the new release: the route's `carry` action (every frame by `carryFrame`). */
export async function carryRoute(r: WaterRoute, was: Reads, now: Reads, places?: CarryPlaces): Promise<RouteAction> {
  const stack = (await Promise.all(r.stack.map((f) => carryFrame(f, was, now, places))))
    .filter((f): f is WaterFrame => f !== null);
  return { type: "carry", stack };
}
