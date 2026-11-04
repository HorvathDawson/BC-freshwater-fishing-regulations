/**
 * (T369 D8, owner 2026-11-04) A DATA UPDATE APPLIED WHILE A WATER IS OPEN: "apply right away and keep the same place on
 * the water". A frame's part and stretch are indexes into ONE release's parts (a re-split moves them), so the open
 * stack is carried into the new release by PLACE, from release reads alone (pure, offline, both platforms):
 *
 *   a named water   its item id (durable, AGENTS rule 5) → the part / stretch of the NEW release holding the point the
 *                   frame was on: the tap's point, else its part's / stretch's middle (`part_geo` / `part_stretch` `mid`)
 *                   — the new entry whose box holds it, the nearest middle among them (none holds it: the nearest)
 *   the tap         dropped (a section handle is not durable): the frame is on its part, and its middle is the reach
 *   a whole water   stays whole (no part to carry)
 *   an unnamed reach, or a water the new release does not hold: dropped — no release read on a phone can say where
 *                   a section went (the web's `lines` file is web-only); an empty stack leaves for the map beneath
 */
import type { NavRows, RegsStore } from "@app/data/regs";
import type { LatLon, RouteAction, WaterFrame, WaterRoute } from "./route";

type Reads = Pick<RegsStore, "itemOrd" | "navRows">;
type Entry = { partIx: number; stretch: number; box: readonly number[] | null; mid: readonly [number, number] | null };

const entries = (nav: Pick<NavRows, "geo" | "stretches">): Entry[] => {
  const split = new Set(nav.stretches.map((s) => s.partIx));
  const out: Entry[] = nav.stretches.map((s) => ({ partIx: s.partIx, stretch: s.stretch, box: s.box, mid: s.mid }));
  for (const g of nav.geo) if (!split.has(g.partIx)) out.push({ partIx: g.partIx, stretch: 0, box: g.box, mid: g.mid });
  return out;
};

/** The point a frame is on in the release it was opened in: its tap's, else its part's / stretch's middle. */
function pointOf(f: WaterFrame, nav: Pick<NavRows, "geo" | "stretches">): LatLon | null {
  if (f.at) return f.at;
  const e = entries(nav).find((x) => x.partIx === f.part && x.stretch === (f.stretch ?? 0))
    ?? entries(nav).find((x) => x.partIx === f.part);
  return e?.mid ? { lon: e.mid[0], lat: e.mid[1] } : null;
}

/** The entry of `nav` at point `p`: the ones whose box holds it, the nearest middle of those (else of all). */
export function entryAt(nav: Pick<NavRows, "geo" | "stretches">, p: LatLon): { part: number; stretch: number } | null {
  const all = entries(nav).filter((e) => e.mid);
  const holds = all.filter((e) => e.box && p.lon >= e.box[0]! && p.lat >= e.box[1]! && p.lon <= e.box[2]! && p.lat <= e.box[3]!);
  const k = Math.cos((p.lat * Math.PI) / 180);
  const d = (e: Entry) => ((e.mid![0] - p.lon) * k) ** 2 + (e.mid![1] - p.lat) ** 2;
  const pool = holds.length ? holds : all;
  if (!pool.length) return null;
  const best = pool.reduce((a, b) => (d(b) < d(a) ? b : a));
  return { part: best.partIx, stretch: best.stretch };
}

/** One frame carried from `was` into `now`; null: it cannot be (an unnamed reach, a water `now` does not hold). */
export async function carryFrame(f: WaterFrame, was: Reads, now: Reads): Promise<WaterFrame | null> {
  if (f.item === null) return null;
  const ord = await now.itemOrd(f.item);
  if (ord === null) return null;
  const base: WaterFrame = { ...f, tapSid: null, at: null };
  if (f.part === null) return { ...base, part: null, stretch: undefined };
  const oldOrd = await was.itemOrd(f.item);
  const p = oldOrd === null ? null : pointOf(f, await was.navRows(oldOrd));
  const at = p ? entryAt(await now.navRows(ord), p) : null;
  if (!at) return { ...base, part: null, stretch: undefined, ...(f.view === "map" ? { whole: true } : {}) };
  return { ...base, part: at.part, stretch: at.stretch || undefined };
}

/** THE OPEN STACK CARRIED into the new release: the route's `carry` action (every frame by `carryFrame`). */
export async function carryRoute(r: WaterRoute, was: Reads, now: Reads): Promise<RouteAction> {
  const stack = (await Promise.all(r.stack.map((f) => carryFrame(f, was, now)))).filter((f): f is WaterFrame => f !== null);
  return { type: "carry", stack };
}
