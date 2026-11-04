/**
 * T41 — THE START LINK, ON THE WEB ONLY. `?regs=<item>&blk=<stream key>&m=<metres>&view=<view>&date=YYYY-MM-DD&tab=<tab>
 * &sheet=<key>&age=&res=&theme=&scroll=` opens the app straight on one water's Regulations page (or, T221 `view`, its
 * Conditions or Map) — the browser tools (`app/tools/regs-walk.mjs`) put every golden state on screen with it. The app
 * reads it ONCE, at start (`apps/mobile` `startLink.web.ts`; native has none — D1 keeps navigation in the Shell, no
 * route ever ships).
 *
 * (T369, owner 2026-11-03) A LINK ADDRESSES A PLACE, NOT A PART NUMBER: `blk` + `m` (place.ts), the FWA stream key and a
 * distance along it, resolved in the release the app holds (`@app/ui/place` `resolveStartLink`, web-only) to the water and part there — named or
 * not (an unnamed reach's link is `?blk=&m=` alone). A water of one part, a lake and a split lake's closure area need no
 * place: the item is the address (no older link form is read: owner 2026-11-03). `ix=` (+ `stretch=`) is the browser tools' key — a part index in the release the
 * tool was cut against (a test affordance; the app never writes it).
 *
 * It decides nothing: the water opens through `openWater` (the one way in), the day is the Shell's one day, the angler
 * the session's profile (D8), the sheet a `SheetKey` the page opens like any press would. A bad value throws, naming it,
 * so a typo in a test never opens some other page silently.
 */
import { parseSheetKey, type SheetKey, type Tab, type Who } from "@app/core";
import type { ItemId } from "@app/data";
import { openWater } from "./openWater";
import type { StreamPlace } from "@app/data/regs/place";
import { mapDayOf } from "./regs";
import { HOME, reduce, waterFrame, type Route, type View } from "./route";

/** The query keys a route owns (`routeSearch` writes them; the rest — date, angler, theme — are left as they were). `show`
 *  is read once at start and gone at the first write; `stretch` and `ix` are read, never written. */
export const ROUTE_KEYS = ["regs", "blk", "m", "whole", "stretch", "ix", "view", "tab", "sheet", "scroll", "show"] as const;
const VIEWS: readonly View[] = ["regs", "conditions", "map"];

/**
 * (T221, owner 2026-10-28; T369) THE ADDRESS OF WHAT IS ON SCREEN: the water in front as a start link (`?regs=<item>&blk=
 * &m=&view=&whole=1`), so a refresh opens the same water, part and view. `place`: the frame's place (`placeOf`, from the release;
 * null: none needed or none to be had). `current` is the page's query string now: its other keys (date, angler, theme)
 * are kept, the route's replaced. Browsing, or an unnamed reach with no place, says no water — a section id never leaves
 * the bundle (AGENTS 5). Returns the new query string ("" or "?…").
 */
export function routeSearch(r: Route, current = "", place: StreamPlace | null = null): string {
  const q = new URLSearchParams(current);
  for (const k of ROUTE_KEYS) q.delete(k);
  const f = waterFrame(r);
  if (f && (f.item !== null || place)) {
    if (f.item !== null) q.set("regs", f.item);
    if (place) { q.set("blk", String(place.blk)); q.set("m", String(place.m)); }
    if (f.view !== "regs") q.set("view", f.view);
    // (T203, T369) the map framed on the whole water (a search Open's) — said, so a water named alone is its own part
    if (f.whole) q.set("whole", "1");
  }
  const out = q.toString().replace(/%3A/gi, ":");   // an item id's colon reads as typed; a URL query allows it
  return out ? `?${out}` : "";
}

/** Where the Regulations page starts: the tab scrolled to, the sheet open over it, and/or a scroll offset (px). */
export interface RegsStart { tab: Tab; sheet: SheetKey | null; scroll: number }

/**
 * (T256, T345) WHETHER THE MAP'S STATUS WAITS FOR THE PAGE: a start on a water's REGULATIONS page draws the page first
 * and asks the map's status (paint) after the page has applied its start. Only while that page is in front: a start
 * link to the water's Map or Conditions view (`?regs=…&view=map`, a reload of either) has no page to wait for — the
 * page never mounts, its start is never applied, and the held status left the water's map uncoloured (every river
 * "Base rules" blue after a reload of the Coquitlam's Map view, owner 2026-11-03).
 */
export function holdsMapStatus(r: Route, start: RegsStart | null): boolean {
  return start !== null && waterFrame(r)?.view === "regs";
}

export interface StartLink {
  route: Route;
  /** the Shell's one day (null = today) */
  day: Date | null;
  regs: RegsStart;
  /** the angler, or null: the session's default stands */
  who: Who | null;
  theme: "light" | "dark" | null;
  /**
   * (T369) The link's place, still to be resolved in the release (`resolveStartLink`); null: none (`route` is final).
   * While it is set, `route` is the link's water whole (or the map, for a place alone) — what opens if it is not found.
   */
  place: LinkPlace | null;
  /** (T369) What the start must SAY: a place this release does not hold. Null: nothing. */
  notice: StartNotice | null;
  /**
   * (T269, owner 2026-10-29) THE MAP COMES BACK AT THIS WATER: a page that started on a named water (a `/water/<slug>`
   * page, `?regs=`) — or `?show=<item>`, the map at a water, the link a water page's ‹ and × are before the app runs —
   * returns to the map framed on that water (nothing lit: as closing a tapped water leaves it), never to wherever the map
   * last was. Null: none.
   */
  arrive: ItemId | null;
}

/**
 * (T269) A WATER'S ADDRESS: a named water has a page at `/water/<slug>` (the SEO build's committed slug map); the app
 * reads the slug of the water in front from `/water-slugs/<shard>.json`, one of `SLUG_SHARDS` files the build writes,
 * the shard picked by this hash of the item id (FNV-1a), so the app never loads the whole map.
 */
export const SLUG_SHARDS = 64;
export function slugShard(item: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < item.length; i++) { h ^= item.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; }
  return h % SLUG_SHARDS;
}
export const slugShardPath = (item: string): string => `/water-slugs/${slugShard(item)}.json`;
/** The path of the water page `/water/<slug>`; a water's start link otherwise. */
export const WATER_PATH = /^\/water\/([a-z0-9-]+)\/?$/;

/** (T369) Why a start opened less than its link named — said on screen, never a silent guess. */
/** (T369) Whether a start link waits for the release before it opens (a place to resolve, or a water to read the parts of). */
export const startNeedsRelease = (l: StartLink | null): boolean => !!l && (l.place !== null || l.route.mode === "water");

/** (T369) A link's place and the view it asked for (a place-only link's route is the map until resolved). */
export interface LinkPlace extends StreamPlace { readonly view: View }
export type StartNotice = "place-gone";
export const START_NOTICE_WORDS: Readonly<Record<StartNotice, string>> = {
  "place-gone": "The place this link names is not on this version of the map, so the whole water is shown.",
};
/** (T369) The same, for a link to an unnamed reach (no water to show whole). */
export const START_NOTICE_REACH_GONE = "The place this link names is not on this version of the map.";
/** (T369) The line a start says, or null. */
export function startNoticeLine(l: Pick<StartLink, "notice" | "route"> | null): string | null {
  if (!l?.notice) return null;
  return l.notice === "place-gone" && waterFrame(l.route) === null ? START_NOTICE_REACH_GONE : START_NOTICE_WORDS[l.notice];
}

const TABS: readonly Tab[] = ["keep", "gear", "methods", "licence"];
const AGES = ["adult", "u16"] as const;
const RESIDENCIES = ["resident", "non_resident", "non_resident_alien"] as const;

/** The link in a page's query string (`location.search`), or null when it names no water. Throws on a bad value. */
export function parseStartLink(search: string, now: Date = new Date()): StartLink | null {
  const q = new URLSearchParams(search);
  const item = q.get("regs");
  const bad = (k: string, what: string): never => { throw new Error(`start link: ${k}=${q.get(k)} is not ${what}`); };
  const ITEM = /^[a-z]+:-?\d+$/;
  // (T369) the place: both keys or neither
  const blkS = q.get("blk"), mS = q.get("m");
  if ((blkS === null) !== (mS === null)) bad(blkS === null ? "m" : "blk", `a place without ${blkS === null ? "blk" : "m"}`);
  const where: StreamPlace | null = blkS === null ? null
    : { blk: /^-?\d+$/.test(blkS) ? Number(blkS) : bad("blk", "a stream key"),
        m: /^\d+$/.test(mS!) ? Number(mS) : bad("m", "a distance in metres") };
  if (item === null && where === null) {
    // (T269) `?show=<item>`: the map, framed on that water (nothing lit)
    const show = q.get("show");
    if (show === null) return null;
    if (!ITEM.test(show)) bad("show", "an item id");
    return { route: HOME, day: null, regs: { tab: "keep", sheet: null, scroll: 0 }, who: null, theme: null, arrive: show as ItemId,
             place: null, notice: null };
  }
  if (item !== null && !ITEM.test(item)) bad("regs", "an item id");

  const index = (k: string, what: string): number | null => {
    const v = q.get(k);
    return v === null ? null : /^\d+$/.test(v) ? Number(v) : bad(k, what);
  };
  // `ix=` (+ `stretch=`): the browser tools' own part index
  const ix = index("ix", "a part index");
  const stretch = ix === null ? undefined : index("stretch", "a stretch index") ?? undefined;
  const part = where === null ? ix : null;

  const dateS = q.get("date");
  let day: Date | null = null;
  if (dateS !== null) {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(dateS);
    const [y, mo, d] = m ? [Number(m[1]), Number(m[2]), Number(m[3])] : [0, 0, 0];
    const at = new Date(y, mo - 1, d, 12);
    if (!m || at.getMonth() !== mo - 1 || at.getDate() !== d) bad("date", "a YYYY-MM-DD day");
    day = mapDayOf({ month: mo, day: d }, y, now);
  }

  const tabS = q.get("tab") ?? "keep";
  const tab = (TABS as readonly string[]).includes(tabS) ? (tabS as Tab) : bad("tab", "a tab");
  const sheetS = q.get("sheet");
  const sheet = sheetS === null ? null : parseSheetKey(sheetS) ?? bad("sheet", "a sheet key");
  const scrollS = q.get("scroll");
  const scroll = scrollS === null ? 0 : /^\d+$/.test(scrollS) ? Number(scrollS) : bad("scroll", "a scroll offset");

  const age = q.get("age"), res = q.get("res");
  if (age !== null && !(AGES as readonly string[]).includes(age)) bad("age", AGES.join("|"));
  if (res !== null && !(RESIDENCIES as readonly string[]).includes(res)) bad("res", RESIDENCIES.join("|"));
  const who: Who | null = age === null && res === null ? null
    : { age: (age ?? "adult") as Who["age"], residency: (res ?? "resident") as Who["residency"] };

  const viewS = q.get("view");
  const view = viewS === null ? "regs" : (VIEWS as readonly string[]).includes(viewS) ? (viewS as View) : bad("view", VIEWS.join("|"));

  const themeS = q.get("theme");
  const theme = themeS === null ? null : themeS === "light" || themeS === "dark" ? themeS : bad("theme", "light|dark");

  const place: LinkPlace | null = where && { ...where, view };
  // the whole water: asked (`whole=1`)
  const whole = view === "map" && part === null && q.get("whole") === "1";
  const route = item === null ? HOME : reduce(HOME, openWater({ item: item as ItemId, part, view, stretch, whole }));
  const link: StartLink = { route, day, regs: { tab, sheet, scroll }, who, theme, arrive: item as ItemId | null, place,
                            notice: null };
  return link;
}
