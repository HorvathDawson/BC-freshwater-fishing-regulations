/**
 * T41 — THE START LINK, ON THE WEB ONLY. `?regs=<item>&part=<i>&view=<view>&date=YYYY-MM-DD&tab=<tab>&sheet=<key>
 * &age=&res=&theme=&scroll=` opens the app straight on one water's Regulations page (or, T221 `view`, its Conditions or
 * Map) — the browser tools
 * (`app/tools/regs-walk.mjs`) put every golden state on screen with it. The app reads it ONCE, at
 * start (`apps/mobile` `startLink.web.ts`; native has none — D1 keeps navigation in the Shell, no route ever ships).
 *
 * It decides nothing: the water opens through `openWater` (the one way in), the day is the Shell's one day, the angler
 * the session's profile (D8), the sheet a `SheetKey` the page opens like any press would. A bad value throws, naming it,
 * so a typo in a test never opens some other page silently.
 */
import { parseSheetKey, type SheetKey, type Tab, type Who } from "@app/core";
import type { ItemId } from "@app/data";
import { openWater } from "./openWater";
import { mapDayOf } from "./regs";
import { HOME, reduce, waterFrame, type Route, type View } from "./route";

/** The query keys a route owns (`routeSearch` writes them; the rest — date, angler, theme — are left as they were). */
export const ROUTE_KEYS = ["regs", "part", "view", "tab", "sheet", "scroll"] as const;
const VIEWS: readonly View[] = ["regs", "conditions", "map"];

/**
 * (T221, owner 2026-10-28) THE ADDRESS OF WHAT IS ON SCREEN: the water in front as a start link (`?regs=<item>&part=
 * &view=`), so a refresh opens the same water, part and view. `current` is the page's query string now: its other keys
 * (date, angler, theme) are kept, the route's replaced. Browsing, or an unnamed reach (no item: its section id must
 * never leave the bundle, AGENTS 5), says no water. Returns the new query string ("" or "?…").
 */
export function routeSearch(r: Route, current = ""): string {
  const q = new URLSearchParams(current);
  for (const k of ROUTE_KEYS) q.delete(k);
  const f = waterFrame(r);
  if (f && f.item !== null) {
    q.set("regs", f.item);
    if (f.part !== null) q.set("part", String(f.part));
    if (f.view !== "regs") q.set("view", f.view);
  }
  const out = q.toString().replace(/%3A/gi, ":");   // an item id's colon reads as typed; a URL query allows it
  return out ? `?${out}` : "";
}

/** Where the Regulations page starts: the tab scrolled to, the sheet open over it, and/or a scroll offset (px). */
export interface RegsStart { tab: Tab; sheet: SheetKey | null; scroll: number }

export interface StartLink {
  route: Route;
  /** the Shell's one day (null = today) */
  day: Date | null;
  regs: RegsStart;
  /** the angler, or null: the session's default stands */
  who: Who | null;
  theme: "light" | "dark" | null;
}

const TABS: readonly Tab[] = ["keep", "gear", "methods", "licence"];
const AGES = ["adult", "u16"] as const;
const RESIDENCIES = ["resident", "non_resident", "non_resident_alien"] as const;

/** The link in a page's query string (`location.search`), or null when it names no water. Throws on a bad value. */
export function parseStartLink(search: string, now: Date = new Date()): StartLink | null {
  const q = new URLSearchParams(search);
  const item = q.get("regs");
  if (item === null) return null;
  const bad = (k: string, what: string): never => { throw new Error(`start link: ${k}=${q.get(k)} is not ${what}`); };
  if (!/^[a-z]+:-?\d+$/.test(item)) bad("regs", "an item id");

  const partS = q.get("part");
  const part = partS === null ? null : /^\d+$/.test(partS) ? Number(partS) : bad("part", "a part index");

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

  return { route: reduce(HOME, openWater({ item: item as ItemId, part, view, whole: view === "map" && part === null })), day, regs: { tab, sheet, scroll }, who, theme };
}
