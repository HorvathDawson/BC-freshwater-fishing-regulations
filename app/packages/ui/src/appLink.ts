/**
 * (T625) A SHARED LINK OPENED BY THE INSTALLED PHONE APP. Android hands the app the https address that was tapped (the
 * intent filter in `apps/mobile/app.json`, verified against `/.well-known/assetlinks.json`). This file reads it, so the
 * phone opens the same water the browser would — and it asks the SAME authority the web's start link does: a water page
 * names its water in `<meta name="cift:item">` (`startLink.web.ts`), so the phone reads that page for the slug's item
 * instead of keeping a second slug table (AGENTS rule 5: a link carries `item_id` or a durable place, never a section id).
 *
 * Read here: `/water/<slug>[?view=]` and the start link `/?regs=<item>[&view=][&part=][&theme=]`, on `APP_LINK_HOSTS`.
 * (T1038) and the web start link's `date`, `horizon`, `tab`, `sheet`, `scroll`, `age`, `res` and `whole`, read by the SAME
 * readers `parseStartLink` uses (`linkDay`, `linkPage`, `linkWhole`), so a link lands on the phone where it lands on the web. A bare `/` just opens
 * the app. (T605, T1010) `/?fit=w,s,e,n` — a region page's "Open the map" — opens the map framed on that box (the intent
 * filter takes `/`, so the phone gets it; it was refused as unread and reported before), read by the web's own `fitOf`. Anything else on those hosts (a place `blk`+`m`, `show`, a browse tab…) is a link the phone does not read yet:
 * it throws, naming what it saw, and the caller reports it — it never opens some other page silently.
 */
import { APP_LINK_HOSTS, bcNow, reportFailure, type Horizon, type Who } from "@app/core";
import { isItemId, type ItemId, type StationId } from "@app/data";
import type { View } from "./route";
import { fitOf, linkDay, linkPage, linkWhole, pageStartOn, type FitBox, type LinkBad, type RegsStart } from "./startLink";

const VIEWS: readonly View[] = ["regs", "conditions", "map"];
/** (T1038) the day keys: carried on every link, as the web's start link reads them on every link */
const DAY_KEYS = ["date", "horizon"] as const;
/** (T1038) the water page's keys: carried on a water's link (the web reads them on no other) */
const PAGE_KEYS = ["tab", "sheet", "scroll", "age", "res", "whole"] as const;
/** (T1038) the keys only a water's link opens (its view, part and theme) */
const WATER_KEYS = ["view", "part", "theme"] as const;
/** the keys the phone opens from a link */
const READ_KEYS = new Set<string>(["regs", "view", "part", "theme", "fit", "gauge", ...DAY_KEYS, ...PAGE_KEYS]);

export interface AppLink {
  /** the water page's slug (`/water/<slug>`), or null */
  readonly slug: string | null;
  /** the item the link names itself (`?regs=`), or null */
  readonly item: ItemId | null;
  readonly view: View;
  /** (T1034) `?part=<n>`: the part of the water the link names (0-based, as the web's start link), or null */
  readonly part: number | null;
  /** (T1035) `?theme=light|dark`: the theme the link asks for, or null */
  readonly theme: "light" | "dark" | null;
  /** (T605) `?fit=w,s,e,n`: the map framed on this box, no water. Absent: none. */
  readonly fit?: FitBox;
  /** (T1194) `?gauge=<station>`: that gauge's own page over the Conditions map, no water. Absent: none. */
  readonly gauge?: StationId;
  /** (T1038) the link's day and horizon (`linkDay`), on a water's link or a box's */
  readonly days: LinkDays;
  /** (T1038) where the water's Regulations page starts, who is fishing and the whole water (`linkPage`, `linkWhole`) */
  readonly page: LinkPage;
  /**
   * (T1034, T1035) what in the link was not opened, each said once by `resolveAppLink` (`reportFailure`): a bad `part` or
   * `theme` (that key alone is dropped, the water opens) and (T1038) a key on a link it does nothing on
   */
  readonly problems: readonly string[];
}

/** (T1038) The day a link opens on (null: today), whether the day it named had passed (opened as today, said), and the
 *  Conditions horizon (null: Now) — what the web's start link hands the Shell as `initialDay` / `initialHorizon`. */
export interface LinkDays { readonly day: Date | null; readonly past: boolean; readonly horizon: Horizon | null }
/** (T1038) A water link's page start (`regsStart` on the web), its angler (null: the session's stands) and `whole`. */
/** (T1049) `regs` null: the link is to the water's Map or Conditions, which keeps no page start (`pageStartOn`) */
export interface LinkPage { readonly regs: RegsStart | null; readonly who: Who | null; readonly whole: boolean }

/** What a tapped link opens: a water on a view, or (T605) the map framed on a box. */
export type AppLinkOpen = ({ readonly item: ItemId; readonly view: View; readonly part: number | null; readonly theme: "light" | "dark" | null }
                           & LinkDays & LinkPage)
  | ({ readonly fit: FitBox } & LinkDays)
  /** (T1194) a gauge's page (`/?gauge=<station>`, the web's address): the Conditions map with that gauge's page over it */
  | ({ readonly gauge: StationId } & LinkDays)
  /** (T1038) a link naming a day or horizon and no water or box (`/?date=`): the map on that day, as the web opens it */
  | ({ readonly home: true } & LinkDays);

/** The link's reading, or null when it is not an address of ours (another scheme or host: not a share link). Throws on one of ours the phone cannot read. */
export function parseAppLink(url: string, now: Date = bcNow()): AppLink | null {
  const m = /^https:\/\/([^/?#:]+)(\/[^?#]*)?(?:\?([^#]*))?/i.exec(url);
  if (!m || !APP_LINK_HOSTS.includes(m[1]!.toLowerCase())) return null;
  const path = (m[2] ?? "/").replace(/(.)\/$/, "$1");
  const q = new Map<string, string>();
  for (const kv of (m[3] ?? "").split("&")) {
    if (!kv) continue;
    const i = kv.indexOf("=");
    q.set(decodeURIComponent(i < 0 ? kv : kv.slice(0, i)), i < 0 ? "" : decodeURIComponent(kv.slice(i + 1)));
  }
  const problems: string[] = [];
  const unread = [...q.keys()].filter((k) => !READ_KEYS.has(k));
  if (unread.length) throw new Error(`app link: ${url} carries ${unread.join(", ")}, which the app does not open yet`);
  const viewS = q.get("view") ?? "regs";
  if (!(VIEWS as readonly string[]).includes(viewS)) throw new Error(`app link: view=${viewS} is not ${VIEWS.join("|")}`);
  const view = viewS as View;
  const regs = q.get("regs") ?? null;
  if (regs !== null && !isItemId(regs)) throw new Error(`app link: regs=${regs} is not an item id`);
  const slug = /^\/water\/([a-z0-9-]+)$/.exec(path)?.[1] ?? null;
  if (path !== "/" && !slug) throw new Error(`app link: ${url} is not a water page or the start link`);
  if (slug && regs) throw new Error(`app link: ${url} names a water twice (page and regs=)`);
  const partS = q.get("part");
  // a bad value drops THAT key and says so; the link's water still opens
  const partOk = partS !== undefined && /^\d+$/.test(partS);
  if (partS !== undefined && !partOk) problems.push(`app link: part=${partS} is not a part number: opened without a part`);
  const part = partOk ? Number(partS) : null;
  const themeS = q.get("theme");
  const themeOk = themeS === "light" || themeS === "dark";
  if (themeS !== undefined && !themeOk) problems.push(`app link: theme=${themeS} is not light|dark: the theme is unchanged`);
  const theme = themeOk ? themeS : null;
  // (T1038) the web start link's own readers: a bad value throws, as it does there (the web then opens nothing either)
  const sp = new URLSearchParams([...q]);
  const bad: LinkBad = (k, what) => { throw new Error(`app link: ${k}=${q.get(k)} is not ${what}`); };
  const water = slug !== null || regs !== null;
  const fitS = q.get("fit");
  // the keys a link of this kind does nothing with — on the web too — are said, never dropped without a word
  const idle = (keys: readonly string[], why: string) => {
    for (const k of keys) if (q.has(k)) problems.push(`app link: ${k}= does nothing on a link that ${why}: dropped`);
  };
  if (!water) idle(PAGE_KEYS, "names no water");
  // (T1038 review) and the water's own keys: a box's or a day's link opens the map, no water's view, part or theme
  if (!water) idle(WATER_KEYS, "names no water");
  const { day, past, horizon } = linkDay(sp, bad, now);
  const days: LinkDays = { day, past, horizon };
  const { regs: start, who } = water ? linkPage(sp, bad) : { regs: { tab: "keep" as const, sheet: null, scroll: 0 }, who: null };
  const whole = linkWhole(sp, view, part);
  if (water && q.has("whole") && !whole) problems.push(`app link: whole=${q.get("whole")} frames the whole water only as whole=1 on view=map with no part: dropped`);
  // (T1049) the web's rule: a page start only on the Regulations view; on another its keys are said
  const onView = pageStartOn(view, start, (k) => q.has(k), "app link");
  if (water) problems.push(...onView.dropped);
  const page: LinkPage = { regs: onView.regs, who, whole };
  // (T1194) a gauge's link, as the web's start link reads it: a station id; beside a water or a box it is refused by name
  const gaugeS = q.get("gauge");
  if (gaugeS !== undefined && !/^[0-9A-Za-z]{3,12}$/.test(gaugeS)) throw new Error(`app link: gauge=${gaugeS} is not a station id`);
  if (gaugeS !== undefined && (water || fitS !== undefined)) throw new Error(`app link: ${url} names a gauge beside a water or a box (a link opens one place)`);
  if (gaugeS !== undefined) return { slug, item: null, view, part, theme, days, page, problems, gauge: gaugeS as StationId };
  if (fitS === undefined) return { slug, item: regs as ItemId | null, view, part, theme, days, page, problems };
  const fit = fitOf(fitS);
  if (typeof fit === "string") throw new Error(`app link: fit=${fitS} is not ${fit}`);
  if (water) throw new Error(`app link: ${url} names a box beside a water (a link frames one or the other)`);
  return { slug, item: null, view, part, theme, days, page, problems, fit };
}

/** The item a water page names (`<meta name="cift:item">`). Throws when it names none: a stale or broken page. */
export function itemOfWaterPage(html: string): ItemId {
  const tag = /<meta\s+name="cift:item"\s+content="([^"]*)"/.exec(html) ?? /<meta\s+content="([^"]*)"\s+name="cift:item"/.exec(html);
  if (!tag || !isItemId(tag[1]!)) throw new Error(`app link: the water page names no item (cift:item ${tag ? `"${tag[1]}"` : "missing"})`);
  return tag[1] as ItemId;
}

/**
 * The water a tapped link opens, or null when it opens the app and nothing more (the bare start link). A `/water/<slug>`
 * is read from its own page on the host the link came from; a failed fetch throws (reported by the caller).
 */
export async function resolveAppLink(url: string, fetcher: (u: string) => Promise<Response> = (u) => fetch(u),
                                     now: Date = bcNow()): Promise<AppLinkOpen | null> {
  const link = parseAppLink(url, now);
  if (!link) return null;
  for (const p of link.problems) reportFailure(new Error(p));
  if (link.fit) return { fit: link.fit, ...link.days };
  if (link.gauge) return { gauge: link.gauge, ...link.days };
  const opens = (item: ItemId): AppLinkOpen => ({ item, view: link.view, part: link.part, theme: link.theme, ...link.days, ...link.page });
  if (link.item) return opens(link.item);
  // (T1038) no water and no box: the map on the link's day or horizon (the web's browse start); naming neither, nothing
  if (!link.slug) return link.days.day !== null || link.days.past || link.days.horizon !== null ? { home: true, ...link.days } : null;
  const page = `${/^https:\/\/[^/?#]+/i.exec(url)![0]}/water/${link.slug}`;
  const r = await fetcher(page);
  if (!r.ok) throw new Error(`app link: ${page} answered ${r.status}`);
  return opens(itemOfWaterPage(await r.text()));
}
