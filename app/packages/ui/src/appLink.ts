/**
 * (T625) A SHARED LINK OPENED BY THE INSTALLED PHONE APP. Android hands the app the https address that was tapped (the
 * intent filter in `apps/mobile/app.json`, verified against `/.well-known/assetlinks.json`). This file reads it, so the
 * phone opens the same water the browser would — and it asks the SAME authority the web's start link does: a water page
 * names its water in `<meta name="cift:item">` (`startLink.web.ts`), so the phone reads that page for the slug's item
 * instead of keeping a second slug table (AGENTS rule 5: a link carries `item_id` or a durable place, never a section id).
 *
 * Read here: `/water/<slug>[?view=]` and the start link `/?regs=<item>[&view=][&part=][&theme=]`, on `APP_LINK_HOSTS`. A bare `/` just opens
 * the app. (T605, T1010) `/?fit=w,s,e,n` — a region page's "Open the map" — opens the map framed on that box (the intent
 * filter takes `/`, so the phone gets it; it was refused as unread and reported before), read by the web's own `fitOf`. Anything else on those hosts (a place `blk`+`m`, `show`, a browse tab…) is a link the phone does not read yet:
 * it throws, naming what it saw, and the caller reports it — it never opens some other page silently.
 */
import { APP_LINK_HOSTS, reportFailure } from "@app/core";
import type { ItemId } from "@app/data";
import type { View } from "./route";
import { fitOf, type FitBox } from "./startLink";

const VIEWS: readonly View[] = ["regs", "conditions", "map"];
const ITEM = /^[a-z]+:-?\d+$/;
/** the keys the phone opens from a link */
const READ_KEYS = new Set(["regs", "view", "part", "theme", "fit"]);
/**
 * (T1038) the web start link's keys the phone does not open yet: said when a link carries one (reported, the water still
 * opens without it) — never read and dropped without a word
 */
const UNSUPPORTED_KEYS = new Set(["date", "horizon", "tab", "sheet", "scroll", "age", "res", "whole"]);

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
  /**
   * (T1034, T1035) what in the link was not opened, each said once by `resolveAppLink` (`reportFailure`): a bad `part` or
   * `theme` (that key alone is dropped, the water opens) and a key the phone does not open yet
   */
  readonly problems: readonly string[];
}

/** What a tapped link opens: a water on a view, or (T605) the map framed on a box. */
export type AppLinkOpen = { readonly item: ItemId; readonly view: View; readonly part: number | null; readonly theme: "light" | "dark" | null }
  | { readonly fit: FitBox };

/** The link's reading, or null when it is not an address of ours (another scheme or host: not a share link). Throws on one of ours the phone cannot read. */
export function parseAppLink(url: string): AppLink | null {
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
  for (const k of q.keys()) if (UNSUPPORTED_KEYS.has(k)) problems.push(`app link: ${k}= is not opened by the phone yet (T1038): dropped`);
  const unread = [...q.keys()].filter((k) => !READ_KEYS.has(k) && !UNSUPPORTED_KEYS.has(k));
  if (unread.length) throw new Error(`app link: ${url} carries ${unread.join(", ")}, which the app does not open yet`);
  const viewS = q.get("view") ?? "regs";
  if (!(VIEWS as readonly string[]).includes(viewS)) throw new Error(`app link: view=${viewS} is not ${VIEWS.join("|")}`);
  const view = viewS as View;
  const regs = q.get("regs") ?? null;
  if (regs !== null && !ITEM.test(regs)) throw new Error(`app link: regs=${regs} is not an item id`);
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
  const fitS = q.get("fit");
  if (fitS === undefined) return { slug, item: regs as ItemId | null, view, part, theme, problems };
  const fit = fitOf(fitS);
  if (typeof fit === "string") throw new Error(`app link: fit=${fitS} is not ${fit}`);
  if (slug || regs) throw new Error(`app link: ${url} names a box beside a water (a link frames one or the other)`);
  return { slug, item: null, view, part, theme, problems, fit };
}

/** The item a water page names (`<meta name="cift:item">`). Throws when it names none: a stale or broken page. */
export function itemOfWaterPage(html: string): ItemId {
  const tag = /<meta\s+name="cift:item"\s+content="([^"]*)"/.exec(html) ?? /<meta\s+content="([^"]*)"\s+name="cift:item"/.exec(html);
  if (!tag || !ITEM.test(tag[1]!)) throw new Error(`app link: the water page names no item (cift:item ${tag ? `"${tag[1]}"` : "missing"})`);
  return tag[1] as ItemId;
}

/**
 * The water a tapped link opens, or null when it opens the app and nothing more (the bare start link). A `/water/<slug>`
 * is read from its own page on the host the link came from; a failed fetch throws (reported by the caller).
 */
export async function resolveAppLink(url: string, fetcher: (u: string) => Promise<Response> = (u) => fetch(u)): Promise<AppLinkOpen | null> {
  const link = parseAppLink(url);
  if (!link) return null;
  for (const p of link.problems) reportFailure(new Error(p));
  if (link.fit) return { fit: link.fit };
  if (link.item) return { item: link.item, view: link.view, part: link.part, theme: link.theme };
  if (!link.slug) return null;
  const page = `${/^https:\/\/[^/?#]+/i.exec(url)![0]}/water/${link.slug}`;
  const r = await fetcher(page);
  if (!r.ok) throw new Error(`app link: ${page} answered ${r.status}`);
  return { item: itemOfWaterPage(await r.text()), view: link.view, part: link.part, theme: link.theme };
}
