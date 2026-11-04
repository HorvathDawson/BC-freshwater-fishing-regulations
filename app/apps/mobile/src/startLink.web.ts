/**
 * (T41) The web's start link: the page's query string, read once at start. A bad value is a console error (the browser
 * walk fails on any) and the app opens as usual — never on some other page.
 *
 * (T269) A WATER'S PAGE IS A START LINK TOO: `/water/<slug>` is that water's Regulations page, prerendered by the SEO
 * build with the water's item id in `<meta name="cift:item">`; the app opens it exactly as `?regs=<item>` would (the
 * page's own query — part, view — kept), and knows that water's slug from then on (`waterSlugs.ts`).
 */
import type { WaterSource } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import type { RegsStore } from "@app/data/regs";
import { parseStartLink, regsSoloOf, setRegsFirstStart, WATER_PATH, type StartLink } from "@app/ui";
import { placeReads, resolveStartLink } from "@app/ui/place";
import { preloadParts } from "@app/ui-native";
import { slugSeen } from "./waterSlugs";

function search(): string {
  const { pathname, search: q } = window.location;
  const slug = WATER_PATH.exec(pathname)?.[1];
  const item = slug ? document.querySelector('meta[name="cift:item"]')?.getAttribute("content") ?? null : null;
  if (!slug || !item) return q;
  slugSeen(item, slug);
  const p = new URLSearchParams(q);
  if (!p.has("regs")) p.set("regs", item);
  return `?${p.toString()}`;
}

export function startLink(): StartLink | null {
  try {
    const link = parseStartLink(search());
    // (T256) a start on a Regulations page draws it before the map's paint — (T369) an unnamed reach's place too (its
    // route is the map until the place is resolved in the release)
    const water = link?.route.mode === "water" || !!link?.place;
    setRegsFirstStart(water);
    // (T282) the map's code is in the later chunk (`@app/ui-native` later.ts): any start but a water's own page asks for
    // it now, beside the release's first files; a water's page is handed to the app without it (fetched once idle)
    if (!water) void preloadParts();
    return link;
  }
  catch (e) { console.error(e instanceof Error ? e.message : String(e)); return null; }
}

/** (T369) The start link resolved in the open release (`@app/ui/place`): only a link that carries a place opens `lines`. */
export function resolveStart(link: StartLink, store: RegsStore, loader: ReleaseLoader,
                             itemFor: Pick<WaterSource, "itemForSection">): Promise<StartLink> {
  // (T369 D13) a Map asked of a solo water opens its Regulations straight away (the opener's own question)
  return resolveStartLink(link, placeReads(store, loader), itemFor, regsSoloOf(loader));
}
