/**
 * (T41) The web's start link: the page's query string, read once at start. A bad value is a console error (the browser
 * walk fails on any) and the app opens as usual — never on some other page.
 *
 * (T269) A WATER'S PAGE IS A START LINK TOO: `/water/<slug>` is that water's Regulations page, prerendered by the SEO
 * build with the water's item id in `<meta name="cift:item">`; the app opens it exactly as `?regs=<item>` would (the
 * page's own query — part, view — kept), and knows that water's slug from then on (`waterSlugs.ts`).
 */
import { parseStartLink, setRegsFirstStart, WATER_PATH, type StartLink } from "@app/ui";
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
    // (T256) a start on a Regulations page draws it before the map's paint
    setRegsFirstStart(link?.route.mode === "water");
    return link;
  }
  catch (e) { console.error(e instanceof Error ? e.message : String(e)); return null; }
}
