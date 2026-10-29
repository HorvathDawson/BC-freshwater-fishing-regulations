/**
 * T269 — THE APP PAGE AROUND A PRERENDERED PANE: string work only (no React). `pane.tsx` renders the app's own Shell; this
 * file turns those renders into one document:
 *
 *   themes     the light and the dark render of one layout are the same markup but for their colours (checked here, tag by
 *              tag); each colour pair becomes a CSS variable, so ONE markup paints either theme by `prefers-color-scheme`,
 *              as the app picks its theme (App.tsx: the system's scheme). A pair of renders that differ in anything else
 *              is refused (null) and the caller ships both.
 *   layouts    phone / tablet / desktop renders made one tree, switched by the window's width (layouts.ts).
 *   back       the header's ‹ and × become plain links to the map at this water (`/?show=<item>`), so they work before the
 *              app's script has run; once it has, the app's own pane (same place, same look) replaces this one.
 *   document   the app's own index.html (the web export's: its script, its reset) with the page's head and the pane in it.
 */
import type { LayoutClass } from "@app/ui";

const COLOUR = /rgba?\([^)]*\)|#[0-9a-fA-F]{3,8}\b/g;
/** SVG presentation attributes holding a colour: moved into `style`, where a CSS variable works. */
const PAINT_ATTR = /\s(fill|stroke|stop-color|flood-color|lighting-color)="([^"]*)"/g;

/** Every (light, dark) colour pair the build has met, each a CSS variable `--c<n>`; the same pair is one variable everywhere. */
export class ThemeVars {
  private readonly pairs = new Map<string, number>();
  name(light: string, dark: string): string {
    const k = `${light}|${dark}`;
    let n = this.pairs.get(k);
    if (n === undefined) this.pairs.set(k, (n = this.pairs.size));
    return `--c${n}`;
  }
  get size(): number { return this.pairs.size; }
  /** The variables, light by default and dark under `prefers-color-scheme: dark`. */
  css(): string {
    const all = [...this.pairs].map(([k, n]) => { const [l, d] = k.split("|") as [string, string]; return { n, l, d }; });
    return `:root{${all.map((v) => `--c${v.n}:${v.l}`).join(";")}}` +
      `@media (prefers-color-scheme:dark){:root{${all.map((v) => `--c${v.n}:${v.d}`).join(";")}}}`;
  }
}

const split = (html: string): string[] => html.split(/(<[^>]+>)/);

/**
 * One markup from the light and the dark render of the same tree, or null when they differ in anything but colours.
 * A colour the themes share stays as it is; one they do not is `var(--c<n>)`.
 */
export function mergeThemes(light: string, dark: string, vars: ThemeVars): string | null {
  const a = split(light), b = split(dark);
  if (a.length !== b.length) return null;
  const out: string[] = [];
  for (let i = 0; i < a.length; i++) {
    const x = a[i]!, y = b[i]!;
    if (!x.startsWith("<")) { if (x !== y) return null; out.push(x); continue; }
    if (x === y) { out.push(x); continue; }
    if (x.replace(COLOUR, "§") !== y.replace(COLOUR, "§")) return null;
    const cy = y.match(COLOUR)!;
    let j = 0;
    const merged = x.replace(COLOUR, (c) => { const d = cy[j++]!; return c === d ? c : `var(${vars.name(c, d)})`; });
    out.push(paintToStyle(merged));
  }
  return out.join("");
}

/** `fill="var(--c3)"` → `style="fill:var(--c3)"` (a presentation attribute cannot read a variable everywhere; a style can). */
function paintToStyle(tag: string): string {
  const moved: string[] = [];
  let t = tag.replace(PAINT_ATTR, (all, k: string, v: string) => (v.includes("var(") ? (moved.push(`${k}:${v}`), "") : all));
  if (!moved.length) return t;
  if (/\sstyle="/.test(t)) t = t.replace(/\sstyle="([^"]*)"/, (_, s: string) => ` style="${s}${s && !s.endsWith(";") ? ";" : ""}${moved.join(";")}"`);
  else t = t.replace(/\s*(\/?)>$/, (_, sl: string) => ` style="${moved.join(";")}"${sl}>`);
  return t;
}

/**
 * The element whose `data-testid` is `id` (its opening tag to its matching close) as a link to `href`: the tag becomes
 * `a`, loses its button role, and draws no underline. Absent → the markup unchanged.
 */
export function asLink(html: string, id: string, href: string, from = 0): string {
  const at = html.indexOf(`data-testid="${id}"`, from);
  if (at < 0) return html;
  const open = html.lastIndexOf("<", at);
  const m = /^<(\w+)/.exec(html.slice(open));
  if (!m) return html;
  const name = m[1]!;
  // the matching close: count this element's own kind of tag from the opening one
  const re = new RegExp(`<(/?)${name}\\b[^>]*?(/?)>`, "g");
  re.lastIndex = open;
  let depth = 0, close = -1, closeLen = 0;
  for (let t = re.exec(html); t; t = re.exec(html)) {
    if (t[2]) continue;                    // self-closing
    depth += t[1] ? -1 : 1;
    if (depth === 0) { close = t.index; closeLen = t[0].length; break; }
  }
  if (close < 0) return html;
  const tagEnd = html.indexOf(">", at) + 1;
  let openTag = html.slice(open, tagEnd).replace(/^<\w+/, `<a href="${href}"`).replace(/\srole="button"/, "");
  openTag = /\sstyle="/.test(openTag)
    ? openTag.replace(/\sstyle="([^"]*)"/, (_, s: string) => ` style="${s}${s && !s.endsWith(";") ? ";" : ""}text-decoration:none"`)
    : openTag.replace(/>$/, ` style="text-decoration:none">`);
  const done = html.slice(0, open) + openTag + html.slice(tagEnd, close) + "</a>";
  // every drawing of it (a layout may draw its own)
  return asLink(done + html.slice(close + closeLen), id, href, done.length);
}

/** The map at this water: the start link the app reads (`@app/ui` `parseStartLink` `show`). */
export const mapHref = (item: string): string => `/?show=${encodeURIComponent(item).replace(/%3A/gi, ":")}`;

/** The header's ‹ and × as links to the map at this water (the app's own controls take over once it runs). */
export function linkHeader(html: string, item: string): string {
  return asLink(asLink(html, "water-back", mapHref(item)), "water-close", mapHref(item));
}

export const LAYOUTS: readonly LayoutClass[] = ["phone", "tablet", "desktop"];

/** The box the pane stands in: where the app's `#root` is (the window), over it until the hand-over. */
export const PRERENDER_CSS = "#prerender{position:fixed;top:0;left:0;right:0;bottom:0;display:flex;z-index:1;background:var(--cift-bg)}";

/**
 * THE HAND-OVER, a module script (deferred; it never blocks the parse): when the app has drawn its own regs pane — the
 * same pane, at the same place — the prerendered one goes, keeping the reader's scroll. A start on another view
 * (`?view=conditions|map`) has no prerendered twin: it goes at once. So does it when the app has moved off the page.
 */
export const HANDOFF = `<script type="module">` +
  `const p=document.getElementById("prerender"),r=document.getElementById("root"),v=new URLSearchParams(location.search).get("view"),w=location.pathname;` +
  `const s=e=>[...e.querySelectorAll('[data-testid="regs-scroll"]')].find(x=>x.offsetParent!==null);` +
  `const go=()=>{if(!p.isConnected)return;const a=s(p),b=s(r);if(a&&b)b.scrollTop=a.scrollTop;p.remove();o.disconnect()};` +
  `const o=new MutationObserver(()=>{if(r.querySelector('[data-testid="regs-header"]')||location.pathname!==w)requestAnimationFrame(go)});` +
  `if(v&&v!=="regs")p.remove();else o.observe(r,{childList:true,subtree:true})` +
  `</script>`;


export interface AppDocument {
  /** the page's head: title, description, canonical, social tags, JSON-LD (built by site.ts) */
  readonly head: string;
  /** the inlined critical CSS and the shared pane stylesheet's path */
  readonly critical: string; readonly cssHref: string;
  readonly preload: readonly string[];
  readonly item: string; readonly pane: string;
}

/**
 * The app's index.html (the web export's) as a water page: its title replaced by the page's head, its render-blocking
 * stylesheets (the map's) loaded without blocking, the "needs JavaScript" line gone (the page reads without it), the pane
 * beside the app's empty root, and the hand-over.
 */
export function appDocument(template: string, d: AppDocument): string {
  let t = template;
  const need = (re: RegExp, what: string) => { if (!re.test(t)) throw new Error(`seo: the app's index.html has no ${what}`); };
  need(/<title>[^<]*<\/title>/, "<title>"); need(/<div id="root"><\/div>/, `<div id="root"></div>`); need(/<\/head>/, "</head>");
  t = t.replace(/<html lang="[^"]*"/, `<html lang="en-CA"`)
    .replace(/<title>[^<]*<\/title>/, "")
    .replace(/<link rel="preload" href="([^"]+\.css)" as="style">/g, "")
    .replace(/<link rel="stylesheet" href="([^"]+\.css)">/g, `<link rel="stylesheet" href="$1" media="print" onload="this.media='all'">`)
    .replace(/<noscript>[\s\S]*?<\/noscript>/, "")
    .replace(/<\/head>/, `${d.head}<meta name="cift:item" content="${d.item}">` +
      d.preload.map((f) => `<link rel="preload" href="${f}" as="font" type="font/ttf" crossorigin>`).join("") +
      `<style>${d.critical}</style><link rel="stylesheet" href="${d.cssHref}"></head>`)
    .replace(/<div id="root"><\/div>/, `<div id="prerender">${d.pane}</div><div id="root"></div>${HANDOFF}`);
  return t;
}
