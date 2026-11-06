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
import { regionOfTile } from "@app/data/release";

const COLOUR = /rgba?\([^)]*\)|#[0-9a-fA-F]{3,8}\b/g;
/** SVG presentation attributes holding a colour: moved into `style`, where a CSS variable works. */
const PAINT_ATTR = /\s(fill|stroke|stop-color|flood-color|lighting-color)="([^"]*)"/g;

/** Every (light, dark) colour pair the build has met, each a CSS variable `--c<n>`; the same pair is one variable everywhere. */
export class ThemeVars {
  /** pair → its name. The name is the pair's own hash (T279: the build's threads name a pair alike without talking). */
  private readonly pairs = new Map<string, string>();
  name(light: string, dark: string): string {
    const k = `${light}|${dark}`;
    let n = this.pairs.get(k);
    if (n === undefined) this.pairs.set(k, (n = `--c${shortHash(k)}`));
    return n;
  }
  get size(): number { return this.pairs.size; }
  /** the pairs met, to merge a thread's into the build's (`add`) */
  keys(): string[] { return [...this.pairs.keys()]; }
  add(keys: readonly string[]): void {
    for (const k of keys) { const [l, d] = k.split("|") as [string, string]; this.name(l, d); }
  }
  /** The variables, light by default and dark under `prefers-color-scheme: dark`. Throws on two pairs with one name. */
  css(): string {
    const all = [...this.pairs].sort((a, b) => a[1].localeCompare(b[1])).map(([k, n]) => { const [l, d] = k.split("|") as [string, string]; return { n, l, d }; });
    if (new Set(all.map((v) => v.n)).size !== all.length) throw new Error("seo: two colour pairs share a variable name");
    return `:root{${all.map((v) => `${v.n}:${v.l}`).join(";")}}` +
      `@media (prefers-color-scheme:dark){:root{${all.map((v) => `${v.n}:${v.d}`).join(";")}}}`;
  }
}

/** FNV-1a, base 36: a short stable name for a key (a colour pair, a layout's style). */
export function shortHash(s: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; }
  return h.toString(36);
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

/**
 * The box the pane stands in: where the app's `#root` is (the window), over it until the hand-over.
 *
 * (T405) WHILE THE PRERENDERED PANE IS UP, THE APP'S ROOT IS DRAWN AT 1 % OF ITS SIZE (a transform, so the app's own
 * layout — its offsets, its onLayout — is untouched). The browser reports a page's largest paint (LCP) and its layout
 * shifts (CLS) from what each element first paints, where it paints it: the app's pane painting under the prerendered one
 * at full size made its first frames count — a block of text larger than any the prerender had (Vedder: LCP 38.9 s, the
 * app's own pane at the hand-over) and the shifts of its first layout (a bar measured, a strip arriving; CLS 0.15-0.72).
 * Drawn at 1 %, those first paints are a few pixels and their shifts nothing; the hand-over then only takes the transform
 * off, which paints no element for the first time and is not a layout shift. Not `visibility:hidden` / `opacity:0`: an
 * element first painted when it is shown counts then, at full size (measured, T405).
 */
export const PRERENDER_CSS = "#prerender{position:fixed;top:0;left:0;right:0;bottom:0;display:flex;z-index:1;background:var(--cift-bg)}" +
  "#prerender~#root{transform:scale(.01);transform-origin:0 0}";

/** (T405) Frames the app's pane must hold still (the same boxes) once it has every part the prerendered pane shows. */
export const SETTLE_FRAMES = 3;
/** (T405) A pane that differs from the prerendered one (a live feed changed a part) is handed over once it has been still this long (ms). */
export const QUIET_MS = 1000;
/** (T405) The longest the hand-over waits once the app's pane is drawn (ms): then it hands over, said in the console. */
export const SETTLE_MAX_MS = 5000;

/**
 * THE HAND-OVER, a module script (deferred; it never blocks the parse). The app's regs pane is the same pane at the same
 * place; the prerendered one goes — and the app's is drawn at full size in the same frame, keeping the reader's scroll —
 * when the app's pane is COMPLETE (T405): it shows every part (data-testid) the prerendered pane shows (the access strip,
 * the salmon row… arrive after its first frame) and has held still for SETTLE_FRAMES frames; or, when a part differs from
 * the build's (a live feed), once it has held still for QUIET_MS; or at the reader's first click / key (the app is up:
 * they get it at once); or after SETTLE_MAX_MS, said in the console. A start on another view (`?view=conditions|map`) has
 * no prerendered twin: it goes at once. So does it when the app has moved off the page.
 */
export const HANDOFF = `<script type="module">` +
  `const p=document.getElementById("prerender"),r=document.getElementById("root"),v=new URLSearchParams(location.search).get("view"),w=location.pathname;` +
  // the drawn one of an id (a prerendered pane holds every layout's; only the window's is drawn)
  `const d=(e,i)=>[...e.querySelectorAll('[data-testid="'+i+'"]')].find(x=>x.offsetParent!==null);` +
  `const ids=e=>new Set([...e.querySelectorAll("[data-testid]")].filter(x=>x.offsetParent!==null).map(x=>x.getAttribute("data-testid")));` +
  `const go=()=>{if(!p.isConnected)return;const a=d(p,"regs-scroll"),b=d(r,"regs-scroll");if(a&&b)b.scrollTop=a.scrollTop;p.remove();o.disconnect()};` +
  // the app's pane's boxes: the scroll's place and size, its content's height, the header's, its parts' count
  `const k=()=>{const b=d(r,"regs-scroll"),h=d(r,"regs-header"),g=d(r,"regs-view");if(!b||!h||!g)return"";const q=b.getBoundingClientRect();return[q.top,q.height,b.scrollHeight,h.offsetHeight,g.querySelectorAll("[data-testid]").length].join()};` +
  `const pv=d(p,"regs-view"),want=pv?[...ids(pv)]:[];let t0=-1,l="",n=0,c=0;` +
  `const f=()=>{if(!p.isConnected)return;if(location.pathname!==w)return go();const g=k(),t=performance.now();if(g!==l){l=g;n=0;c=t}else n++;` +
  `if(g&&n>=${SETTLE_FRAMES}){const rv=d(r,"regs-view"),has=ids(rv);if(want.every(i=>has.has(i))||t-c>=${QUIET_MS})return go()}` +
  `if(t-t0>${SETTLE_MAX_MS}){console.info("hand-over: the app's pane did not settle in ${SETTLE_MAX_MS} ms; handed over");return go()}requestAnimationFrame(f)};` +
  `const o=new MutationObserver(()=>{if(t0>=0)return;if(d(r,"regs-header")||location.pathname!==w){t0=performance.now();requestAnimationFrame(f)}});` +
  // a click on a prerendered link (‹, ×, "Rule book p.N") is left to the link
  `const now=e=>{if(!(e.target.closest&&e.target.closest("a"))&&k())go()};p.addEventListener("click",now,true);addEventListener("keydown",now,true);` +
  `if(v&&v!=="regs")p.remove();else o.observe(r,{childList:true,subtree:true})` +
  `</script>`;


/**
 * (T405) THE APP'S SCRIPT, ASKED FOR AFTER THE FIRST PAINT. As a `defer` tag in the page it is requested while the page
 * parses, before the prerendered pane paints, and Lighthouse's lab model (lantern) then counts its download and run (the
 * 3.5 MB bundle, ~1 MB on the wire) in the page's largest paint even though the pane painted without it. Asked for one frame
 * after the pane's first paint, it starts a frame later (milliseconds) and is not in that paint's chain. A page opened in a
 * background tab (no frames) asks at once. One script only: the export's index (its own chunks are loaded by it).
 */
export const afterPaint = (src: string): string => `<script type="module">` +
  `const g=()=>{if(document.querySelector('script[src="${src}"]'))return;const s=document.createElement("script");s.src="${src}";document.body.append(s)};` +
  `document.visibilityState==="hidden"?g():requestAnimationFrame(()=>setTimeout(g))` +
  `</script>`;

export interface AppDocument {
  /** the page's head: title, description, canonical, social tags, JSON-LD (built by site.ts) */
  readonly head: string;
  /** the inlined critical CSS and the shared pane stylesheet's path */
  readonly critical: string; readonly cssHref: string;
  readonly preload: readonly string[];
  readonly item: string; readonly pane: string;
  /** (T304) the water's bundle region (`WaterModel.focus`): the app focuses the release on it before its first read */
  readonly region: string | null;
}

/**
 * The app's index.html (the web export's) as a water page: its title replaced by the page's head, its render-blocking
 * stylesheets (the map's) loaded without blocking, the "needs JavaScript" line gone (the page reads without it), the pane
 * beside the app's empty root, and the hand-over.
 */
export function appDocument(template: string, d: AppDocument): string {
  let t = template;
  const need = (re: RegExp, what: string) => { if (!re.test(t)) throw new Error(`seo: the app's index.html has no ${what}`); };
  // (T304) every water page names its region: a page without one would land reading the whole core + water files
  if (d.region === null || regionOfTile(d.region) !== d.region) throw new Error(`seo: water ${d.item} names no bundle region (${d.region})`);
  need(/<title>[^<]*<\/title>/, "<title>"); need(/<div id="root"><\/div>/, `<div id="root"></div>`); need(/<\/head>/, "</head>");
  t = t.replace(/<html lang="[^"]*"/, `<html lang="en-CA"`)
    .replace(/<title>[^<]*<\/title>/, "")
    .replace(/<link rel="preload" href="([^"]+\.css)" as="style">/g, "")
    .replace(/<link rel="stylesheet" href="([^"]+\.css)">/g, `<link rel="stylesheet" href="$1" media="print" onload="this.media='all'">`)
    .replace(/<noscript>[\s\S]*?<\/noscript>/, "")
    .replace(/<script src="([^"]+)" defer><\/script>/g, (_, src: string) => afterPaint(src))
    .replace(/<\/head>/, `${d.head}<meta name="cift:item" content="${d.item}"><meta name="cift:region" content="${d.region}">` +
      d.preload.map((f) => `<link rel="preload" href="${f}" as="font" type="font/woff2" crossorigin>`).join("") +
      `<style>${d.critical}</style><link rel="stylesheet" href="${d.cssHref}"></head>`)
    .replace(/<div id="root"><\/div>/, `<div id="prerender">${d.pane}</div><div id="root"></div>${HANDOFF}`);
  return t;
}
