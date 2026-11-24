#!/usr/bin/env node
/**
 * (T844, owner 2026-11-17: "loading on webapp should use the new one too like the app", "Opening loader only") THE WEB'S
 * OPENING IS THE COHO. A web export's index.html is an empty `#root` until the app's script (3.8 MB, ~1 MB on the wire)
 * has arrived and run: on a slow line the page sat white for seconds, then the start screen's words without their coho
 * while its sheet (0.9 MB) came down. This puts the coho's frame 0 into the HTML itself, at the start screen's size and
 * place (`COHO_WIDTH` wide, `COHO_GROUND_PAD` of ground, the window's centre: App.tsx `BootScreen`), so the coho is the
 * first thing painted — as the phone's launch picture is frame 0 (T843) — and the app's own coho takes over where it stands.
 *
 *   node tools/web-shell.mjs <export dir>     (chained after `expo export --platform web` in apps/mobile's export:web*)
 *
 * THE STATES (an attribute on <html>, `data-boot-coho`; no attribute: the picture shown over the whole window):
 *   (none) the app has drawn nothing of its own (no testID in #root: not run yet, or only its providers' empty boxes —
 *          measured: an empty-root rule alone left a white frame before the start screen);
 *   over   the start screen (`boot-screen`) is up but its coho's sheet is not in yet (the app's coho draws nothing
 *          until then): frame 0 stays over its coho box, the start screen's words show;
 *   gone   the app's sheet is in, or the app drew anything else: the picture is removed and the watching stops.
 * Frame 0 only, never the sheet: the shell asks for nothing the app would not. Measured on Fast 3G: the shell asking for
 * the then 0.9 MB sheet itself (to step it before the app was up) put the map ~0.7 s later, the sheet taking the line
 * from the release files. Frame 0 (`web-frame0-<theme>.png`, ~10 KB, `pnpm splash`) is the web's own sheet's
 * (`cohoWeb.generated.ts`, ~105 KB since the owner's "make way smaller for web", ~146 KB at T852's sharper frame), so
 * the hand-over is the same picture. Light or dark by the system's scheme, as the start screen reads it. Without script
 * the page says it needs one, uncovered (`<noscript>`).
 * The water pages (tools/seo `appDocument`) take it out: their prerendered pane is the first paint there.
 *
 * (T910, owner's Lighthouse run 2026-11-19: mobile 46, FCP 7.1 s) THE FIRST PAINT NEVER WAITS FOR THE APP'S SCRIPT. The
 * coho was the first paint only when its frame (then a file asked for once the map's stylesheets were in) landed and was
 * drawn before the 1.1 MB script arrived and ran; when the script won, nothing was drawn until it had run (~1 s of main
 * thread on a slow phone) and Lighthouse's mobile run read FCP/LCP 6-7.5 s instead of 1.7 s: measured on production and
 * staging alike, one run in three. So: frame 0 is IN the page (a data: URI, ~10 KB each scheme: no request), the map's two
 * stylesheets load without holding the paint (preloaded, applied when in: the map is drawn long after), and the app's
 * script downloads at once (a preload) but runs only after the first frame is up (`afterPaint`, the water pages' own
 * hand-over, T269). And the faces (`shellFonts`): the web app draws the WOFF2 files at `/assets/fonts/` (apps/mobile
 * `fonts.web.ts`), copied in here from `tools/seo/fonts/`, checked against `fonts.json` and against the bundle.
 */
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { withIconLinks } from "./web-icons.mjs";
import { FONT_DIR, FONTS_RECORD, servedPath } from "./emit-web-fonts.mjs";

const here = (p) => fileURLToPath(new URL(p, import.meta.url));
export const THEMES = ["light", "dark"];
/** What the water pages strip, and what makes a second run a no-op. */
export const MARK = { open: "<!--boot-coho-->", close: "<!--/boot-coho-->" };
export const MARKED = /<!--boot-coho-->[\s\S]*?<!--\/boot-coho-->/g;

/** The coho's facts from its sources: the generated module (the frame), `coho.ts` (the size), the theme tokens. */
export function cohoFacts() {
  const gen = readFileSync(here("../packages/ui-native/src/coho.generated.ts"), "utf8");
  const ts = readFileSync(here("../packages/ui-native/src/coho.ts"), "utf8");
  const num = (src, re, what) => {
    const v = Number(re.exec(src)?.[1]);
    if (!Number.isFinite(v) || v <= 0) throw new Error(`web-shell: cannot read ${what}`);
    return v;
  };
  const colour = (t, key) => {
    const vals = JSON.parse(readFileSync(here(`../packages/map/style/themes/${t}.json`), "utf8")).values;
    let v = vals[key];
    for (let i = 0; typeof v === "string" && v.startsWith("@") && i < 5; i++) v = vals[v.slice(1)];
    if (typeof v !== "string" || !/^#[0-9A-Fa-f]{6}$/.test(v)) throw new Error(`web-shell: ${t} ${key} is not a colour`);
    return v;
  };
  return {
    frame: { width: num(gen, /frame:\s*\{\s*width:\s*(\d+)/, "frame width"), height: num(gen, /frame:\s*\{[^}]*height:\s*(\d+)/, "frame height") },
    width: num(ts, /COHO_WIDTH\s*=\s*(\d+)/, "COHO_WIDTH"), pad: num(ts, /COHO_GROUND_PAD\s*=\s*(\d+)/, "COHO_GROUND_PAD"),
    card: Object.fromEntries(THEMES.map((t) => [t, colour(t, "color.ui.card")])),
    ground: Object.fromEntries(THEMES.map((t) => [t, colour(t, "color.ui.coho.ground")])),
  };
}

/**
 * The shell: `head` (its style) and `body` (the picture and its watcher, placed right after `#root`), each between the
 * marks. `frame0` is the picture's URL per theme (T910: its data: URI, `frame0Uri`).
 */
export function bootCoho(f, { frame0 }) {
  const w = f.width, h = (w * f.frame.height) / f.frame.width;
  const theme = (t) => `#boot-coho{background:${f.card[t]}}#boot-coho .g{background:${f.ground[t]}}` +
    `#boot-coho .f0{background-image:url(${frame0[t]})}`;
  const css =
    `#boot-coho{position:fixed;inset:0;z-index:1;pointer-events:none}[data-boot-coho=over] #boot-coho{background:none}` +
    `#boot-coho .b{position:absolute;left:50%;top:50%;width:${w}px;height:${h}px;margin:${-h / 2}px 0 0 ${-w / 2}px}` +
    `#boot-coho .g{position:absolute;inset:${-f.pad}px}` +
    `#boot-coho .f0{position:absolute;inset:0;background-size:100% 100%}` +
    theme("light") + `@media (prefers-color-scheme:dark){${theme("dark")}}`;
  // A module script: it never holds the page's parse up (a classic one waits for the map's stylesheet first), and runs
  // before the app's deferred script (document order), so it watches from the app's first commit. The app's coho is
  // in when its Image's <img> (react-native-web draws one from the start of the load) is complete.
  const watch =
    `<script type="module">{const d=document.documentElement,r=document.getElementById("root"),p=document.getElementById("boot-coho");` +
    `const end=()=>{o.disconnect();p.remove();d.setAttribute("data-boot-coho","gone")};` +
    `const look=()=>{if(!p.isConnected||!r.querySelector("[data-testid]"))return;` +
    `if(!r.querySelector('[data-testid="boot-screen"]'))return end();` +
    `const i=r.querySelector('[data-testid="coho-sheet"] img');if(i&&i.complete&&i.naturalWidth>0)return end();` +
    `if(i&&!i.dataset.bootCoho){i.dataset.bootCoho="1";i.addEventListener("load",look)}` +
    `if(d.getAttribute("data-boot-coho")!=="over")d.setAttribute("data-boot-coho","over")};` +
    `const o=new MutationObserver(look);o.observe(r,{childList:true,subtree:true});look()}</script>`;
  return {
    head: `${MARK.open}<style id="boot-coho-css">${css}</style><noscript><style>#boot-coho{display:none}</style></noscript>${MARK.close}`,
    body: `${MARK.open}<div id="boot-coho" aria-hidden="true"><div class="b"><div class="g"></div><div class="f0"></div></div></div>` +
      `${watch}${MARK.close}`,
  };
}

/** The export's index.html with the shell in: the style before `</head>`, the picture right after `#root`. */
export function withBootCoho(html, shell) {
  const t = html.replace(MARKED, "");
  if (!/<\/head>/.test(t)) throw new Error("web-shell: the export's index.html has no </head>");
  if (!/<div id="root"><\/div>/.test(t)) throw new Error('web-shell: the export\'s index.html has no <div id="root"></div>');
  return t.replace(/<\/head>/, `${shell.head}</head>`).replace(/<div id="root"><\/div>/, `<div id="root"></div>${shell.body}`);
}

/** (T910) frame 0 as a data: URI: the first paint asks for nothing (`pnpm splash`'s file, byte for byte). */
export function frame0Uri(t) {
  const src = here(`../apps/mobile/assets/splash/web-frame0-${t}.png`);
  if (!existsSync(src)) throw new Error(`web-shell: no ${src} (pnpm splash)`);
  return `data:image/png;base64,${readFileSync(src).toString("base64")}`;
}

/**
 * (T910) The app's script after the first frame: a module script that adds it once a frame is up (at once in a hidden
 * tab). The SAME text as the water pages' (tools/seo/prerender.ts `afterPaint`; web-shell.test holds the two equal), so
 * a water page made from this index.html is the page it was.
 */
export const afterPaint = (src) => `<script type="module">` +
  `const g=()=>{if(document.querySelector('script[src="${src}"]'))return;const s=document.createElement("script");s.src="${src}";document.body.append(s)};` +
  `document.visibilityState==="hidden"?g():requestAnimationFrame(()=>setTimeout(g))` +
  `</script>`;

/**
 * (T910) The export's head and script made not to hold the first paint: each stylesheet applied when in (its preload
 * stays, so it comes as early as before), the app's script preloaded and run after the first frame. A second run is a
 * no-op. The stylesheet line is the water pages' own (`appDocument`), so their pages are unchanged.
 */
export function paintFirst(html) {
  const scripts = [...html.matchAll(/<script src="([^"]+)" defer><\/script>/g)];
  if (scripts.length > 1) throw new Error(`web-shell: the export's index.html has ${scripts.length} app scripts, expected one`);
  let t = html.replace(/<link rel="stylesheet" href="([^"]+\.css)">/g, `<link rel="stylesheet" href="$1" media="print" onload="this.media='all'">`);
  if (scripts.length) {
    const [tag, src] = scripts[0];
    if (!/<\/head>/.test(t)) throw new Error("web-shell: the export's index.html has no </head>");
    t = t.replace(tag, afterPaint(src)).replace(/<\/head>/, `<link rel="preload" href="${src}" as="script"></head>`);
  } else if (!/<link rel="preload" href="[^"]+\.js" as="script">/.test(t)) {
    throw new Error("web-shell: the export's index.html has no app script");
  }
  return t;
}

const sha256 = (b) => createHash("sha256").update(b).digest("hex");
const walkFiles = (d) => readdirSync(d, { withFileTypes: true }).flatMap((e) => e.isDirectory() ? walkFiles(join(d, e.name)) : [join(d, e.name)]);

/**
 * (T910) THE WEB APP'S FACES, into the export: each face's WOFF2 (`tools/seo/fonts/`, checked against `fonts.json`) at its
 * served path, `/assets/fonts/<Face>.<sha12>.woff2` (the water pages' path, T409). Refused: a file not the one recorded,
 * a bundle that does not name a face's path (a stale `webFonts.generated.ts`), and any TTF of the phone's left in the
 * export (the web would fetch it as well).
 */
export function shellFonts(root) {
  const record = JSON.parse(readFileSync(FONTS_RECORD, "utf8"));
  const jsDir = join(root, "_expo/static/js/web");
  const js = existsSync(jsDir) ? walkFiles(jsDir).filter((f) => f.endsWith(".js")).map((f) => readFileSync(f, "utf8")).join("\n") : "";
  const ttf = existsSync(join(root, "assets")) ? walkFiles(join(root, "assets")).filter((f) => f.endsWith(".ttf")) : [];
  if (ttf.length) throw new Error(`web-shell: the export still carries ${ttf.length} TTF face(s) (${ttf[0]}): the web draws the WOFF2 (apps/mobile fonts.web.ts)`);
  mkdirSync(join(root, "assets/fonts"), { recursive: true });
  return Object.fromEntries(Object.entries(record).map(([face, made]) => {
    const b = readFileSync(`${FONT_DIR}${face}.woff2`);
    if (sha256(b) !== made.woff2_sha256) throw new Error(`web-shell: ${face}.woff2 is not the file fonts.json records (rerun node tools/emit-web-fonts.mjs)`);
    const path = servedPath(face, made.woff2_sha256);
    if (!js.includes(`"${path}"`)) throw new Error(`web-shell: the app's bundle does not name ${path} (node tools/emit-web-fonts.mjs --module, then export again)`);
    writeFileSync(join(root, path), b);
    return [face, path];
  }));
}

/**
 * (T910, owner 2026-11-19 ~11:40) THE HOME PAGE'S DESCRIPTION: `/` had none, the one SEO loss on production (Lighthouse
 * 92). The owner's store words (handoff/PLAN/play/store-description.md), under 155 characters as the site's pages are.
 * The water pages take it out: their own description is the page's (tools/seo `appDocument`).
 */
export const HOME_DESCRIPTION = "Your pocket guide to BC fishing regulations: tap any lake, river or stream on an interactive map " +
  "to view its fishing regulations and river conditions.";
export const HOME_DESCRIPTION_TAG = `<meta name="description" content="${HOME_DESCRIPTION}">`;

/** The export's index.html with the home description after its title (a second run: the same page). */
export function withHomeHead(html) {
  if (!/<\/title>/.test(html)) throw new Error("web-shell: the export's index.html has no <title>");
  return html.replace(HOME_DESCRIPTION_TAG, "").replace(/<\/title>/, `</title>${HOME_DESCRIPTION_TAG}`);
}

/**
 * (T1011, owner 2026-11-22) THE HOME PAGE'S CRAWLABLE LINKS. `/` is the JavaScript map: its HTML had no plain `<a href>` to any
 * page, so a crawler reached the site's pages only through the sitemap. A real footer row in the page's static HTML, after the
 * app's root: the root fills the window (the reset's `#root{height:100%}` under `body{overflow:hidden}`), so it never shows
 * over the map; without script the root is taken out and the row is what the page shows, under the "needs JavaScript" line.
 * (T1011 review) WITH script the row is `inert` from the moment it is parsed (a classic inline script right after it, run
 * before the app's): its links stay plain `<a href>` in the page for a crawler, but nothing can focus them — a Tab past the
 * app's last control landed on "River gauges" and the browser scrolled the clipped body (the map and search moved up, with
 * no scrollbar to bring them back).
 * The pages every site build makes: the gauges, the eight regions, About, Privacy — never /changes (no page when a release
 * changed nothing, site.ts). The SEO build refuses a row naming a page it did not make, or missing a region page it made
 * (tools/seo `homeLinkProblems`); the water pages take it out (their own document: tools/seo `appDocument`).
 */
export const HOME_LINKS = [["/gauges", "River gauges"], ...[1, 2, 3, 4, 5, 6, 7, 8].map((r) => [`/region/${r}`, `Region ${r}`]),
  ["/about", "About"], ["/privacy", "Privacy"]];
export const HOME_LINKS_MARK = { open: "<!--home-links-->", close: "<!--/home-links-->" };
export const HOME_LINKS_MARKED = /<!--home-links-->[\s\S]*?<!--\/home-links-->/g;
const HOME_LINKS_CSS = "#site-links{font:14px/1.6 system-ui,-apple-system,sans-serif;padding:12px 16px;text-align:center}" +
  "#site-links a{color:inherit;display:inline-block;padding:4px 6px}";
/** run as the row is parsed (a classic script, never deferred): with script, the row's links are out of focus and reach */
const HOME_LINKS_INERT = `<script>document.getElementById("site-links").setAttribute("inert","")</script>`;
export function withHomeLinks(html) {
  const t = html.replace(HOME_LINKS_MARKED, "");
  if (!/<\/head>/.test(t)) throw new Error("web-shell: the export's index.html has no </head>");
  if (!/<\/body>/.test(t)) throw new Error("web-shell: the export's index.html has no </body>");
  const row = HOME_LINKS.map(([href, label]) => `<a href="${href}">${label}</a>`).join(" · ");
  return t.replace(/<\/head>/, `${HOME_LINKS_MARK.open}<style id="site-links-css">${HOME_LINKS_CSS}</style>` +
      `<noscript><style>#root{display:none}</style></noscript>${HOME_LINKS_MARK.close}</head>`)
    .replace(/<\/body>/, `${HOME_LINKS_MARK.open}<nav id="site-links" aria-label="Pages on this site">${row}</nav>${HOME_LINKS_INERT}${HOME_LINKS_MARK.close}</body>`);
}

/** Put the shell into a web export in place: frame 0 inlined, the paint first, the faces, the home page's links. */
export function shellExport(dir, f = cohoFacts()) {
  const root = resolve(dir);
  const frame0 = Object.fromEntries(THEMES.map((t) => [t, frame0Uri(t)]));
  const fonts = shellFonts(root);
  const index = join(root, "index.html");
  // (T1042) the home page names the PNG icons: Google reads a site's favicon from it
  writeFileSync(index, withIconLinks(withHomeLinks(withHomeHead(paintFirst(withBootCoho(readFileSync(index, "utf8"), bootCoho(f, { frame0 })))))));
  return { frame0, fonts };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const dir = process.argv[2];
  if (!dir) { console.error("usage: node tools/web-shell.mjs <web export dir>"); process.exit(2); }
  const { fonts } = shellExport(dir);
  console.log(`web-shell: the coho opening (frame 0 inlined), the paint first, ${Object.keys(fonts).length} faces in ${join(resolve(dir), "index.html")}`);
}
