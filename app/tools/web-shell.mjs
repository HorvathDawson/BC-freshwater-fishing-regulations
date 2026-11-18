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
 * the 0.9 MB sheet itself (to step it before the app was up) put the map ~0.7 s later, the sheet taking the line from the
 * release files; frame 0 (`web-frame0-<theme>.png`, 20 KB, `pnpm splash`) costs ~0.2 s. Light or dark by the system's
 * scheme, as the start screen reads it. Without script the page says it needs one, uncovered (`<noscript>`).
 * The water pages (tools/seo `appDocument`) take it out: their prerendered pane is the first paint there.
 */
import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

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
 * marks. `frame0` is the served URL per theme.
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

/** Put the shell into a web export in place: frame 0 (`pnpm splash`'s files) copied in under a content hash. */
export function shellExport(dir, f = cohoFacts()) {
  const root = resolve(dir);
  mkdirSync(join(root, "assets/boot"), { recursive: true });
  const frame0 = Object.fromEntries(THEMES.map((t) => {
    const src = here(`../apps/mobile/assets/splash/web-frame0-${t}.png`);
    if (!existsSync(src)) throw new Error(`web-shell: no ${src} (pnpm splash)`);
    const hash = createHash("md5").update(readFileSync(src)).digest("hex");
    const to = `assets/boot/web-frame0-${t}.${hash}.png`;
    copyFileSync(src, join(root, to));
    return [t, `/${to}`];
  }));
  const index = join(root, "index.html");
  writeFileSync(index, withBootCoho(readFileSync(index, "utf8"), bootCoho(f, { frame0 })));
  return { frame0 };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const dir = process.argv[2];
  if (!dir) { console.error("usage: node tools/web-shell.mjs <web export dir>"); process.exit(2); }
  const { frame0 } = shellExport(dir);
  console.log(`web-shell: the coho opening is in ${join(resolve(dir), "index.html")} (${frame0.light}, ${frame0.dark})`);
}
