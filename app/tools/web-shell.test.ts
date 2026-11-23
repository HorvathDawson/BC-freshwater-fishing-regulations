/**
 * (T844, owner 2026-11-17: "loading on webapp should use the new one too like the app", "Opening loader only") THE WEB'S
 * OPENING IS THE COHO: the web export's index.html carries the start screen's coho, frame 0 (tools/web-shell.mjs), so the
 * first paint before the app's script is the coho, not a white page; it stands where the start screen's coho stands and
 * stays over it until the app's own coho is in; the water pages take it out. Reads source off disk (tools/: the boundaries
 * gate keeps node:fs out of the packages).
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";
// @ts-expect-error a plain .mjs tool (no types)
import { afterPaint as shellAfterPaint, bootCoho, cohoFacts, HOME_DESCRIPTION, HOME_DESCRIPTION_TAG, MARKED, paintFirst, shellExport, withBootCoho,
         withHomeHead, withHomeLinks } from "./web-shell.mjs";
import { COHO } from "../packages/ui-native/src/coho.generated";
import { COHO_WEB } from "../packages/ui-native/src/cohoWeb.generated";
import { COHO_GROUND_PAD, COHO_WIDTH, cohoHeight } from "../packages/ui-native/src/coho";
import { THEMES } from "../packages/ui-native/src/theme";
import { BARE_APP } from "./seo/build";
import { afterPaint, appDocument, BOOT_COHO } from "./seo/prerender";
import { WEB_FONTS } from "../apps/mobile/src/webFonts.generated";

const at = (p: string) => fileURLToPath(new URL(`../${p}`, import.meta.url));
/** an export's index.html as Expo writes it (the map's two stylesheets, the app's deferred script) */
const EXPORT_APP = BARE_APP
  .replace("</head>", `<link rel="preload" href="/_expo/static/css/m-1.css" as="style"><link rel="stylesheet" href="/_expo/static/css/m-1.css"></head>`)
  .replace("</body>", `<script src="/_expo/static/js/web/index-1.js" defer></script></body>`);
const F = cohoFacts();
const URLS = { frame0: { light: "/assets/boot/f-light.png", dark: "/assets/boot/f-dark.png" } };
const SHELL = bootCoho(F, URLS);
const css = /<style id="boot-coho-css">([\s\S]*?)<\/style>/.exec(SHELL.head)![1]!;
const darkCss = /@media \(prefers-color-scheme:dark\)\{([\s\S]*)\}$/.exec(css)![1]!;

describe("the web's opening is the coho (T844)", () => {
  it("its facts are the app's coho: the frame, the start screen's width and ground, each scheme's colours", () => {
    expect(F.frame).toEqual(COHO.frame);
    expect([F.width, F.pad]).toEqual([COHO_WIDTH, COHO_GROUND_PAD]);
    for (const t of ["light", "dark"] as const) expect([F.card[t], F.ground[t]]).toEqual([THEMES[t].card, THEMES[t].coho.ground]);
  });

  it("is the start screen's coho, at its size and place: the window's centre, COHO_WIDTH wide, its ground past the frame", () => {
    const h = cohoHeight(COHO_WIDTH);
    expect(css).toContain(`#boot-coho .b{position:absolute;left:50%;top:50%;width:${COHO_WIDTH}px;height:${h}px;margin:${-h / 2}px 0 0 ${-COHO_WIDTH / 2}px}`);
    expect(css).toContain(`#boot-coho .g{position:absolute;inset:${-COHO_GROUND_PAD}px}`);
    expect(css).toContain("#boot-coho .f0{position:absolute;inset:0;background-size:100% 100%}");
    // the start screen (App.tsx) puts its coho at the window's centre, under the testIDs the hand-over looks for
    expect(readFileSync(at("apps/mobile/src/App.tsx"), "utf8")).toMatch(/testID="boot-screen"[\s\S]{0,400}top: "50%", marginTop: -BOOT_LOADER_H \/ 2/);
    expect(readFileSync(at("packages/ui-native/src/CohoLoader.web.tsx"), "utf8")).toMatch(/<Image testID="coho-sheet"/);
  });

  it("each scheme wears its own frame 0, card and ground; the whole window until the start screen, then the box alone", () => {
    const light = css.slice(0, css.indexOf("@media"));
    for (const [t, part] of [["light", light], ["dark", darkCss]] as const)
      expect(part).toContain(`#boot-coho{background:${THEMES[t].card}}#boot-coho .g{background:${THEMES[t].coho.ground}}` +
                             `#boot-coho .f0{background-image:url(${URLS.frame0[t]})}`);
    expect(css).toMatch(/^#boot-coho\{position:fixed;inset:0;z-index:1;pointer-events:none\}\[data-boot-coho=over\] #boot-coho\{background:none\}/);
    // never the sheet: the shell asks for nothing the app would not (measured: it held the map back)
    expect(SHELL.head + SHELL.body).not.toMatch(/\.webp|new Image/);
  });

  describe("its states, run in a page (jsdom: the shell's own lines, the app's drawing played by hand)", () => {
    let win: JSDOM["window"];
    const html = () => win.document.documentElement;
    const settle = () => new Promise((r) => setTimeout(r, 0));
    const state = () => html().getAttribute("data-boot-coho");
    const picture = () => win.document.getElementById("boot-coho");
    const draw = (inner: string) => { win.document.getElementById("root")!.innerHTML = inner; return settle(); };
    const BOOT = (img: string) => `<div data-testid="boot-screen"><div role="progressbar"><div data-testid="coho-sheet">${img}</div></div><p>Casting about…</p></div>`;
    /** an <img> as jsdom has it: never loaded unless the test says so */
    const inNow = (i: HTMLImageElement) => {
      Object.defineProperty(i, "complete", { value: true }); Object.defineProperty(i, "naturalWidth", { value: 2304 });
    };
    beforeEach(() => {
      win = new JSDOM(withBootCoho(BARE_APP, SHELL).replace(/<script[^>]*>[\s\S]*?<\/script>/g, ""), { runScripts: "outside-only" }).window;
      for (const m of SHELL.body.matchAll(/<script type="module">([\s\S]*?)<\/script>/g)) win.eval(m[1]!);
    });

    it("the coho over the whole window until the app draws something of its own; its providers' empty boxes are not that", async () => {
      expect([state(), !!picture()]).toEqual([null, true]);
      await draw("<div><div></div></div>");
      expect([state(), !!picture()]).toEqual([null, true]);
    });

    it("over the start screen's coho box until that coho's sheet is in, then gone (the watching stops)", async () => {
      await draw(BOOT('<img src="/s.webp">'));
      expect([state(), !!picture()]).toEqual(["over", true]);
      const img = win.document.querySelector("img")!;
      win.document.querySelector("p")!.textContent = "1.9 MB of 2.3 MB";   // the start screen's words move on
      await settle();
      expect(state()).toBe("over");
      inNow(img); img.dispatchEvent(new win.Event("load"));
      await settle();
      expect([state(), picture()]).toEqual(["gone", null]);
    });

    it("a sheet already in when the start screen is drawn: gone at once", async () => {
      win.document.getElementById("root")!.innerHTML = BOOT('<img src="/s.webp">');
      inNow(win.document.querySelector("img")!);
      await settle();
      expect([state(), picture()]).toEqual(["gone", null]);
    });

    it("anything else the app draws (the map, a failed start) ends it, sheet or no sheet; once gone, gone", async () => {
      await draw('<div data-testid="boot-failed"></div>');
      expect([state(), picture()]).toEqual(["gone", null]);
      await draw(BOOT('<img src="/s.webp">'));
      expect(state()).toBe("gone");
    });
  });

  it("goes into the export's index.html: the style before </head>, the picture and its watcher after the root; a second run is the same page", () => {
    const once = withBootCoho(BARE_APP, SHELL);
    expect(once).toContain(`<div id="root"></div>${SHELL.body}`);
    expect(once).toContain(`${SHELL.head}</head>`);
    expect(withBootCoho(once, SHELL)).toBe(once);
    expect(() => withBootCoho("<html><head></head><body></body></html>", SHELL)).toThrow(/no <div id="root"><\/div>/);
    // a module script: it never holds up the parse, and runs before the app's deferred script (document order)
    expect(SHELL.body).toMatch(/<script type="module">/);
    expect(SHELL.head).toContain("<noscript><style>#boot-coho{display:none}</style></noscript>");
    // the circling fish is not in it
    expect(once).not.toMatch(/fish-sprite|fish-ring/);
  });

  it("the water pages take it out (their prerendered pane is the first paint): the page is the one built without it", () => {
    expect(BOOT_COHO.source).toBe(MARKED.source);
    const d = { head: "", critical: "", cssHref: "/c.css", preload: [], item: "gnis:1", region: "2", pane: "<p>pane</p>" };
    expect(appDocument(withBootCoho(BARE_APP, SHELL), d)).toBe(appDocument(BARE_APP, d));
    // (T910) and the paint-first head (the map's stylesheets applied when in, the script preloaded and run after the
    // first frame) makes no other water page: the same page as from the export before it
    expect(appDocument(withHomeHead(paintFirst(withBootCoho(EXPORT_APP, SHELL))), d)).toBe(appDocument(EXPORT_APP, d));
  });

  it("(T910) the home page has a description (the owner's store words, the site's 155-character limit), once", () => {
    const t = withHomeHead(EXPORT_APP);
    expect(t).toContain(`</title>${HOME_DESCRIPTION_TAG}`);
    expect(HOME_DESCRIPTION.length).toBeLessThanOrEqual(155);
    expect(HOME_DESCRIPTION).not.toMatch(/["<>&]/);
    expect(withHomeHead(t)).toBe(t);
    expect(t.match(/<meta name="description"/g)).toHaveLength(1);
  });

  it("(T910) the first paint never waits for the app's script: stylesheets applied when in, the script preloaded and run after the first frame", () => {
    const t = paintFirst(withBootCoho(EXPORT_APP, SHELL));
    expect(t).toContain(`<link rel="preload" href="/_expo/static/css/m-1.css" as="style"><link rel="stylesheet" href="/_expo/static/css/m-1.css" media="print" onload="this.media='all'">`);
    expect(t).not.toMatch(/<link rel="stylesheet" href="[^"]+" ?>|<script src="[^"]+" defer>/);
    expect(t).toContain(`<link rel="preload" href="/_expo/static/js/web/index-1.js" as="script"></head>`);
    expect(t).toContain(`${SHELL.body}${afterPaint("/_expo/static/js/web/index-1.js")}`);   // the watcher first (document order)
    expect(shellAfterPaint("/x.js")).toBe(afterPaint("/x.js"));                            // the water pages' own hand-over
    expect(paintFirst(t)).toBe(t);
    expect(() => paintFirst(BARE_APP)).toThrow(/no app script/);
  });

  describe("an export put through it", () => {
    let dir = "";
    afterEach(() => { if (dir) rmSync(dir, { recursive: true, force: true }); dir = ""; });
    /** an export: its index.html, and a bundle naming `faces` (default: every face the web app loads) */
    const exported = (faces: Record<string, string> = WEB_FONTS) => {
      dir = mkdtempSync(join(tmpdir(), "web-shell-"));
      mkdirSync(join(dir, "assets"), { recursive: true });
      mkdirSync(join(dir, "_expo/static/js/web"), { recursive: true });
      writeFileSync(join(dir, "index.html"), EXPORT_APP);
      writeFileSync(join(dir, "_expo/static/js/web/index-1.js"), `var f=${JSON.stringify(faces)};`);
      return dir;
    };
    it("(T910) carries frame 0 in the page: `pnpm splash`'s files byte for byte, no request", () => {
      const { frame0 } = shellExport(exported());
      const html = readFileSync(join(dir, "index.html"), "utf8");
      expect(html).toContain(HOME_DESCRIPTION_TAG);
      for (const t of ["light", "dark"] as const) {
        const png = readFileSync(at(`apps/mobile/assets/splash/web-frame0-${t}.png`));
        expect(frame0[t]).toBe(`data:image/png;base64,${png.toString("base64")}`);
        expect(html).toContain(`url(${frame0[t]})`);
      }
      expect(existsSync(join(dir, "assets/boot"))).toBe(false);
    });
    it("(T910) puts each face's WOFF2 at the path the bundle names; refuses a bundle without it, and a phone TTF left in", () => {
      const { fonts } = shellExport(exported());
      expect(fonts).toEqual(WEB_FONTS);
      for (const [face, path] of Object.entries(WEB_FONTS))
        expect(readFileSync(join(dir, path)).equals(readFileSync(at(`tools/seo/fonts/${face}.woff2`)))).toBe(true);
      rmSync(dir, { recursive: true, force: true });
      const { Archivo_400Regular: _gone, ...fewer } = WEB_FONTS;
      expect(() => shellExport(exported(fewer))).toThrow(/does not name \/assets\/fonts\/Archivo_400Regular\./);
      rmSync(dir, { recursive: true, force: true });
      exported(); mkdirSync(join(dir, "assets/x"), { recursive: true }); writeFileSync(join(dir, "assets/x/A.ttf"), "");
      expect(() => shellExport(dir)).toThrow(/still carries 1 TTF/);
    });
  });

  it("frame 0 is the web sheet's own frame size (the web draws the sheet at that density too)", () => {
    for (const t of ["light", "dark"]) {
      const b = readFileSync(at(`apps/mobile/assets/splash/web-frame0-${t}.png`));
      expect([b.readUInt32BE(16), b.readUInt32BE(20)]).toEqual([COHO_WEB.frame.width, COHO_WEB.frame.height]);
    }
  });
});

describe("the web's own coho sheet, the phones' unchanged (T844, owner 2026-11-17: \"make way smaller for web\")", () => {
  const ROOT = at("");
  const files: [string, string][] = [];
  const walk = (d: string) => {
    for (const f of readdirSync(d)) {
      const p = join(d, f);
      if (statSync(p).isDirectory()) { if (f !== "node_modules" && f !== "dist") walk(p); continue; }
      if (/\.tsx?$/.test(f) && !/\.test\.tsx?$/.test(f) && !f.endsWith(".d.ts")) files.push([relative(ROOT, p), readFileSync(p, "utf8")]);
    }
  };
  for (const d of ["packages/ui-native/src", "packages/ui/src", "packages/core/src", "apps/mobile/src"]) walk(at(d));
  const imports = (s: string, m: string) => new RegExp(`from "\\./(?:[\\w/]*/)?${m}"`).test(s);

  it("only the web's halves import it (a phone never bundles it); the phones' loader draws the phones' sheet", () => {
    expect(files.length).toBeGreaterThan(50);
    expect(files.filter(([, s]) => imports(s, "cohoWeb\\.generated")).map(([f]) => f)).toEqual(["packages/ui-native/src/CohoLoader.web.tsx"]);
    const native = files.find(([f]) => f === "packages/ui-native/src/CohoLoader.native.tsx")![1];
    expect([imports(native, "coho\\.generated"), imports(native, "cohoWeb\\.generated"), /cohoSheet\(palette\)/.test(native)]).toEqual([true, false, true]);
  });

  it("is way smaller: each theme's web sheet at most 150 KB (the phones' are 1.4 to 1.8 MB), same loop and aspect", () => {
    const gen = readFileSync(at("packages/ui-native/src/cohoWeb.generated.ts"), "utf8");
    const sheets = [...gen.matchAll(/from "\.\/(coho\/[\w-]+\.webp)"/g)].map((m) => m[1]!);
    expect(sheets.sort()).toEqual(["coho/coho-dark-web.webp", "coho/coho-light-web.webp"]);
    for (const f of sheets) expect([f, statSync(at(`packages/ui-native/src/${f}`)).size <= 150 * 1024]).toEqual([f, true]);
    // (T852) its own frame (2 px per CSS px; the phones' 2.5 px per dp), at the phones' aspect
    expect([COHO_WEB.frame.width * COHO.frame.height, COHO_WEB.loopMs, COHO_WEB.columns * COHO_WEB.rows >= COHO_WEB.frames])
      .toEqual([COHO.frame.width * COHO_WEB.frame.height, COHO.loopMs, true]);
  });
});

describe("(T1011) the home page's link row", () => {
  it("(T1011 review) with script the row is inert (no Tab can focus it and scroll the clipped page); without script it is a working row", () => {
    const home = withHomeLinks(BARE_APP);
    const run = new JSDOM(home, { runScripts: "dangerously" }).window.document, still = new JSDOM(home).window.document;
    expect(run.getElementById("site-links")!.hasAttribute("inert")).toBe(true);
    expect(still.getElementById("site-links")!.hasAttribute("inert")).toBe(false);
    expect(still.querySelectorAll("#site-links a[href]").length).toBe(11);           // the crawler's plain links, either way
    expect(run.querySelectorAll("#site-links a[href]").length).toBe(11);
  });
});
