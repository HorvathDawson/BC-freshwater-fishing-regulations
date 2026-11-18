/**
 * (T844, owner 2026-11-17: "loading on webapp should use the new one too like the app", "Opening loader only") THE WEB'S
 * OPENING IS THE COHO: the web export's index.html carries the start screen's coho, frame 0 (tools/web-shell.mjs), so the
 * first paint before the app's script is the coho, not a white page; it stands where the start screen's coho stands and
 * stays over it until the app's own coho is in; the water pages take it out. Reads source off disk (tools/: the boundaries
 * gate keeps node:fs out of the packages).
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";
// @ts-expect-error a plain .mjs tool (no types)
import { bootCoho, cohoFacts, MARKED, shellExport, withBootCoho } from "./web-shell.mjs";
import { COHO } from "../packages/ui-native/src/coho.generated";
import { COHO_GROUND_PAD, COHO_WIDTH, cohoHeight } from "../packages/ui-native/src/coho";
import { THEMES } from "../packages/ui-native/src/theme";
import { BARE_APP } from "./seo/build";
import { appDocument, BOOT_COHO } from "./seo/prerender";

const at = (p: string) => fileURLToPath(new URL(`../${p}`, import.meta.url));
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
  });

  describe("an export put through it", () => {
    let dir = "";
    afterEach(() => { if (dir) rmSync(dir, { recursive: true, force: true }); dir = ""; });
    it("serves frame 0 from `pnpm splash`'s files under a content hash, and the page points at them", () => {
      dir = mkdtempSync(join(tmpdir(), "web-shell-"));
      mkdirSync(join(dir, "assets"), { recursive: true });
      writeFileSync(join(dir, "index.html"), BARE_APP);
      const { frame0 } = shellExport(dir);
      const html = readFileSync(join(dir, "index.html"), "utf8");
      for (const t of ["light", "dark"] as const) {
        expect(frame0[t]).toMatch(new RegExp(`^/assets/boot/web-frame0-${t}\\.[0-9a-f]{32}\\.png$`));
        expect(readFileSync(join(dir, frame0[t])).equals(readFileSync(at(`apps/mobile/assets/splash/web-frame0-${t}.png`)))).toBe(true);
        expect(html).toContain(`url(${frame0[t]})`);
      }
    });
  });

  it("frame 0 is the sheet's own frame size (the web draws the sheet at that density too)", () => {
    for (const t of ["light", "dark"]) {
      const b = readFileSync(at(`apps/mobile/assets/splash/web-frame0-${t}.png`));
      expect([b.readUInt32BE(16), b.readUInt32BE(20)]).toEqual([COHO.frame.width, COHO.frame.height]);
    }
  });
});
