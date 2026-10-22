// Map rail web check (owner 2026-10-21): the side rail, the Spots menu, the long press and its drop sheet.
//
// Run from the repo root (any checkout or worktree), with the app's web dev server already up:
//
//   node handoff/PLAN/app-data/map-chrome/rail/web-check.mjs <out-dir> [url]      # url defaults to http://localhost:39562/
//
// Playwright is found in the repo, never in a machine's npx cache: `playwright` (or `playwright-core`) from
// app/node_modules if a package there provides it, else the Python Playwright in the repo's .venv — its bundled
// driver is the `playwright-core` package. The browser is Playwright's own Chromium for that version, from the
// usual cache; if it is missing, `.venv/bin/python -m playwright install chromium` fetches it. PLAYWRIGHT_CORE
// overrides the lookup with a path to a playwright(-core) package directory.
import { createRequire } from "node:module";
import { existsSync, mkdirSync, readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "../../../../..");

function playwright() {
  const tried = [];
  const attempt = (from, name) => {
    tried.push(`${name} from ${from}`);
    try { return createRequire(join(from, "noop.js"))(name); } catch { return null; }
  };
  if (process.env.PLAYWRIGHT_CORE) {
    const p = attempt(dirname(process.env.PLAYWRIGHT_CORE), process.env.PLAYWRIGHT_CORE);
    if (p) return p;
  }
  for (const name of ["playwright", "playwright-core"]) {
    const p = attempt(join(REPO, "app"), name);
    if (p) return p;
  }
  const lib = join(REPO, ".venv", "lib");
  for (const py of existsSync(lib) ? readdirSync(lib).filter((d) => d.startsWith("python")) : []) {
    const driver = join(lib, py, "site-packages", "playwright", "driver", "package");
    if (existsSync(join(driver, "package.json"))) {
      const p = attempt(driver, driver);
      if (p) return p;
    }
  }
  throw new Error(`no Playwright found in the repo; tried:\n  ${tried.join("\n  ")}\n` +
                  "install the Python one into .venv (pip install playwright) or set PLAYWRIGHT_CORE");
}
const { chromium } = playwright();


const OUT = process.argv[2];
if (!OUT) { console.error("usage: node handoff/PLAN/app-data/map-chrome/rail/web-check.mjs <out-dir> [url]"); process.exit(2); }
mkdirSync(OUT, { recursive: true });
const URL = process.argv[3] ?? "http://localhost:39562/";

const browser = await chromium.launch({ headless: true });
const notes = [];
const errors = [];

async function open(scheme, size = { width: 390, height: 844 }, touch = true) {
  const ctx = await browser.newContext({ viewport: size, hasTouch: touch, isMobile: touch, deviceScaleFactor: 2,
                                         colorScheme: scheme });
  const p = await ctx.newPage();
  p.on("pageerror", (e) => errors.push(String(e).slice(0, 200)));
  p.on("console", (m) => { if (m.type() === "error") errors.push(m.text().slice(0, 200)); });
  await p.goto(URL, { waitUntil: "load", timeout: 180000 });
  await p.getByLabel("Search", { exact: true }).first().waitFor({ timeout: 180000 });
  await p.waitForTimeout(9000);          // the first data loads busy the page; a held finger's timer waits behind them
  return { ctx, p, cdp: touch ? await ctx.newCDPSession(p) : null };
}
const visible = async (p, label) => p.getByLabel(label, { exact: true }).first().isVisible().catch(() => false);
const text = async (p, t) => p.getByText(t, { exact: true }).first().isVisible().catch(() => false);
const section = async (name, body) => {
  try { await body(); } catch (e) { notes.push(`${name} FAILED: ${String(e).slice(0, 300)}`); }
};
const hold = async (p, cdp, x, y, ms = 800) => {
  await cdp.send("Input.dispatchTouchEvent", { type: "touchStart", touchPoints: [{ x, y }] });
  await p.waitForTimeout(ms);
  await cdp.send("Input.dispatchTouchEvent", { type: "touchEnd", touchPoints: [] });
  await p.waitForTimeout(1000);
};

try {
  await section("phone-light", async () => {
    const { ctx, p, cdp } = await open("light");
    const shot = (n) => p.screenshot({ path: `${OUT}/${n}.png` });
    await shot("01-map-rail");
    notes.push(`rail: layers=${await visible(p, "Choose layers")} spots=${await visible(p, "Your spots")}; ` +
               `Spots tab gone=${!(await p.getByRole("tab", { name: "Spots" }).count())}`);

    await p.getByLabel("Your spots", { exact: true }).first().click();
    await p.waitForTimeout(900);
    await shot("02-spots-menu");
    notes.push(`spots menu: add here=${await text(p, "Add a spot here")} empty=${await text(p, "No spots yet")} ` +
               `mine=${await text(p, "At my location")}`);
    await p.goBack();
    await p.waitForTimeout(1200);
    notes.push(`browser back with the menu open: menu still open=${await text(p, "No spots yet")}, ` +
               `app still here=${await visible(p, "Your spots")}`);

    notes.push('under the finger: ' + await p.evaluate(() => { const e = document.elementFromPoint(195, 470); return e.tagName + '.' + e.className.toString().slice(0, 60); }));
    await hold(p, cdp, 195, 470);
    await shot("03-long-press-drop-sheet");
    notes.push(`held finger 800 ms: drop sheet=${await text(p, "New spot")}`);
    await p.getByRole("button", { name: "Cancel", exact: true }).first().click();
    await p.waitForTimeout(900);
    await shot("04-after-cancel");
    notes.push(`Cancel: drop sheet still=${await text(p, "New spot")}`);

    // a slow pan never drops one
    await cdp.send("Input.dispatchTouchEvent", { type: "touchStart", touchPoints: [{ x: 195, y: 470 }] });
    for (let i = 1; i <= 8; i++) {
      await cdp.send("Input.dispatchTouchEvent", { type: "touchMove", touchPoints: [{ x: 195 + i * 6, y: 470 }] });
      await p.waitForTimeout(100);
    }
    await cdp.send("Input.dispatchTouchEvent", { type: "touchEnd", touchPoints: [] });
    await p.waitForTimeout(900);
    notes.push(`slow pan 48 px over 800 ms: drop sheet=${await text(p, "New spot")}`);

    await hold(p, cdp, 200, 420);
    const before = await text(p, "New spot");
    await p.goBack();
    await p.waitForTimeout(1200);
    notes.push(`drop sheet up=${before}; browser back: still up=${await text(p, "New spot")}, ` +
               `app still here=${await visible(p, "Your spots")}`);

    await hold(p, cdp, 200, 420);
    await p.getByRole("button", { name: "Add a spot here", exact: true }).first().click();
    await p.waitForTimeout(3000);
    await shot("05-continue-capture");
    notes.push(`Continue: capture coach "Pick the water"=${await text(p, "Pick the water")}`);
    await p.goBack();
    await p.waitForTimeout(1500);
    notes.push(`browser back from the capture: rail back=${await visible(p, "Your spots")}`);
    await ctx.close();
  });

  await section("phone-dark", async () => {
    const { ctx, p } = await open("dark");
    await p.screenshot({ path: `${OUT}/06-dark-map-rail.png` });
    await p.getByLabel("Your spots", { exact: true }).first().click();
    await p.waitForTimeout(900);
    await p.screenshot({ path: `${OUT}/07-dark-spots-menu.png` });
    await ctx.close();
  });

  await section("conditions", async () => {
    const { ctx, p } = await open("light");
    await p.getByLabel("Conditions", { exact: true }).first().click();
    await p.waitForTimeout(4000);
    await p.screenshot({ path: `${OUT}/08-conditions-rail.png` });
    notes.push(`conditions rail: layers=${await visible(p, "Choose layers")} spots=${await visible(p, "Your spots")}`);
    await ctx.close();
  });

  await section("desktop", async () => {
    const { ctx, p } = await open("light", { width: 1280, height: 800 }, false);
    await p.screenshot({ path: `${OUT}/09-desktop-rail.png` });
    await p.mouse.click(640, 420, { button: "right" });
    await p.waitForTimeout(1000);
    await p.screenshot({ path: `${OUT}/10-desktop-right-click.png` });
    notes.push(`desktop right click: drop sheet=${await text(p, "New spot")}`);
    await ctx.close();
  });
} finally {
  console.log(JSON.stringify({ notes, errors: [...new Set(errors)].slice(0, 12) }, null, 2));
  await browser.close();
}
