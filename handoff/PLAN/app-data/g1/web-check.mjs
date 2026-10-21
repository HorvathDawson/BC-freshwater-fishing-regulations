// G1 web check: a phone-sized, touch-emulated Chromium against an expo web dev server.
//
// Run from the repo root (any checkout or worktree), with the app's web dev server already up:
//
//   node handoff/PLAN/app-data/g1/web-check.mjs <out-dir> [url]      # url defaults to http://localhost:39418/
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

const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "../../../..");

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
if (!OUT) { console.error("usage: node handoff/PLAN/app-data/g1/web-check.mjs <out-dir> [url]"); process.exit(2); }
mkdirSync(OUT, { recursive: true });
const URL = process.argv[3] ?? "http://localhost:39418/";

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true,
                                       deviceScaleFactor: 2 });
const p = await ctx.newPage();
const errors = [];
p.on("pageerror", (e) => errors.push(String(e).slice(0, 200)));
p.on("console", (m) => { if (m.type() === "error") errors.push(m.text().slice(0, 200)); });
const notes = [];
const note = async (what) => {
  const h = await p.evaluate(() => ({ length: history.length, state: JSON.stringify(history.state) }));
  notes.push({ what, ...h });
};
const shot = (n) => p.screenshot({ path: `${OUT}/${n}.png` });
const cdp = await ctx.newCDPSession(p);
const touchDrag = async (x0, y0, x1, y1, steps = 12, ms = 16) => {
  await cdp.send("Input.dispatchTouchEvent", { type: "touchStart", touchPoints: [{ x: x0, y: y0 }] });
  for (let i = 1; i <= steps; i++) {
    await cdp.send("Input.dispatchTouchEvent", { type: "touchMove",
      touchPoints: [{ x: x0 + ((x1 - x0) * i) / steps, y: y0 + ((y1 - y0) * i) / steps }] });
    await p.waitForTimeout(ms);
  }
  await cdp.send("Input.dispatchTouchEvent", { type: "touchEnd", touchPoints: [] });
};

/** The app, freshly loaded at the browse root (a page of its own: no history entries of the app's yet). */
const home = async () => {
  await p.goto(URL, { waitUntil: "load", timeout: 180000 });
  await p.getByLabel("Search", { exact: true }).first().waitFor({ timeout: 180000 });
  await p.waitForTimeout(3000);
};
/** Each check runs on its own, so one whose path through the app has moved on does not hide the others. */
const section = async (name, body) => {
  try { await body(); } catch (e) {
    notes.push({ what: `${name} FAILED: ${String(e).slice(0, 300)}` });
    await shot(`99-failure-${name}`).catch(() => {});
  }
};
const layers = p.getByLabel("Choose layers").first();

try {
  await section("water", async () => {
  await home();
  await shot("01-home"); await note("home (browse root)");

  // ---- browser back pops the stack -------------------------------------------------------------------------------
  await p.getByLabel("Search", { exact: true }).first().click();
  await p.getByLabel("Search for a river, lake or town").fill("Vedder River");
  await p.waitForTimeout(2500);
  await shot("02-search");
  const row = p.getByText("Vedder River", { exact: true }).first();
  await row.click();
  await p.waitForTimeout(800);
  // a picked row may need its "go" control
  if (!(await p.getByText("Regulations are coming").count())) {
    const go = p.getByRole("button", { name: "View Vedder River" }).first();
    if (await go.count()) await go.click();
  }
  await p.getByText("Regulations are coming").waitFor({ timeout: 30000 });
  await shot("03-water-open"); await note("water opened from search (depth 1)");
  await p.goBack();
  await p.waitForTimeout(1200);
  const afterBrowserBack = await p.getByText("Regulations are coming").count();
  await shot("04-after-browser-back"); await note(`browser back: water still shown = ${afterBrowserBack > 0}`);

  // ---- the ‹ leaves no stranded entry --------------------------------------------------------------------------
  await p.getByLabel("Search", { exact: true }).first().click();
  await p.waitForTimeout(500);
  await p.getByLabel("Search for a river, lake or town").fill("Vedder River");
  await p.waitForTimeout(2500);
  await shot("04b-search-again");
  const row2 =p.getByText("Vedder River", { exact: true }).first();
  if (await row2.count()) await row2.click();
  await p.waitForTimeout(800);
  if (!(await p.getByText("Regulations are coming").count())) {
    const go = p.getByRole("button", { name: "View Vedder River" }).first();
    if (await go.count()) await go.click();
  }
  await p.getByText("Regulations are coming").waitFor({ timeout: 30000 });
  await note("water opened again (depth 1)");
  await p.getByRole("button", { name: "Back", exact: true }).first().click();
  await p.waitForTimeout(1200);
  await note(`‹ pressed: water still shown = ${(await p.getByText("Regulations are coming").count()) > 0}`);
  });

  // ---- the Layers sheet drags closed -----------------------------------------------------------------------------
  await section("sheet-drag", async () => {
  await home();
  await layers.click();
  await p.getByLabel("Done").waitFor({ timeout: 10000 });
  await p.waitForTimeout(800);
  await shot("05-sheet-open");
  const handle = await p.getByTestId("sheet-handle").boundingBox();
  // a short slow drag: springs back
  await touchDrag(handle.x + handle.width / 2, handle.y + 20, handle.x + handle.width / 2, handle.y + 60, 20, 40);
  await p.waitForTimeout(900);
  const afterShort = await p.getByLabel("Done").isVisible().catch(() => false);
  notes.push({ what: `short slow drag (40 px): sheet still open = ${afterShort}` });
  await shot("06-sheet-after-short-drag");
  const h2 = await p.getByTestId("sheet-handle").boundingBox();
  await touchDrag(h2.x + h2.width / 2, h2.y + 20, h2.x + h2.width / 2, h2.y + 420, 14, 16);
  await p.waitForTimeout(1200);
  const afterLong = await p.getByLabel("Done").isVisible().catch(() => false);
  notes.push({ what: `long drag (400 px): sheet still open = ${afterLong}` });
  await shot("07-sheet-after-long-drag");
  });

  // ---- the browser's back closes an open sheet first (G1 follow-up) ------------------------------------------------
  await section("sheet-back", async () => {
  await home();
  await note("sheet closed (browse root)");
  await layers.click();
  await p.getByLabel("Done").waitFor({ timeout: 10000 });
  await p.waitForTimeout(800);
  await note("sheet open: one more history entry");
  await p.goBack();
  await p.waitForTimeout(1200);
  const afterSheetBack = await p.getByLabel("Done").isVisible().catch(() => false);
  const stillHere = await p.getByLabel("Choose layers").first().isVisible().catch(() => false);
  await shot("08-sheet-after-browser-back");
  await note(`browser back with the sheet open: sheet still open = ${afterSheetBack}, app still here = ${stillHere}`);
  await layers.click();
  await p.getByLabel("Done").waitFor({ timeout: 10000 });
  await p.waitForTimeout(800);
  await p.getByLabel("Done").click();
  await p.waitForTimeout(1200);
  await note("sheet opened and closed by Done: its entry dropped (state back to the page's own)");
  });
} catch (e) {
  notes.push({ what: `FAILED: ${String(e).slice(0, 300)}` });
  await shot("99-failure").catch(() => {});
} finally {
  console.log(JSON.stringify({ notes, errors: errors.slice(0, 10) }, null, 2));
  await browser.close();
}
