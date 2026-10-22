// Back, everywhere — web check: the browser's own back (page.goBack(), i.e. the back button and a phone browser's
// swipe-back) through every surface reachable without a map tap, in light and dark, phone width 390x844.
//
//   node handoff/PLAN/app-data/map-chrome/back/web-check.mjs <out-dir> [url]
//
// Playwright is found as in ../../g1/web-check.mjs (app/node_modules, else the repo .venv's driver package).
import { createRequire } from "node:module";
import { existsSync, mkdirSync, readdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "../../../../..");
function playwright() {
  const attempt = (from, name) => { try { return createRequire(join(from, "noop.js"))(name); } catch { return null; } };
  for (const name of ["playwright", "playwright-core"]) { const p = attempt(join(REPO, "app"), name); if (p) return p; }
  const lib = join(REPO, ".venv", "lib");
  for (const py of existsSync(lib) ? readdirSync(lib).filter((d) => d.startsWith("python")) : []) {
    const driver = join(lib, py, "site-packages", "playwright", "driver", "package");
    if (existsSync(join(driver, "package.json"))) { const p = attempt(driver, driver); if (p) return p; }
  }
  throw new Error("no Playwright found in the repo");
}
const { chromium } = playwright();
const OUT = process.argv[2];
if (!OUT) { console.error("usage: web-check.mjs <out-dir> [url]"); process.exit(2); }
mkdirSync(OUT, { recursive: true });
const URL = process.argv[3] ?? "http://localhost:39463/";

const browser = await chromium.launch({ headless: true });
const log = [];
for (const scheme of ["light", "dark"]) {
  const ctx = await browser.newContext({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true,
                                         deviceScaleFactor: 2, colorScheme: scheme });
  const p = await ctx.newPage();
  const errors = [];
  p.on("pageerror", (e) => errors.push(String(e).slice(0, 200)));
  let n = 0;
  const step = async (what) => {
    await p.waitForTimeout(700);
    const sel = await p.evaluate(() => [...document.querySelectorAll("[role=tab][aria-selected=true]")]
      .map((t) => t.getAttribute("aria-label") ?? t.textContent).join(","));
    const sheet = await p.getByRole("button", { name: "Done" }).count();
    const entry = `${scheme} ${String(++n).padStart(2, "0")} ${what}: selected [${sel}] sheet ${sheet > 0} history ${await p.evaluate(() => history.length)}`;
    log.push(entry); console.log(entry);
    await p.screenshot({ path: `${OUT}/${scheme}-${String(n).padStart(2, "0")}-${what}.png` });
  };
  const back = async () => { await p.goBack({ waitUntil: "commit" }).catch(() => {}); };
  await p.goto(URL, { waitUntil: "networkidle" });
  await p.waitForTimeout(4000);
  await step("map-root");
  await p.getByRole("tab", { name: "Search" }).click();
  await step("search-tab");
  await back();
  await step("after-back-map-tab");
  await p.getByLabel("Layers").first().click();
  await step("layers-sheet");
  await back();
  await step("after-back-sheet-closed");
  await p.getByRole("tab", { name: "Spots" }).click();
  await step("spots-tab");
  await back();
  await step("after-back-map");
  // search's eye: the water shown on the main map; back returns to search (a day sheet appears only if a water opened)
  await p.getByRole("tab", { name: "Search" }).click();
  await p.getByLabel("Search for a river, lake or town").fill("vedder");
  await p.waitForTimeout(2500);
  const row = p.getByRole("button", { name: /Vedder River/ }).first();
  if (await row.count()) {
    await row.click();                      // the first tap selects (the map below shows it)
    await p.waitForTimeout(1500);
    await p.getByLabel("View Vedder River").click();   // the selected row's eye shows it on the main map
    await p.waitForTimeout(4000);
    await step("search-eye-shown-on-map");
    const date = p.getByTestId("regs-date");
    if (await date.count()) {
      await date.click();
      await step("day-sheet");
      await back();
      await step("after-back-day-sheet-closed");
    }
    await back();
    await step("after-back-search-again");
  }
  log.push(`${scheme} errors: ${JSON.stringify(errors)}`);
  await ctx.close();
}
await browser.close();
writeFileSync(`${OUT}/web-check.log`, log.join("\n") + "\n");
