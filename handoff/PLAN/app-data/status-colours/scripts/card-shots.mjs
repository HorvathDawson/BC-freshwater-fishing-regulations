// T47 part 2 (review): the CARD, not only the map — where restricted and open are words. One water's Regulations page
// (the start link, `?regs=…&tab=…`, app/tools/regs-web/states.mjs) in light, dark and the colour-blind palette:
//   keep-top      the answer word ("Open", open colour) and the year strip (open / closed hatch / restricted marks)
//   keep-release  the Keep tab scrolled to the first "release" status word (restricted colour) with the rows around it
//
//   node handoff/PLAN/app-data/status-colours/scripts/card-shots.mjs <palette id> <out dir> [app url] [state id]
//
// Same app and data server as shots.mjs (this worktree's Expo web, the palette in its theme files).
import { createRequire } from "node:module";
import { existsSync, mkdirSync, readdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "../../../../..");
function playwright() {
  const lib = join(REPO, ".venv", "lib");
  for (const py of existsSync(lib) ? readdirSync(lib).filter((d) => d.startsWith("python")) : []) {
    const driver = join(lib, py, "site-packages", "playwright", "driver", "package");
    if (existsSync(join(driver, "package.json"))) return createRequire(join(driver, "x.js"))(driver);
  }
  throw new Error("no Playwright in .venv (pip install playwright)");
}
const { chromium } = playwright();
const { urlsOf } = await import(join(REPO, "app/tools/regs-web/states.mjs"));

const [ID, OUT, URL = "http://localhost:39482/", STATE = "vedder_1010"] = process.argv.slice(2);
if (!ID || !OUT) { console.error("usage: card-shots.mjs <palette id> <out dir> [app url] [state id]"); process.exit(2); }
mkdirSync(OUT, { recursive: true });
const link = (theme) => { const u = urlsOf(STATE, { app: URL, tab: "keep", theme }); return typeof u === "string" ? u : u.app; };

const browser = await chromium.launch({ headless: true });
const log = { palette: ID, state: STATE, url: URL, at: new Date().toISOString(), shots: [], errors: [] };
for (const g of [{ id: "light", scheme: "light" }, { id: "dark", scheme: "dark" }, { id: "cvd", scheme: "light", palette: "cvd" }]) {
  const ctx = await browser.newContext({ viewport: { width: 390, height: 844 }, deviceScaleFactor: 2, colorScheme: g.scheme });
  const p = await ctx.newPage();
  p.on("pageerror", (e) => log.errors.push(`${g.id}: ${String(e).slice(0, 200)}`));
  try {
    await p.goto(link(g.scheme), { waitUntil: "load", timeout: 300000 });
    await p.waitForSelector("[data-testid='st:open'], [data-testid='st:closed']", { timeout: 120000 });
    await p.waitForTimeout(2500);
    if (g.palette) {   // the colour-blind palette is a menu choice: the water's Map view, menu → Display, back to Regulations
      await p.getByRole("tab", { name: "Map", exact: true }).click();
      await p.waitForTimeout(2500);
      await p.getByLabel("Open menu", { exact: true }).click();
      await p.waitForTimeout(800);
      await p.getByRole("button", { name: "Display", exact: true }).click();
      await p.waitForTimeout(800);
      await p.getByRole("radio", { name: "Colour-blind", exact: true }).click();
      await p.waitForTimeout(800);
      await p.getByTestId("menu-page-display").getByRole("button", { name: "Back", exact: true }).click();
      await p.waitForTimeout(1500);
      await p.getByRole("tab", { name: "Regulations", exact: true }).click();
      await p.waitForTimeout(2500);
    }
    const shot = async (what) => {
      const file = `${ID}--card-${what}--${g.id}.jpg`;
      await p.screenshot({ path: join(OUT, file), type: "jpeg", quality: 82 });
      const colours = await p.evaluate(() => Object.fromEntries([...document.querySelectorAll("[data-testid^='st:']")]
        .map((e) => [e.getAttribute("data-testid"), getComputedStyle(e).color])));
      log.shots.push({ file, view: `card-${what}`, ground: g.id, colours });
    };
    await shot("keep-top");
    const found = await p.evaluate(() => { const e = document.querySelector("[data-testid='st:release']");
      if (!e) return false; e.scrollIntoView({ block: "center" }); return true; });
    if (!found) log.errors.push(`${g.id}: no release word on ${STATE}`);
    await p.waitForTimeout(1200);
    await shot("keep-release");
  } catch (e) {
    log.errors.push(`${g.id} FAILED: ${String(e).slice(0, 300)}`);
  } finally {
    await ctx.close();
  }
}
await browser.close();
writeFileSync(join(OUT, `${ID}--card-log.json`), JSON.stringify(log, null, 1));
console.log(JSON.stringify({ shots: log.shots.length, errors: log.errors }));
