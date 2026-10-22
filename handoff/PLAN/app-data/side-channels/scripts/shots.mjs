// Conditions-panel shots for the side-channel report: phone-sized headless Chromium against this worktree's Expo web
// server (own ports), data from the main checkout's serve-tiles.mjs pinned to the live release.
// Usage: node shots.mjs <out dir> <url> "<search text>|<result label starts with>|<file stem>|x,y;x,y" ...
// Route: Search -> the result's eye (frames the water on the map) -> the Conditions tab -> a tap on the water at the
// first CSS point that opens it.
import { createRequire } from "node:module";
const require = createRequire("/Users/dawson.horvath/.npm/_npx/9833c18b2d85bc59/node_modules/");
const { chromium } = require("playwright");

const [OUT, URL, ...cases] = process.argv.slice(2);
const exe = "/Users/dawson.horvath/Library/Caches/ms-playwright/chromium-1234/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing";
const browser = await chromium.launch({ executablePath: exe, headless: true });
const log = [];

const labels = (p) => p.evaluate(() => [...document.querySelectorAll("[aria-label]")]
  .map((e) => e.getAttribute("aria-label")).filter(Boolean).slice(0, 80));

for (const c of cases) {
  const [query, result, stem, tap] = c.split("|");
  const ctx = await browser.newContext({ viewport: { width: 420, height: 900 }, deviceScaleFactor: 2 });
  const p = await ctx.newPage();
  p.on("pageerror", (e) => log.push({ stem, error: String(e).slice(0, 200) }));
  try {
    await p.goto(URL, { waitUntil: "load", timeout: 180000 });
    await p.getByRole("tab", { name: "Search" }).first().waitFor({ timeout: 180000 });
    await p.waitForTimeout(3000);
    await p.getByRole("tab", { name: "Search" }).first().click();
    await p.getByRole("textbox").first().fill(query);
    await p.waitForTimeout(2500);
    const btn = p.locator(`[aria-label^="Show ${result}"]`).first();
    await btn.click();
    await p.waitForTimeout(3000);
    log.push({ stem, after: "result", labels: await labels(p) });
    // the water screen: its views strip names a Conditions view
    // the eye: opens the water's own screen
    await p.locator(`[aria-label^="View ${result}"]`).first().click();
    await p.waitForTimeout(4000);
    // the map is now framed on the water: switch the map to Conditions and tap the water at the given point
    await p.getByRole("tab", { name: "Conditions" }).first().click();
    await p.waitForTimeout(6000);
    await p.screenshot({ path: `${OUT}/${stem}-map.png` });
    for (const xy of (tap || "").split(";").filter(Boolean)) {
      const [x, y] = xy.split(",").map(Number);
      await p.mouse.click(x, y);
      await p.waitForTimeout(1500);
      if (await p.locator('[role=tablist][aria-label="Views of this water"]').count()) break;
    }
    await p.waitForTimeout(8000);
    log.push({ stem, after: "conditions", labels: await labels(p) });
    await p.screenshot({ path: `${OUT}/${stem}.png` });
    await p.screenshot({ path: `${OUT}/${stem}-full.png`, fullPage: true });
  } catch (e) {
    log.push({ stem, failed: String(e).slice(0, 300) });
    await p.screenshot({ path: `${OUT}/${stem}-failure.png` }).catch(() => {});
  } finally {
    await ctx.close();
  }
}
await browser.close();
console.log(JSON.stringify(log, null, 1));
