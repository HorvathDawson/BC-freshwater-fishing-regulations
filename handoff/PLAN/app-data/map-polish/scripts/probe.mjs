// Lists the app's controls (roles + accessible names) on the first screen, and shoots it. Dev aid for shots.mjs.
// Usage: node probe.mjs <out dir> <app url>
import { createRequire } from "node:module";
const require = createRequire("/Users/dawson.horvath/.npm/_npx/9833c18b2d85bc59/node_modules/");
const { chromium } = require("playwright");
const exe = "/Users/dawson.horvath/Library/Caches/ms-playwright/chromium-1234/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing";
const [OUT, URL, click] = process.argv.slice(2);
const browser = await chromium.launch({ executablePath: exe, headless: true });
try {
  const ctx = await browser.newContext({ viewport: { width: 390, height: 844 }, deviceScaleFactor: 2 });
  const p = await ctx.newPage();
  p.on("pageerror", (e) => console.log("ERR", String(e).slice(0, 200)));
  await p.goto(URL, { waitUntil: "load", timeout: 300000 });
  await p.waitForFunction(() => !!globalThis.__map, null, { timeout: 300000 });
  await p.waitForTimeout(5000);
  for (const name of (click ?? "").split("|").filter(Boolean)) {
    await p.locator(`[aria-label="${name}"]`).first().click();
    await p.waitForTimeout(1500);
  }
  console.log(await p.evaluate(() => [...document.querySelectorAll("[aria-label],[role=tab],[role=button],[role=radio],[role=switch],[role=checkbox]")]
    .map((e) => `${e.getAttribute("role") ?? ""}:${e.getAttribute("aria-label") ?? e.textContent?.slice(0, 40)}`)
    .slice(0, 150).join("\n")));
  await p.screenshot({ path: `${OUT}/probe.png` });
} finally { await browser.close(); }
