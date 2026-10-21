// G1 web check: a phone-sized, touch-emulated Chromium against the worktree's expo web dev server.
import { createRequire } from "node:module";
const require = createRequire("/Users/dawson.horvath/.npm/_npx/9833c18b2d85bc59/node_modules/");
const { chromium } = require("playwright");

const OUT = process.argv[2];
const URL = process.argv[3] ?? "http://localhost:39418/";
const exe = "/Users/dawson.horvath/Library/Caches/ms-playwright/chromium-1234/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing";

const browser = await chromium.launch({ executablePath: exe, headless: true });
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

try {
  await p.goto(URL, { waitUntil: "load", timeout: 180000 });
  await p.getByLabel("Search", { exact: true }).first().waitFor({ timeout: 180000 });
  await p.waitForTimeout(3000);
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

  // ---- the Layers sheet drags closed -----------------------------------------------------------------------------
  // the ‹ returned to the Map tab already
  await p.waitForTimeout(1500);
  const layers = p.getByLabel("Choose layers").first();
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
} catch (e) {
  notes.push({ what: `FAILED: ${String(e).slice(0, 300)}` });
  await shot("99-failure").catch(() => {});
} finally {
  console.log(JSON.stringify({ notes, errors: errors.slice(0, 10) }, null, 2));
  await browser.close();
}
