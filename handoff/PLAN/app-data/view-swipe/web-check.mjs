// view-swipe web check: a phone-sized, touch-emulated Chromium (and a desktop window) against the worktree's expo web
// dev server. Usage: node web-check.mjs <out dir> [url]. Prints a JSON log of what each step saw.
import { createRequire } from "node:module";
const require = createRequire("/Users/dawson.horvath/.npm/_npx/9833c18b2d85bc59/node_modules/");
const { chromium } = require("playwright");

const OUT = process.argv[2];
const URL = process.argv[3] ?? "http://localhost:39431/";
const exe = "/Users/dawson.horvath/Library/Caches/ms-playwright/chromium-1234/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing";
const browser = await chromium.launch({ executablePath: exe, headless: true });
const notes = [];
const errors = [];

async function session(name, { width, height, touch, scheme }, steps) {
  const ctx = await browser.newContext({ viewport: { width, height }, hasTouch: touch, isMobile: touch,
                                         deviceScaleFactor: 2, colorScheme: scheme });
  const p = await ctx.newPage();
  p.on("pageerror", (e) => errors.push(`${name}: ${String(e).slice(0, 200)}`));
  const cdp = await ctx.newCDPSession(p);
  const touchDrag = async (x0, y0, x1, y1, steps = 12, ms = 16) => {
    await cdp.send("Input.dispatchTouchEvent", { type: "touchStart", touchPoints: [{ x: x0, y: y0 }] });
    for (let i = 1; i <= steps; i++) {
      await cdp.send("Input.dispatchTouchEvent", { type: "touchMove",
        touchPoints: [{ x: x0 + ((x1 - x0) * i) / steps, y: y0 + ((y1 - y0) * i) / steps }] });
      await p.waitForTimeout(ms);
    }
    await cdp.send("Input.dispatchTouchEvent", { type: "touchEnd", touchPoints: [] });
    await p.waitForTimeout(700);
  };
  const shot = (n) => p.screenshot({ path: `${OUT}/${name}-${n}.png` });
  // the selected view in the strip, the back row's words, whether the bottom bar shows
  const state = async () => p.evaluate(() => {
    const strip = document.querySelector('[role=tablist][aria-label="Views of this water"]');
    const tabs = strip ? [...strip.querySelectorAll("[role=tab]")].map((t) =>
      t.getAttribute("aria-label") + (t.getAttribute("aria-selected") === "true" ? "*" : "")).join(" · ") : "(no strip)";
    const back = [...document.querySelectorAll('[aria-label="Back"]')].map((b) => b.textContent).join();
    return { tabs, back, bar: !!document.querySelector('[role=tab][aria-label="Spots"]') };
  });
  const note = async (what) => notes.push({ session: name, what, ...(await state()) });
  try {
    await p.goto(URL, { waitUntil: "load", timeout: 180000 });
    await p.getByLabel("Search", { exact: true }).first().waitFor({ timeout: 180000 });
    await p.waitForTimeout(3000);
    await steps({ p, touchDrag, shot, note, state });
  } catch (e) {
    notes.push({ session: name, what: `FAILED: ${String(e).slice(0, 300)}` });
    await shot("99-failure").catch(() => {});
  } finally {
    await ctx.close();
  }
}

/** Open a Conditions reach: the Conditions tab, then a tap down the middle of the map until a reach opens. */
async function openReach({ p, note, shot }) {
  await p.getByRole("tab", { name: "Conditions" }).first().click();
  await p.waitForTimeout(4000);
  const box = await p.locator("canvas").first().boundingBox();
  const cx = box.x + box.width / 2, cy = box.y + box.height / 2;
  for (let dy = -120; dy <= 120; dy += 4) {
    for (const dx of [0, -40, 40]) {
      await p.mouse.click(cx + dx, cy + dy);
      await p.waitForTimeout(250);
      if (await p.locator('[role=tablist][aria-label="Views of this water"]').count()) {
        await p.waitForTimeout(2500);
        await note(`a Conditions reach opened (tap at ${dx}, ${dy} from the map's centre)`);
        await shot("01-reach");
        return true;
      }
    }
  }
  return false;
}

const header = async (p) => {
  const b = await p.locator('[role=tablist][aria-label="Views of this water"]').first().boundingBox();
  return { x: b.x + b.width / 2, y: b.y + b.height / 2, w: b.width };
};

// ---- phone, touch: reach -> Regs -> Conditions -> Map -> Conditions -> Regs, back, back ------------------------------
for (const scheme of ["light", "dark"]) {
  await session(`phone-${scheme}`, { width: 390, height: 844, touch: true, scheme }, async (s) => {
    const { p, touchDrag, shot, note } = s;
    if (!(await openReach(s))) { notes.push({ session: `phone-${scheme}`, what: "no reach found to tap" }); return; }
    if (scheme === "dark") {
      // dark: the screens only (the swipe sequence is the light run's)
      await p.getByRole("tab", { name: "Regulations" }).first().click();
      await p.getByText("Regulations are coming").first().waitFor({ timeout: 30000 });
      await shot("02-regs");
      await p.getByRole("tab", { name: "Map" }).first().click();
      await p.waitForTimeout(4000);
      await shot("04-map");
      return;
    }
    let h = await header(p);
    await touchDrag(80, h.y, 330, h.y);                        // finger right on the reach's header: its regulations
    await p.getByText("Regulations are coming").first().waitFor({ timeout: 30000 });
    await note("swipe right on the reach's header"); await shot("02-regs");
    await touchDrag(320, 600, 60, 610);                        // finger left anywhere on the Regulations view
    await p.waitForTimeout(2500);
    await note("swipe left on the Regulations body"); await shot("03-conditions");
    h = await header(p);
    await touchDrag(320, h.y, 60, h.y);                        // finger left on the Conditions header
    await p.waitForTimeout(3500);
    await note("swipe left on the Conditions header"); await shot("04-map");
    // a pan on the Map canvas must PAN, not change the view
    await touchDrag(320, 600, 80, 600);
    await note("pan left on the map canvas (must stay on Map)");
    h = await header(p);
    await touchDrag(60, h.y, 320, h.y);                        // finger right on the Map header
    await p.waitForTimeout(2500);
    await note("swipe right on the Map header");
    // a vertical scroll on Conditions must not change the view (direction lock)
    await touchDrag(200, 700, 220, 300);
    await note("vertical scroll on Conditions (must stay)");
    h = await header(p);
    await touchDrag(60, h.y, 320, h.y);
    await p.getByText("Regulations are coming").first().waitFor({ timeout: 30000 });
    await note("swipe right on the Conditions header");
    await touchDrag(200, 600, 360, 600);                       // at the left end: nothing (no wrap)
    await note("swipe right on the Regulations body (the first view: stays)");
    await p.getByLabel("Back").first().click();
    await p.waitForTimeout(2500);
    await note("‹ from the Regulations view (back to the reach)"); await shot("05-back-to-reach");
  });
}

// ---- desktop: the same views at 1440x900 --------------------------------------------------------------------------
for (const scheme of ["light", "dark"]) {
  await session(`desktop-${scheme}`, { width: 1440, height: 900, touch: false, scheme }, async (s) => {
    const { p, shot } = s;
    if (!(await openReach(s))) { notes.push({ session: `desktop-${scheme}`, what: "no reach found to tap" }); return; }
    await p.getByRole("tab", { name: "Regulations" }).first().click();
    await p.getByText("Regulations are coming").first().waitFor({ timeout: 30000 });
    await s.note("Regulations from the strip"); await shot("02-regs");
    await p.getByRole("tab", { name: "Conditions" }).first().click();
    await p.waitForTimeout(2500); await shot("03-conditions");
    await p.getByRole("tab", { name: "Map" }).first().click();
    await p.waitForTimeout(4000); await s.note("Map from the strip"); await shot("04-map");
  });
}

console.log(JSON.stringify({ notes, errors: errors.slice(0, 10) }, null, 2));
await browser.close();
