// view-swipe web check: a phone-sized, touch-emulated Chromium (and a desktop window) against the worktree's expo web
// dev server. Usage: node web-check.mjs <out dir> [url]. Prints a JSON log of what each step saw.
import { createRequire } from "node:module";
const require = createRequire("/Users/dawson.horvath/.npm/_npx/9833c18b2d85bc59/node_modules/");
const { chromium } = require("playwright");

const OUT = process.argv[2];
const URL = process.argv[3] ?? "http://localhost:39561/";
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

/** A y on the Conditions body that is text (the reading or the estimate), clear of the maps and the chart. */
const textY = async (p) => p.evaluate(() => {
  const avoid = [...document.querySelectorAll("canvas, svg")].map((e) => e.getBoundingClientRect());
  const strip = document.querySelector('[role=tablist][aria-label="Views of this water"]').getBoundingClientRect();
  for (const t of document.querySelectorAll("div[dir=auto]")) {
    const r = t.getBoundingClientRect();
    const y = r.top + r.height / 2;
    if (r.height < 10 || r.width < 150 || y < strip.bottom + 40 || y > innerHeight - 120) continue;
    if (avoid.some((a) => y > a.top - 8 && y < a.bottom + 8)) continue;
    return y;
  }
  return 500;
});
/** The centre of the first map canvas on the Conditions body, if one is in view. */
const minimap = async (p) => p.evaluate(async () => {
  const small = [...document.querySelectorAll("canvas")].find((c) => {
    const r = c.getBoundingClientRect();
    return r.width > 0 && r.height > 80 && r.height < 400;
  });
  if (small) { small.scrollIntoView({ block: "center" }); await new Promise((r) => setTimeout(r, 600)); }
  for (const c of document.querySelectorAll("canvas")) {
    const r = c.getBoundingClientRect();
    if (r.height > 80 && r.height < 400 && r.top > 150 && r.bottom < innerHeight - 20) return { x: r.left + r.width / 2, y: r.top + r.height / 2 };
  }
  return null;
});

/** The centre of the hydrograph's plot (the tallest svg), scrolled into view, if there is one. */
const plot = async (p) => p.evaluate(async () => {
  const svg = [...document.querySelectorAll("svg")].filter((e) => e.getBoundingClientRect().height > 100)
    .sort((a, b) => b.getBoundingClientRect().height - a.getBoundingClientRect().height)[0];
  if (!svg) return null;
  svg.scrollIntoView({ block: "center" }); await new Promise((r) => setTimeout(r, 600));
  const r = svg.getBoundingClientRect();
  return { x: r.left + r.width / 2, y: r.top + r.height / 2 };
});

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
    // review fix: the Conditions BODY swipes (not only its header); its maps and chart keep their own drags
    const body = await textY(p);
    await touchDrag(80, body, 330, body);                     // finger right on the reach's body text: its regulations
    await p.getByText("Regulations are coming").first().waitFor({ timeout: 30000 });
    await note(`swipe right on the reach's BODY (text at y=${Math.round(body)})`); await shot("02-regs");
    await touchDrag(320, 600, 60, 610);                       // Regs -> Conditions
    await p.waitForTimeout(2500);
    const m = await minimap(p);
    if (m) {
      await touchDrag(m.x + 80, m.y, m.x - 80, m.y);           // a pan on the Conditions minimap: stays on Conditions
      await note("pan left on the Conditions minimap (must stay on Conditions)");
    } else notes.push({ session: `phone-${scheme}`, what: "no minimap in view to pan" });
    const c = await plot(p);
    if (c) {
      await touchDrag(c.x + 80, c.y, c.x - 80, c.y);           // a drag on the chart's plot: stays on Conditions
      await note("drag left on the Conditions chart plot (must stay on Conditions)");
    } else notes.push({ session: `phone-${scheme}`, what: "no chart in view to drag" });
    const b2 = await textY(p);
    await touchDrag(330, b2, 80, b2);                         // finger left on the Conditions body: Map
    await p.waitForTimeout(3500);
    await note("swipe left on the water's Conditions BODY"); await shot("03b-map-from-body");
    let h = await header(p);
    await touchDrag(60, h.y, 320, h.y);                        // Map header -> Conditions
    await p.waitForTimeout(2500);
    const b3 = await textY(p);
    await touchDrag(80, b3, 330, b3);                          // Conditions body -> Regulations
    await p.getByText("Regulations are coming").first().waitFor({ timeout: 30000 });
    await note("swipe right on the water's Conditions BODY");
    // the original walk, from the header, as before
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
