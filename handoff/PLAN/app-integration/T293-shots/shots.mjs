// T293 shots: the guided walkthrough end to end on the web app (headless), every step, then Help, replay, back, Skip, the Salmon hint, the Conditions map tour.
//   node shots.mjs <width> <height> <out dir>   (app at http://localhost:39301; Playwright from the main checkout's .venv)
import { createRequire } from "node:module";
import { existsSync, readdirSync } from "node:fs";
import { join } from "node:path";
const lib = join("/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations", ".venv", "lib");
let pw;
for (const py of readdirSync(lib).filter((d) => d.startsWith("python"))) {
  const driver = join(lib, py, "site-packages", "playwright", "driver", "package");
  if (existsSync(join(driver, "package.json"))) pw = createRequire(join(driver, "x.js"))(driver);
}
const { chromium } = pw;
import { mkdirSync } from "node:fs";
const [W = "390", H = "844", OUT = "out"] = process.argv.slice(2);
mkdirSync(OUT, { recursive: true });
const b = await chromium.launch({ headless: true });
const ctx = await b.newContext({ viewport: { width: +W, height: +H } });
if (process.env.START) await ctx.addInitScript((k) => { if (!sessionStorage.getItem("seeded")) { localStorage.setItem("canifishthis.walkthrough.v1", "guide-at:" + k); sessionStorage.setItem("seeded", "1"); } }, process.env.START);
const p = await ctx.newPage();
p.on("pageerror", (e) => console.log("PAGEERR", e.message.slice(0, 300)));
await p.goto("http://localhost:39301", { waitUntil: "load", timeout: 300000 });
await p.waitForTimeout(8000);
await p.getByText("I understand").click();
await p.waitForTimeout(2500);
let n = 0;
const tag = W > 600 ? "desktop" : "phone";
const shot = async (name) => { await p.screenshot({ path: `${OUT}/${tag}-${String(++n).padStart(2, "0")}-${name}.png` }); };
const step = async () => p.evaluate(() => document.querySelector('[data-testid^="guide-step-"]')?.getAttribute("data-testid")?.slice(11) ?? null);
const primary = () => p.locator('[data-testid="guide-primary"] [role="button"], [data-testid="guide-primary"] button').first().click();
const wait = (ms) => p.waitForTimeout(ms);
let last = null, same = 0;
for (let k = 0; k < 40; k++) {
  const s = await step();
  if (!s) { console.log("guide closed"); break; }
  if (s === last) { if (++same > 2) { console.log("stuck on", s); await shot("stuck-" + s); break; } } else same = 0;
  last = s;
  await wait(900);
  await shot(s);
  console.log("step", s, "|", await p.evaluate(() => document.querySelector('[data-testid="guide-body"]')?.textContent?.slice(0, 80)));
  if (s === "search") { await p.locator('[data-testid="search-field"] input').click(); await p.keyboard.type("ved", { delay: 80 }); await wait(2500); continue; }
  if (s === "pick") { await p.getByRole("button", { name: /^Show Vedder River,/ }).first().click(); await wait(3000); continue; }
  if (s === "conditions") { await p.getByRole("tab", { name: "Conditions" }).first().click(); await wait(5000); continue; }
  if (s === "capture") {
    // step 1: the water near the pin — a tap at the pin (the map opens on it)
    for (let j = 0; j < 8; j++) {
      const save = p.getByRole("button", { name: /^Save/ });
      if (await save.count()) { await shot("capture-details"); await save.first().click(); await wait(3500); break; }
      const txt = await p.evaluate(() => document.body.innerText);
      if (/Pick the water/.test(txt)) {
        // tap along the river at the foot of the capture map until a water is picked (its Next turns on)
        const enabledNext = async () => { for (const c of await p.getByRole("button", { name: "Next" }).all()) {
          if (!(await c.evaluate((el) => !!el.closest('[data-testid="guide"]'))) && !(await c.isDisabled())) return true; } return false; };
        const pts = +W > 600 ? [[760, 612], [700, 640], [820, 603], [1000, 597], [650, 668]]
          : [700, 680, 660, 640, 620, 600, 580].flatMap((y) => [60, 120, 180, 240, 300].map((x) => [x, y]));
        outer: for (const [x, y] of pts) {
          await p.mouse.click(x, y); await wait(500);
          if (await enabledNext()) { console.log("picked at", x, y); break outer; }
        }
      }
      const cands = await p.getByRole("button", { name: "Next" }).all();
      let clicked = false;
      for (const c of cands) { const inGuide = await c.evaluate((el) => !!el.closest('[data-testid="guide"]')); const dis = await c.isDisabled(); if (!inGuide && !dis) { await c.click(); clicked = true; break; } }
      await wait(3500); await shot("capture-" + j);
      if (!clicked) console.log("capture: no enabled Next at", j);
    }
    continue;
  }
  if (s === "open") { await p.getByRole("tab", { name: /Spots/i }).first().click().catch(() => p.getByText("SPOTS").click()); await wait(1500); await shot("spots-list");
    await p.locator('[data-testid^="spot-row-"]').first().click().catch(async () => { console.log("no spot-row testid"); }); await wait(3000); continue; }
  await primary();
  await wait(1800);
}
// AFTER THE WALK: Help (its short pages), replay, the one back stepping back, Skip, then the Salmon hint
await wait(1500);
await p.getByText("MORE", { exact: true }).click().catch(() => p.getByRole("tab", { name: /More/i }).first().click()); await wait(1500);
await p.getByText("Help", { exact: true }).first().click(); await wait(2000);
await shot("help-page");
await p.getByText("Replay the walkthrough").click(); await wait(2500);
console.log("replay ->", await step()); await shot("replay-welcome");
await primary(); await wait(1500); console.log("next ->", await step());
await p.goBack(); await wait(1500); console.log("system back ->", await step()); await shot("back-steps-back");
await p.locator('[data-testid="guide-skip"]').click(); await wait(1000); console.log("skip ->", await step());
await p.getByRole("radio", { name: /Salmon/ }).first().click(); await wait(6000);
console.log("salmon hint:", await p.evaluate(() => !!document.querySelector('[data-testid^="coach-step-salmon"]'))); await shot("salmon-hint");
await p.getByText("Got it").click().catch(() => {}); await wait(800);
// THE CONDITIONS MAP'S TOUR: the camera out to a watershed, in to its gauge, then the colours, taps and change
await p.getByRole("tab", { name: /^Conditions/ }).first().click(); await wait(4000);
for (let k = 0; k < 6; k++) {
  const key = await p.evaluate(() => document.querySelector('[data-testid^="coach-step-"]')?.getAttribute("data-testid")?.slice(11) ?? null);
  if (!key) break;
  await wait(4500); await shot("conditions-tour-" + key); console.log("cond tour", key);
  const btn = p.getByRole("button", { name: /^(Next|Got it)$/ }).last(); await btn.click(); await wait(1200);
}
await b.close();
