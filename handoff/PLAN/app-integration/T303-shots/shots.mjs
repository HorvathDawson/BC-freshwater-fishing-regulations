// T303 shots: my location on the web map (headless Chromium, mocked geolocation; never a visible window).
//   node shots.mjs <width> <height> <out dir>   (app at http://localhost:39310)
import { createRequire } from "node:module";
import { existsSync, readdirSync, mkdirSync } from "node:fs";
import { join } from "node:path";
const lib = join("/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations", ".venv", "lib");
let pw;
for (const py of readdirSync(lib).filter((d) => d.startsWith("python"))) {
  const driver = join(lib, py, "site-packages", "playwright", "driver", "package");
  if (existsSync(join(driver, "package.json"))) pw = createRequire(join(driver, "x.js"))(driver);
}
const { chromium } = pw;
const [W = "390", H = "844", OUT = "out"] = process.argv.slice(2);
mkdirSync(OUT, { recursive: true });
const tag = +W > 600 ? "desktop" : "phone";
const b = await chromium.launch({ headless: true });
const run = async (name, granted) => {
  const ctx = await b.newContext({ viewport: { width: +W, height: +H }, geolocation: { latitude: 49.1, longitude: -121.95, accuracy: 400 },
                                   permissions: granted ? ["geolocation"] : [] });
  // the walkthrough is seen (only the locate hint is new): the first-run notice is accepted below
  await ctx.addInitScript(() => { try { if (!localStorage.getItem("seeded")) { localStorage.setItem("canifishthis.walkthrough.v1", "walkthrough-3"); localStorage.setItem("seeded", "1"); } } catch {} });
  const p = await ctx.newPage();
  p.on("pageerror", (e) => console.log("PAGEERR", e.message.slice(0, 300)));
  let n = 0;
  const shot = async (s) => p.screenshot({ path: `${OUT}/${tag}-${name}-${String(++n).padStart(2, "0")}-${s}.png` });
  const wait = (ms) => p.waitForTimeout(ms);
  await p.goto("http://localhost:39310", { waitUntil: "load", timeout: 300000 });
  await wait(8000);
  await p.getByText("I understand").click().catch(() => {});
  await wait(3000);
  const btn = p.getByRole("button", { name: /location/i });
  console.log(name, "button count", await btn.count());
  await shot("hint");                                       // the first-use hint, if the walkthrough/notice let it open
  const gotIt = p.getByText(/^(Got it|Done|Close|OK)$/);
  if (await gotIt.count()) await gotIt.first().click().catch(() => {});
  await wait(800);
  await shot("map-button");
  const hasGeoCalls = () => p.evaluate(() => window.__geo ?? 0);
  await btn.first().click();                                 // tap 1: asks, dot + centre
  await wait(4000);
  console.log(name, "dot", await p.locator('[data-testid="my-location"]').count(), "notice", await p.locator('[data-testid="my-location-notice"]').count());
  console.log(name, "labels", await p.getByRole("button", { name: /location/i }).evaluateAll((e) => e.map((x) => x.getAttribute("aria-label"))));
  await shot("tap1-dot-centre");
  if (granted) {
    await p.getByRole("button", { name: /location/i }).first().click();  // tap 2: follow
    await wait(1500);
    console.log(name, "labels2", await p.getByRole("button", { name: /location/i }).evaluateAll((e) => e.map((x) => x.getAttribute("aria-label"))));
    await ctx.setGeolocation({ latitude: 49.12, longitude: -121.9, accuracy: 30 });   // moves: the map follows
    await wait(3500);
    await shot("tap2-following-moved");
    await p.getByRole("button", { name: /location/i }).first().click();  // tap 3: stop following
    await wait(1000);
    console.log(name, "labels3", await p.getByRole("button", { name: /location/i }).evaluateAll((e) => e.map((x) => x.getAttribute("aria-label"))));
    await shot("tap3-stopped");
  }
  await ctx.close();
};
await run("granted", true);
await run("denied", false);
await b.close();
