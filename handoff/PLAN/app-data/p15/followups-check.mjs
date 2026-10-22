// P15 FOLLOW-UPS web check (2026-10-21): the selected part OUTLINED (its regulation colour kept), the stepped part is
// the frame's (Regulations opens on it), ▲ / ▼ across a continuation (Vedder <-> Chilliwack), and the step time on the
// web phone profile (390×844 @2x, Metal, 4G + 4× CPU, as handoff/PLAN/app-data/colour-latency).
//   node followups-check.mjs <out dir> [app url]
// The app must run against a release that ships `water_continue` (EXPO_PUBLIC_DATA_BASE at a tile server whose
// RELEASE_ROOT holds it). Prints a JSON log: each step's bar, the feature-state `selected` the map holds, and timings.
import { createRequire } from "node:module";
const require = createRequire("/Users/dawson.horvath/.npm/_npx/9833c18b2d85bc59/node_modules/");
const { chromium } = require("playwright");

const OUT = process.argv[2];
const APP = process.argv[3] ?? "http://localhost:39631/";
const exe = "/Users/dawson.horvath/Library/Caches/ms-playwright/chromium-1234/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing";
const browser = await chromium.launch({ executablePath: exe, headless: true,
  args: ["--use-angle=metal", "--enable-gpu", "--ignore-gpu-blocklist"] });
const log = [];
const errors = [];
const PHONE = { viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true, deviceScaleFactor: 2, colorScheme: "light" };

const bar = (p) => p.evaluate(() => {
  const t = (id) => document.querySelector(`[data-testid="${id}"]`)?.textContent ?? null;
  const steps = [...document.querySelectorAll('[data-testid="water-map"] [role=button]')]
    .filter((b) => /Downstream|Upstream|Lowest|Highest/.test(b.textContent ?? ""))
    .map((b) => `${b.getAttribute("aria-label")}${b.getAttribute("aria-disabled") === "true" ? " (off)" : ""}`);
  return { where: t("water-map-where"), name: t("water-map-name"), steps, exit: t("water-map-exit") };
});

/**
 * The map's selection as the GL map holds it: per source-layer, the features whose feature-state `selected` is true
 * (from every setFeatureState call since the timer was installed), and how many are `water` (the other parts).
 */
const selected = (p) => p.evaluate(() => {
  const out = {};
  for (const [k, s] of Object.entries(globalThis.__sel ?? {})) {
    const sl = k.split("|")[0];
    if (s.selected) out[`${sl}.selected`] = (out[`${sl}.selected`] ?? 0) + 1;
    if (s.water) out[`${sl}.water`] = (out[`${sl}.water`] ?? 0) + 1;
  }
  return out;
});

/** Install the step timer once: records the setFeatureState calls a step makes, and the state they leave. */
const installTimer = (p) => p.evaluate(() => {
  const m = globalThis.__map;
  if (!m || m.__stepTimer) return;
  m.__stepTimer = true;
  globalThis.__fs = [];
  globalThis.__sel = {};
  const real = m.setFeatureState.bind(m);
  m.setFeatureState = (f, s) => {
    globalThis.__fs.push(performance.now());
    const k = `${f.sourceLayer}|${f.id}`;
    globalThis.__sel[k] = { ...(globalThis.__sel[k] ?? {}), ...s };
    return real(f, s);
  };
});

/**
 * One timed step: the click (dispatched in the page), then the first frame after the bar changed AND the map's
 * selection was re-set. `ui` is that frame; `idle` is the map's next idle (the camera's ease to the part included).
 */
const timedStep = (p, aria) => p.evaluate(async (aria) => {
  const btn = [...document.querySelectorAll('[data-testid="water-map"] [role=button]')]
    .find((b) => b.getAttribute("aria-label") === aria);
  if (!btn) return { error: `no button ${aria}` };
  const where = () => document.querySelector('[data-testid="water-map-where"]')?.textContent
    + "|" + document.querySelector('[data-testid="water-map-name"]')?.textContent;
  const before = where();
  globalThis.__fs = [];
  const m = globalThis.__map;
  const idle = new Promise((r) => m.once("idle", () => r(performance.now())));
  const t0 = performance.now();
  btn.click();
  const ui = await new Promise((resolve) => {
    const tick = () => {
      if (where() !== before && globalThis.__fs.length) resolve(performance.now());
      else if (performance.now() - t0 > 5000) resolve(null);
      else requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  });
  const lastFs = globalThis.__fs.length ? Math.max(...globalThis.__fs) : null;
  const tIdle = await Promise.race([idle, new Promise((r) => setTimeout(() => r(null), 8000))]);
  return { uiMs: ui === null ? null : Math.round(ui - t0), stateSetMs: lastFs === null ? null : Math.round(lastFs - t0),
           idleMs: tIdle === null ? null : Math.round(tIdle - t0), stateCalls: globalThis.__fs.length };
}, aria);

/** Pixels of the outline colour (#c2187a), spread out, to tap the lit water. */
async function litPoints(p) {
  const png = (await p.screenshot()).toString("base64");
  return p.evaluate(async (b64) => {
    const img = new Image();
    img.src = `data:image/png;base64,${b64}`;
    await img.decode();
    const c = document.createElement("canvas");
    c.width = img.width; c.height = img.height;
    const g = c.getContext("2d");
    g.drawImage(img, 0, 0);
    const d = g.getImageData(0, 0, c.width, c.height).data;
    const sc = img.width / innerWidth;
    const hits = [];
    for (let y = Math.round(120 * sc); y < c.height - 160 * sc; y += 2)
      for (let x = Math.round(30 * sc); x < c.width - 30 * sc; x += 2) {
        const i = (y * c.width + x) * 4;
        const [r, gg, b] = [d[i], d[i + 1], d[i + 2]];
        if (r > 140 && r - gg > 40 && b - gg > 20 && r - b > 10) hits.push([x / sc, y / sc]);
      }
    const out = [];
    for (let k = 1; k <= 8; k += 1) if (hits.length) out.push(hits[Math.floor((hits.length * k) / 9)]);
    return out;
  }, png);
}

async function openWaterMap(p, name) {
  await p.getByRole("tab", { name: "Search" }).first().click();
  const box = p.getByLabel("Search for a river, lake or town");
  await box.fill(name);
  const re = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  await p.getByRole("button", { name: new RegExp(`^Show ${re}(,| on the map)`) }).first().click();
  const eye = p.getByRole("button", { name: new RegExp(`^View ${re}(,|$)`) }).first();
  await eye.waitFor({ timeout: 60000 });
  await eye.click();
  await p.waitForTimeout(6000);
  for (const at of await litPoints(p)) {
    await p.mouse.click(at[0], at[1]);
    try {
      await p.locator('[role=tablist][aria-label="Views of this water"]').first().waitFor({ timeout: 8000 });
    } catch { continue; }
    await p.waitForTimeout(1500);
    if (await p.getByText(name, { exact: true }).count()) break;
    await p.getByRole("button", { name: "Back" }).first().click();
    await p.waitForTimeout(2500);
  }
  await p.locator('[role=tablist][aria-label="Views of this water"] [role=tab][aria-label="Map"]').first().click();
  await p.getByTestId("water-map-where").waitFor({ timeout: 60000 });
  await p.waitForTimeout(5000);
}

/** The Regulations view's part as the reader sees it: the selected tab and the part named at the top of the page. */
const regsPart = (p) => p.evaluate(() => {
  const tabs = [...document.querySelectorAll('[role=tablist][aria-label="Views of this water"] [role=tab]')]
    .map((t) => t.getAttribute("aria-label") + (t.getAttribute("aria-selected") === "true" ? "*" : "")).join(" · ");
  const sel = [...document.querySelectorAll('[aria-selected="true"], [aria-current="true"]')]
    .map((e) => e.getAttribute("aria-label") ?? e.textContent).filter((t) => t && !/^(Regulations|Map|Conditions)$/.test(t));
  return { tabs, selectedPart: sel.slice(0, 3) };
});

async function app(name, slug, steps) {
  const ctx = await browser.newContext(PHONE);
  const p = await ctx.newPage();
  const cdp = await ctx.newCDPSession(p);
  await cdp.send("Network.enable");
  await cdp.send("Network.emulateNetworkConditions", { offline: false, latency: 120,
    downloadThroughput: 9e6 / 8, uploadThroughput: 3e6 / 8 });
  p.on("pageerror", (e) => errors.push(`${slug}: ${String(e).slice(0, 200)}`));
  const shot = async (n) => { await p.waitForTimeout(2500); await p.screenshot({ path: `${OUT}/fu-${slug}-${n}.png` }); };
  const note = async (what, extra = {}) => log.push({ water: slug, what, ...(await bar(p)), selected: await selected(p), ...extra });
  const throttle = (rate) => cdp.send("Emulation.setCPUThrottlingRate", { rate });
  try {
    await p.goto(APP, { waitUntil: "load", timeout: 180000 });
    await p.getByRole("tab", { name: "Search" }).first().waitFor({ timeout: 180000 });
    await p.waitForTimeout(3000);
    await openWaterMap(p, name);
    await installTimer(p);
    await note("opened");
    await shot("1-opened");
    await steps({ p, shot, note, throttle });
  } catch (e) {
    log.push({ water: slug, what: `FAILED: ${String(e).slice(0, 300)}` });
    await p.screenshot({ path: `${OUT}/fu-${slug}-99-failure.png` }).catch(() => {});
  } finally { await ctx.close(); }
}

const timings = [];
/** Step by the button whose aria-label matches `re` (the plain step or the one into another water), timed, at 4× CPU. */
async function step(p, note, throttle, re, what) {
  const b = await bar(p);
  const aria = b.steps.find((s) => re.test(s) && !s.endsWith("(off)"));
  if (!aria) return false;
  await throttle(4);
  const t = await timedStep(p, aria);
  await throttle(1);
  timings.push({ what, ...t });
  await p.waitForTimeout(1500);
  await note(what, { timing: t });
  return true;
}
const DOWN = /^▼ Downstream$/;
const UP = /^▲ Upstream$/;

await app("Chilliwack River", "chilliwack", async ({ p, shot, note, throttle }) => {
  let k = 0;
  while (k < 4 && await step(p, note, throttle, DOWN, `▼ ${++k}`));
  await shot("2-mouth");
  for (let i = 1; i <= 3; i += 1) { await step(p, note, throttle, UP, `▲ ${i}`); await shot(`3-up${i}`); }
  // ONE WATER SCREEN: Regulations now opens on the stepped (top) part
  await p.locator('[role=tablist][aria-label="Views of this water"] [role=tab][aria-label="Regulations"]').first().click();
  await p.waitForTimeout(4000);
  log.push({ water: "chilliwack", what: "Regulations after ▲ ×3", ...(await regsPart(p)) });
  await shot("4-regs-after-steps");
  await p.locator('[role=tablist][aria-label="Views of this water"] [role=tab][aria-label="Map"]').first().click();
  await p.getByTestId("water-map-where").waitFor({ timeout: 60000 });
  await p.waitForTimeout(3000);
  await note("Map again");
  // back to the lowest part, then ▼ INTO the Vedder (the continuation)
  k = 0;
  while (k < 4 && await step(p, note, throttle, DOWN, `▼ ${++k} (back down)`));
  await shot("5-lowest");
  await step(p, note, throttle, /^▼ Downstream into /, "▼ into the next water");
  await p.waitForTimeout(3000);
  await note("after ▼ into");
  await shot("6-into-vedder");
  await step(p, note, throttle, /^▲ Upstream into /, "▲ into the water above");
  await p.waitForTimeout(3000);
  await note("after ▲ into");
  await shot("7-back-up-into-chilliwack");
});

await app("Cowichan River", "cowichan", async ({ p, shot, note, throttle }) => {
  let k = 0;
  while (k < 4 && await step(p, note, throttle, DOWN, `▼ ${++k}`));
  await shot("2-mouth");
  for (let i = 1; i <= 3; i += 1) { await step(p, note, throttle, UP, `▲ ${i}`); await shot(`3-up${i}`); }
});

await app("Kootenay Lake — Main Body", "klwa", async ({ p, shot, note, throttle }) => {
  for (let i = 1; i <= 2; i += 1) { if (await step(p, note, throttle, DOWN, `▼ ${i}`)) await shot(`2-down${i}`); }
  for (let i = 1; i <= 2; i += 1) await step(p, note, throttle, UP, `▲ ${i}`);
  await shot("3-up-again");
});

await app("Vedder River", "vedder", async ({ p, shot, note, throttle }) => {
  await step(p, note, throttle, /^▲ Upstream into /, "▲ into the Chilliwack");
  await p.waitForTimeout(3000);
  await note("after ▲ into");
  await shot("2-up-into-chilliwack");
});

await browser.close();
const ui = timings.map((t) => t.uiMs).filter((v) => v !== null).sort((a, b) => a - b);
const q = (f) => ui[Math.min(ui.length - 1, Math.floor(f * ui.length))];
console.log(JSON.stringify({ log, errors, timings,
  stepUiMs: { n: ui.length, median: q(0.5), p95: q(0.95), max: ui[ui.length - 1] } }, null, 1));
