// P15 web check: a phone-sized, touch-emulated Chromium against the worktree's expo web dev server (release
// r-1bb06fa28daf from the tile server), and the prototype's water map beside it. Usage:
//   node web-check.mjs <out dir> [app url] [prototype url]
// Prints a JSON log of what each step saw (the bar's words, the marks, the buttons' states).
import { createRequire } from "node:module";
const require = createRequire("/Users/dawson.horvath/.npm/_npx/9833c18b2d85bc59/node_modules/");
const { chromium } = require("playwright");

const OUT = process.argv[2];
const APP = process.argv[3] ?? "http://localhost:39631/";
const PROTO = process.argv[4] ?? "http://127.0.0.1:39632/regs-v11.html";
const exe = "/Users/dawson.horvath/Library/Caches/ms-playwright/chromium-1234/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing";
const browser = await chromium.launch({ executablePath: exe, headless: true });
const log = [];
const errors = [];
const PHONE = { viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true, deviceScaleFactor: 2, colorScheme: "light" };

/** The water map's bar and marks as the reader sees them. */
const appState = (p) => p.evaluate(() => {
  const t = (id) => document.querySelector(`[data-testid="${id}"]`)?.textContent ?? null;
  const steps = [...document.querySelectorAll('[data-testid="water-map"] [role=button]')]
    .filter((b) => /Downstream|Upstream|Lowest|Highest/.test(b.textContent ?? ""))
    .map((b) => `${b.textContent}${b.getAttribute("aria-disabled") === "true" ? " (off)" : ""}`);
  const marks = [...document.querySelectorAll("button[data-mark]")].map((b) => b.getAttribute("aria-label")
    + (b.querySelector("span") ? ` [${b.querySelector("span").textContent}]` : "") + (b.getAttribute("data-mark") === "on" ? " (open)" : ""));
  return { where: t("water-map-where"), name: t("water-map-name"), steps, exit: t("water-map-exit"),
           whole: t("water-map-whole"), status: t("water-map-status"), marks,
           pop: t("water-map-mark"), chooser: t("water-map-chooser") };
});

/** Pixels of the highlight colour (#c2187a; a lake's translucent fill of it) on the map in a screenshot, spread out. */
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
  // a row is a look (it lights the water on the pinned map); its eye is the way to the main map
  const re = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  await p.getByRole("button", { name: new RegExp(`^Show ${re}(,| on the map)`) }).first().click();
  const eye = p.getByRole("button", { name: new RegExp(`^View ${re}(,|$)`) }).first();
  await eye.waitFor({ timeout: 60000 });
  await eye.click();
  await p.waitForTimeout(6000);
  // tap the lit water until THIS water opens (a tap beside it can open a neighbour: back, and the next point)
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

async function app(name, slug, steps) {
  const ctx = await browser.newContext(PHONE);
  const p = await ctx.newPage();
  p.on("pageerror", (e) => errors.push(`${slug}: ${String(e).slice(0, 200)}`));
  const shot = async (n) => { await p.waitForTimeout(2500); await p.screenshot({ path: `${OUT}/app-${slug}-${n}.png` }); };
  const note = async (what) => log.push({ water: slug, what, ...(await appState(p)) });
  try {
    await p.goto(APP, { waitUntil: "load", timeout: 180000 });
    await p.getByRole("tab", { name: "Search" }).first().waitFor({ timeout: 180000 });
    await p.waitForTimeout(3000);
    await openWaterMap(p, name);
    await note("opened");
    await shot("1-opened");
    await steps({ p, shot, note });
  } catch (e) {
    log.push({ water: slug, what: `FAILED: ${String(e).slice(0, 300)}` });
    await p.screenshot({ path: `${OUT}/app-${slug}-99-failure.png` }).catch(() => {});
  } finally { await ctx.close(); }
}

async function proto(slug, w, mps) {
  const ctx = await browser.newContext(PHONE);
  const p = await ctx.newPage();
  try {
    await p.goto(`${PROTO}#w=${w}&map=1&shot=1`, { waitUntil: "load" });
    await p.waitForTimeout(800);
    // to the lowest part, then ▲ one part at a time
    for (let k = 0; k < 8; k += 1) {
      const b = p.locator('.mstep button[data-step="-1"]:not([disabled])');
      if (!(await b.count())) break;
      await b.click();
      await p.waitForTimeout(300);
    }
    for (const mp of mps) {
      if (mp > 0) { await p.locator('.mstep button[data-step="1"]').click(); await p.waitForTimeout(500); }
      const s = await p.evaluate(() => ({
        where: document.querySelector(".mwhere")?.textContent, name: document.querySelector(".mname")?.textContent,
        steps: [...document.querySelectorAll(".mstep button")].map((b) => `${b.textContent}${b.disabled ? " (off)" : ""}`),
        marks: [...document.querySelectorAll("#mmarks .mk")].map((b) => b.getAttribute("aria-label")
          + (b.querySelector("span") ? ` [${b.querySelector("span").textContent}]` : "")),
        whole: document.querySelector(".mwhole")?.textContent }));
      log.push({ water: `proto-${slug}`, what: `mp=${mp}`, ...s });
      await p.screenshot({ path: `${OUT}/proto-${slug}-mp${mp}.png` });
    }
  } finally { await ctx.close(); }
}

const up = (p) => p.getByRole("button", { name: "▲ Upstream" }).first().click();
const down = (p) => p.getByRole("button", { name: "▼ Downstream" }).first().click();

// ---- the prototype ---------------------------------------------------------------------------------------------------
await proto("vedder", "vedder", [0, 1, 2, 3, 4]);
await proto("cowichan", "cowichan", [0, 1, 2, 3]);
await proto("klwa", "klwa", [0, 1, 2]);

// ---- the app ---------------------------------------------------------------------------------------------------------
await app("Chilliwack River", "chilliwack", async ({ p, shot, note }) => {
  // to the mouth part, then up through every part to the top
  for (let k = 0; k < 4; k += 1) {
    if (await p.getByRole("button", { name: "▼ Downstream" }).count()) await down(p); else break;
  }
  await note("at the mouth part");
  await shot("2-mouth");
  for (let k = 1; k <= 3; k += 1) { await up(p); await note(`▲ ${k}`); await shot(`3-up${k}`); }
  // a boundary mark, tapped
  const mk = p.locator("button[data-mark]").first();
  if (await mk.count()) { await mk.click(); await note("a mark tapped"); await shot("4-mark"); await mk.click(); }
  await p.getByRole("button", { name: "Whole river" }).first().click();
  await note("⤢ whole river");
  await shot("5-whole");
  await p.getByRole("button", { name: "This part" }).first().click();
  // back to the mouth and out by the exit
  for (let k = 0; k < 3; k += 1) await down(p);
  await note("back at the mouth");
  await p.getByRole("link", { name: /^Flows into / }).first().click();
  await p.waitForTimeout(3000);
  log.push({ water: "chilliwack", what: "after Flows into", title: await p.evaluate(() =>
    [...document.querySelectorAll("div[dir=auto]")].map((d) => d.textContent).find((t) => /River/.test(t ?? "")) ?? null) });
  await shot("6-flowed-into");
});

await app("Vedder River", "vedder", async ({ note }) => { await note("one part"); });

await app("Cowichan River", "cowichan", async ({ p, shot, note }) => {
  for (let k = 0; k < 4; k += 1) {
    if (await p.getByRole("button", { name: "▼ Downstream" }).count()) await down(p); else break;
  }
  await note("at the mouth part");
  await shot("2-mouth");
  for (let k = 1; k <= 3; k += 1) { await up(p); await note(`▲ ${k}`); await shot(`3-up${k}`); }
  await p.getByRole("button", { name: "Show regulations for this part" }).first().click();
  await p.waitForTimeout(4000);
  log.push({ water: "cowichan", what: "Show regulations for this part", tabs: await p.evaluate(() =>
    [...document.querySelectorAll('[role=tablist][aria-label="Views of this water"] [role=tab]')]
      .map((t) => t.getAttribute("aria-label") + (t.getAttribute("aria-selected") === "true" ? "*" : "")).join(" · ")) });
  await shot("4-regs");
});

await app("Kootenay Lake — Main Body", "klwa", async ({ p, shot, note }) => {
  for (let k = 0; k < 2; k += 1) {
    if (await p.getByRole("button", { name: "▼ Downstream" }).count()) { await down(p); await note(`▼ ${k + 1}`); await shot(`2-down${k + 1}`); }
  }
  await p.getByRole("button", { name: "Whole lake" }).first().click();
  await note("⤢ whole lake");
  await shot("3-whole");
});

await browser.close();
console.log(JSON.stringify({ log, errors }, null, 1));
