// T157 shots, on this worktree's Expo web build against the owner's data server (:39217, GET only: the live feed of
// 2026-10-23T20:41Z, already republished with T143/T144) — own headless Chromium, own port.
//
//   VENV_REPO=<main checkout> node conditions-shots.mjs <out dir> <app url> [step…]
// steps: map-change (the Change map, legend + dots), harrison-change (the Harrison sheet under Change: the section's
// low-water caveat), englishman-change (the joined day line with the COFFEE handover), chilliwack-level3 (the switcher
// under Level +3).
import { createRequire } from "node:module";
import { existsSync, mkdirSync, readdirSync } from "node:fs";
import { join } from "node:path";

function playwright() {
  const lib = join(process.env.VENV_REPO, ".venv", "lib");
  for (const py of readdirSync(lib).filter((d) => d.startsWith("python"))) {
    const driver = join(lib, py, "site-packages", "playwright", "driver", "package");
    if (existsSync(join(driver, "package.json"))) return createRequire(join(driver, "x.js"))(driver);
  }
  throw new Error("no Playwright in .venv");
}
const { chromium } = playwright();
const [OUT, URL, ...STEPS] = process.argv.slice(2);
mkdirSync(OUT, { recursive: true });
const browser = await chromium.launch({ headless: true });
const SIZES = { phone: { width: 390, height: 844 }, desktop: { width: 1280, height: 900 } };

async function open(size, scheme) {
  const ctx = await browser.newContext({ viewport: SIZES[size], deviceScaleFactor: 2, colorScheme: scheme });
  const p = await ctx.newPage();
  await p.goto(URL, { waitUntil: "load", timeout: 300000 });
  await p.waitForFunction(() => !!globalThis.__map, null, { timeout: 300000 });
  await p.waitForTimeout(4000);
  await p.getByText(/^conditions$/i).first().click();
  await p.waitForTimeout(4000);
  return { ctx, p };
}
const idle = (p) => p.evaluate(() => new Promise((done) => {
  const m = globalThis.__map; const t = setTimeout(done, 30000);
  if (m.loaded()) { clearTimeout(t); done(); } else m.once("idle", () => { clearTimeout(t); done(); });
}));
async function jump(p, c, z) {
  await p.evaluate(([c, z]) => globalThis.__map.jumpTo({ center: c, zoom: z, bearing: 0, pitch: 0 }), [c, z]);
  await p.waitForTimeout(5000); await idle(p); await p.waitForTimeout(1500);
}
async function horizon(p, label) {
  await p.getByRole("button", { name: label }).first().click().catch(async () => {
    await p.getByText(label, { exact: true }).first().click();
  });
  await p.waitForTimeout(4000); await idle(p);
}
/** Tap a named stream where the map draws it. */
async function tapStream(p, name) {
  const at = await p.evaluate((name) => {
    const m = globalThis.__map;
    const fs = m.queryRenderedFeatures().filter((f) => f.properties?.name === name && f.geometry.type.includes("Line"));
    const r = m.getCanvas().getBoundingClientRect();
    // the first vertex drawn well inside the canvas (a tile's feature runs past the screen)
    for (const f of fs) {
      const lines = f.geometry.type === "LineString" ? [f.geometry.coordinates] : f.geometry.coordinates;
      for (const line of lines)
        for (const c of line) {
          const pt = m.project(c);
          if (pt.x > r.width * 0.15 && pt.x < r.width * 0.85 && pt.y > r.height * 0.2 && pt.y < r.height * 0.8)
            return { x: r.left + pt.x, y: r.top + pt.y, n: fs.length };
        }
    }
    return null;
  }, name);
  if (!at) throw new Error(`no ${name} on screen`);
  await p.mouse.click(at.x, at.y);
  await p.waitForTimeout(6000);
  return at;
}

for (const step of STEPS) {
  if (step === "map-change") {
    for (const scheme of ["dark", "light"]) {
      const { ctx, p } = await open("phone", scheme);
      await jump(p, [-121.85, 49.3], 9.2);
      await horizon(p, "Change");
      await p.waitForTimeout(3000);
      await p.screenshot({ path: join(OUT, `map-change-lower-fraser--phone-${scheme}.png`) });
      await ctx.close();
    }
  }
  if (step === "harrison-change" || step === "englishman-change" || step === "chilliwack-level3") {
    const { ctx, p } = await open("desktop", "dark");
    const [c, z, name] = step === "harrison-change" ? [[-121.86, 49.27], 12, "Harrison River"]
      : step === "englishman-change" ? [[-124.33, 49.30], 11.5, "Englishman River"]
      : [[-121.62, 49.075], 11, "Chilliwack River"];
    await jump(p, c, z);
    if (step !== "chilliwack-level3") await horizon(p, "Change");
    const at = await tapStream(p, name);
    console.log(step, "tapped", JSON.stringify(at));
    if (step === "chilliwack-level3") {
      // the chart's own Flow / Level control sets the quantity (T130), then +3
      await p.getByText("Level", { exact: true }).first().click().catch(() => console.log("no Level control"));
      await p.waitForTimeout(4000);
      await p.getByText("+3d", { exact: true }).first().click().catch(() => console.log("no +3 chip"));
      await p.waitForTimeout(6000);
    }
    await p.screenshot({ path: join(OUT, `${step}--desktop-dark.png`) });
    // and the sheet scrolled to the switcher / Change block
    const target = step === "chilliwack-level3" ? p.getByText(/flow forecast only/).first()
      : step === "englishman-change" ? p.getByTestId("change-block") : p.getByTestId("section-low-water");
    await target.scrollIntoViewIfNeeded({ timeout: 15000 }).catch(() => console.log("target not found"));
    await p.waitForTimeout(1500);
    await p.screenshot({ path: join(OUT, `${step}--desktop-dark--scrolled.png`) });
    await ctx.close();
  }
}
await browser.close();
