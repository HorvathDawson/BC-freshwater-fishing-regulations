// T47 map shots: the SAME four views in light, dark, the colour-blind palette and satellite, for one palette.
//
//   node handoff/PLAN/app-data/status-colours/scripts/shots.mjs <palette id> <out dir> [app url]
//
// The app must be the Expo web dev server of THIS worktree (its theme files carry the palette under test, via
// apply-palette.mjs + `pnpm style:build`, and Metro restarted), with the data server on its own port
// (`TILE_PORT=<port> node app/tools/serve-tiles.mjs`, EXPO_PUBLIC_DATA_BASE pointing at it). The camera is set through
// the development-only `globalThis.__map` (Map.web.tsx), so every palette is shot at the identical frame; the day is
// the app's default (today), so every palette sees the same statuses.
//
// Playwright is the repo's own: the Python package's bundled driver in .venv (as map-chrome/rail/web-check.mjs finds it).
import { createRequire } from "node:module";
import { existsSync, mkdirSync, readdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "../../../../..");
function playwright() {
  const lib = join(REPO, ".venv", "lib");
  for (const py of existsSync(lib) ? readdirSync(lib).filter((d) => d.startsWith("python")) : []) {
    const driver = join(lib, py, "site-packages", "playwright", "driver", "package");
    if (existsSync(join(driver, "package.json"))) return createRequire(join(driver, "x.js"))(driver);
  }
  throw new Error("no Playwright in .venv (pip install playwright)");
}
const { chromium } = playwright();

const [ID, OUT, URL = "http://localhost:39482/"] = process.argv.slice(2);
if (!ID || !OUT) { console.error("usage: shots.mjs <palette id> <out dir> [app url]"); process.exit(2); }
mkdirSync(OUT, { recursive: true });

/** The views (ticket T47 Work): real waters, real zooms. */
export const VIEWS = [
  { id: "vedder-z11", label: "Vedder / Chilliwack, z11", center: [-121.93, 49.09], zoom: 11 },
  { id: "kootenay-z9", label: "Kootenay Lake, z9", center: [-116.87, 49.72], zoom: 9 },
  { id: "province-z6", label: "Province, z6", center: [-122.6, 50.9], zoom: 6 },
  { id: "boundary-z10", label: "Region 2/3/8 + MU boundaries beside closed and own water (Coquihalla summit), z10", center: [-121.05, 49.62], zoom: 10 },
];
const GROUNDS = [
  { id: "light", scheme: "light" },
  { id: "dark", scheme: "dark" },
  { id: "cvd", scheme: "light", palette: "cvd" },
  { id: "satellite", scheme: "light", satellite: true },
];

const browser = await chromium.launch({ headless: true });
const log = { palette: ID, url: URL, at: new Date().toISOString(), shots: [], errors: [] };

async function boot(ground) {
  const ctx = await browser.newContext({ viewport: { width: 390, height: 844 }, deviceScaleFactor: 2,
                                         colorScheme: ground.scheme });
  const p = await ctx.newPage();
  p.on("pageerror", (e) => log.errors.push(`${ground.id}: ${String(e).slice(0, 200)}`));
  await p.goto(URL, { waitUntil: "load", timeout: 300000 });
  await p.waitForFunction(() => !!globalThis.__map, null, { timeout: 300000 });
  await p.waitForTimeout(6000);
  if (ground.palette) {
    await p.getByLabel("Open menu", { exact: true }).click();
    await p.waitForTimeout(800);
    await p.getByRole("button", { name: "Display", exact: true }).click();
    await p.waitForTimeout(800);
    await p.getByRole("radio", { name: "Colour-blind", exact: true }).click();
    await p.waitForTimeout(800);
    // Back from the Display page returns to the map (the menu closes with it)
    await p.getByRole("button", { name: "Back", exact: true }).click();
    await p.waitForTimeout(1500);
  }
  if (ground.satellite) {
    await p.getByLabel("Choose layers", { exact: true }).click();
    await p.waitForTimeout(800);
    await p.getByRole("radio", { name: "Satellite", exact: true }).click();
    await p.waitForTimeout(800);
    await p.getByRole("button", { name: "Done", exact: true }).click();
    await p.waitForTimeout(1500);
  }
  return { ctx, p };
}

async function frame(p, v) {
  await p.evaluate(([c, z]) => new Promise((done) => {
    const m = globalThis.__map;
    const t = setTimeout(done, 45000);
    m.once("idle", () => { clearTimeout(t); done(); });
    m.jumpTo({ center: c, zoom: z, bearing: 0, pitch: 0 });
  }), [v.center, v.zoom]);
  // the status colouring is pushed per tile as it arrives (feature-state), after the first idle
  await p.waitForTimeout(4000);
  await p.evaluate(() => new Promise((done) => {
    const m = globalThis.__map; const t = setTimeout(done, 20000);
    if (m.loaded()) { clearTimeout(t); done(); } else m.once("idle", () => { clearTimeout(t); done(); });
  }));
  await p.waitForTimeout(2500);
}

for (const g of GROUNDS.filter((x) => !process.env.ONLY || process.env.ONLY.split(",").includes(x.id))) {
  let s;
  try {
    s = await boot(g);
    for (const v of VIEWS) {
      await frame(s.p, v);
      const file = `${ID}--${v.id}--${g.id}.jpg`;
      await s.p.screenshot({ path: join(OUT, file), type: "jpeg", quality: 82 });
      log.shots.push({ file, view: v.id, ground: g.id });
    }
  } catch (e) {
    log.errors.push(`${g.id} FAILED: ${String(e).slice(0, 300)}`);
    await s?.p.screenshot({ path: join(OUT, `${ID}--failure--${g.id}.jpg`), type: "jpeg" }).catch(() => {});
  } finally {
    await s?.ctx.close();
  }
}
await browser.close();
writeFileSync(join(OUT, `${ID}--log.json`), JSON.stringify(log, null, 1));
console.log(JSON.stringify({ shots: log.shots.length, errors: log.errors }));
