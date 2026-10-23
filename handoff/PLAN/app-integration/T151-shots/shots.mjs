// T150 / T151 shots: a many-part water's Regulations header and its Map view (the parts list under the map, the
// selected part marked by dimming the others), light and dark, phone (390) and desktop (1280).
//
//   VENV_REPO=<main checkout> node shots.mjs <out dir> <app url> <prefix>
//   SIZES=phone|desktop|phone,desktop  WATERS=fraser,kootenay,chilliwack  OPEN_PARTS=1 (also shoot the list open)
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
const [OUT, URL, PREFIX = "shot"] = process.argv.slice(2);
mkdirSync(OUT, { recursive: true });
const ALL = [
  { id: "fraser", q: "regs=gnis:39325&part=3" },
  { id: "kootenay", q: "regs=wbk:-22" },
  { id: "chilliwack", q: "regs=gnis:8634&part=1" },
];
const want = (process.env.WATERS ?? "fraser,kootenay,chilliwack").split(",");
const PAGES = ALL.filter((p) => want.includes(p.id));
const SIZES = (process.env.SIZES ?? "phone,desktop").split(",").map((s) =>
  s === "phone" ? { id: s, viewport: { width: 390, height: 844 } } : { id: s, viewport: { width: 1280, height: 860 } });
const SCHEMES = (process.env.SCHEMES ?? "light,dark").split(",");
const browser = await chromium.launch({ headless: true });
for (const scheme of SCHEMES)
  for (const size of SIZES)
    for (const pg of PAGES) {
      const ctx = await browser.newContext({ viewport: size.viewport, deviceScaleFactor: 2, colorScheme: scheme });
      const p = await ctx.newPage();
      const errors = [];
      p.on("pageerror", (e) => errors.push(String(e).slice(0, 200)));
      await p.goto(`${URL}?${pg.q}&theme=${scheme}`, { waitUntil: "load", timeout: 300000 });
      await p.getByTestId("regs-header").first().waitFor({ timeout: 180000 });
      await p.waitForTimeout(1500);
      const base = `${PREFIX}--${pg.id}--${size.id}--${scheme}`;
      const parts = await p.getByTestId("regs-action").count();
      await p.screenshot({ path: join(OUT, `${base}--regs.jpg`), type: "jpeg", quality: 82 });
      await p.getByRole("tab", { name: "Map", exact: true }).first().click();
      await p.getByTestId("water-map-name").first().waitFor({ timeout: 60000 }).catch(() => errors.push("no map bar"));
      await p.waitForTimeout(8000);
      await p.screenshot({ path: join(OUT, `${base}--map.jpg`), type: "jpeg", quality: 82 });
      const list = p.getByTestId("water-map-parts-toggle");
      const hasList = await list.count();
      if (process.env.OPEN_PARTS && hasList) {
        await list.first().click();
        await p.waitForTimeout(800);
        await p.screenshot({ path: join(OUT, `${base}--map-parts.jpg`), type: "jpeg", quality: 82, fullPage: true });
      }
      console.log(JSON.stringify({ base, regsPartsButton: parts, partsList: hasList, errors }));
      await ctx.close();
    }
await browser.close();
