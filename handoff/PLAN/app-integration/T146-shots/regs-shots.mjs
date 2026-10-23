// T147 shots: a no-access water's Regulations page opened by the start link (no tapped section: the page's part speaks),
// light and dark, phone (390) and desktop (1280).
//
//   VENV_REPO=<main checkout> node regs-shots.mjs <out dir> <app url>
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
const [OUT, URL] = process.argv.slice(2);
mkdirSync(OUT, { recursive: true });
const PAGES = [
  { id: "coquitlam-lake", q: "regs=wbk:329291808" },
  { id: "seymour-river-part0", q: "regs=gnis:6105&part=0" },
];
const SIZES = (process.env.SIZES ?? "phone,desktop").split(",").map((s) =>
  s === "phone" ? { id: s, viewport: { width: 390, height: 844 } } : { id: s, viewport: { width: 1280, height: 860 } });
const browser = await chromium.launch({ headless: true });
for (const scheme of ["light", "dark"])
  for (const size of SIZES)
    for (const pg of PAGES) {
      const ctx = await browser.newContext({ viewport: size.viewport, deviceScaleFactor: 2, colorScheme: scheme });
      const p = await ctx.newPage();
      const errors = [];
      p.on("pageerror", (e) => errors.push(String(e).slice(0, 200)));
      await p.goto(`${URL}?${pg.q}&theme=${scheme}`, { waitUntil: "load", timeout: 300000 });
      const ok = await p.getByTestId("access-banner").first().waitFor({ timeout: 120000 }).then(() => true, () => false);
      await p.waitForTimeout(1500);
      const file = `T147--${pg.id}--${size.id}--${scheme}.jpg`;
      await p.screenshot({ path: join(OUT, file), type: "jpeg", quality: 85 });
      const text = ok ? await p.getByTestId("access-banner").first().textContent() : null;
      console.log(JSON.stringify({ file, banner: text, errors }));
      await ctx.close();
    }
await browser.close();
