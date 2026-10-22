// SHEET BACKDROP MOTION — a frame recording of the Layers sheet opening and closing on Expo web (headless Chrome).
//   node handoff/PLAN/app-data/sheet-motion/record.mjs <app url> <label>
//     e.g. node handoff/PLAN/app-data/sheet-motion/record.mjs http://localhost:19431 before
// Writes, next to this file: <label>-trace.json (every animation frame: the scrim's top edge and its EFFECTIVE opacity —
// its own times every ancestor's — and the sheet's top edge) and <label>-NN.png (a frame strip through open and close).
// What the owner saw ("the screen opacity slides down with the menu"): the scrim's top edge moving. Fixed, it never moves.
import { writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { open } from "../../../../app/design/regs-v11/tools/cdp.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const [url, label, scheme = "light"] = process.argv.slice(2);
const b = await open(url, { width: 390, height: 844 });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
try {
  await b.send("Emulation.setScrollbarsHidden", { hidden: true });
  await b.send("Emulation.setEmulatedMedia", { features: [{ name: "prefers-color-scheme", value: scheme }] });
  for (let t = 0; t < 120; t++) {
    if (await b.evaluate(`!!document.querySelector('[aria-label^="Choose layers"]')`)) break;
    await sleep(500);
  }
  await sleep(3000); // the map settles
  // the sampler: one row per animation frame, from the press until stopped
  await b.evaluate(`(() => {
    const eff = (el) => { let o = 1; for (let e = el; e && e !== document.body; e = e.parentElement) o *= Number(getComputedStyle(e).opacity); return o; };
    window.__rows = []; window.__t0 = performance.now(); window.__on = true;
    const tick = () => {
      if (!window.__on) return;
      const scrim = [...document.querySelectorAll('[aria-label="Close"]')].at(-1);
      const sheet = document.querySelector('[data-testid="sheet-panel"]') ?? [...document.querySelectorAll('[aria-modal="true"]')].at(-1);
      window.__rows.push({ t: Math.round(performance.now() - window.__t0),
        scrimTop: scrim ? Math.round(scrim.getBoundingClientRect().top) : null,
        scrimOpacity: scrim ? Math.round(eff(scrim) * 100) / 100 : null,
        sheetTop: sheet ? Math.round(sheet.getBoundingClientRect().top) : null });
      requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  })()`);
  const shots = [];
  const snap = async () => { const { data } = await b.send("Page.captureScreenshot", { format: "png" }); shots.push(data); };
  await b.evaluate(`document.querySelector('[aria-label^="Choose layers"]').click()`);
  for (let i = 0; i < 4; i++) { await snap(); await sleep(60); }
  await sleep(600); await snap();
  await b.evaluate(`window.__close0 = Math.round(performance.now() - window.__t0); [...document.querySelectorAll('[aria-label="Done"]')].at(-1).click()`);
  for (let i = 0; i < 6; i++) { await snap(); await sleep(50); }
  await sleep(700); await snap();
  const trace = await b.evaluate(`(window.__on = false, { closeAt: window.__close0, rows: window.__rows })`);
  writeFileSync(join(HERE, `${label}-trace.json`), JSON.stringify(trace, null, 1));
  shots.forEach((d, i) => writeFileSync(join(HERE, `${label}-${String(i).padStart(2, "0")}.png`), Buffer.from(d, "base64")));
  const moved = new Set(trace.rows.filter((r) => r.scrimTop !== null).map((r) => r.scrimTop));
  console.log(JSON.stringify({ label, frames: trace.rows.length, shots: shots.length, scrimTops: [...moved].slice(0, 12) }));
} finally { await b.close(); }
