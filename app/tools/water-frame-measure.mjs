/**
 * T44 — lays out pages in headless Chrome and measures the water frame's shared elements (see
 * `water-frame-layout.test.ts`, which writes the pages). Its own process: the test runs under jsdom, whose WebSocket
 * is not the DevTools protocol's.
 *
 *   node tools/water-frame-measure.mjs '<json: {"sizes": [[w, h], …], "pages": {"<view>": "<file.html>", …}}>'
 *
 * Prints `{ "<w>x<h>": { "<view>": { back: [x, y], strip: [x, y], title: [x, y], left: x } } }`.
 */
import { pathToFileURL } from "node:url";
import { existsSync } from "node:fs";

const MAC_CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
process.env.CHROME ??= existsSync(MAC_CHROME) ? MAC_CHROME : "google-chrome";
const { open } = await import("../design/regs-v11/tools/cdp.mjs");

/** Each box's left/top; the content's left edge is the leftmost line of text under the water's name row. */
const MEASURE = `(() => {
  const box = (e) => { if (!e) return null; const r = e.getBoundingClientRect(); return [r.left, r.top]; };
  const title = document.querySelector('[data-testid=water-title]');
  const below = title ? title.getBoundingClientRect().bottom : Infinity;
  let left = Infinity;
  const walk = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  for (let n; (n = walk.nextNode());) {
    if (!n.textContent.trim() || n.parentElement.closest('[aria-hidden=true]')) continue;
    const range = document.createRange(); range.selectNodeContents(n);
    for (const r of range.getClientRects())
      if (r.width > 0 && r.top >= below && r.top < innerHeight) left = Math.min(left, r.left);
  }
  return JSON.stringify({ back: box(document.querySelector('[aria-label="Back"]')),
                          strip: box(document.querySelector('[role=tablist][aria-label="Views of this water"]')),
                          title: box(title), left: left === Infinity ? null : left });
})()`;

const { sizes, pages } = JSON.parse(process.argv[2]);
const out = {};
const b = await open("about:blank", { width: 390, height: 844 });
try {
  for (const [view, file] of Object.entries(pages)) {
    const url = pathToFileURL(file).href;
    await b.send("Page.navigate", { url });
    for (let t = 0; t < 200; t++) {
      const loaded = `location.href === ${JSON.stringify(url)} && document.readyState === "complete"`;
      if (await b.evaluate(loaded).catch(() => false)) break;
      await new Promise((r) => setTimeout(r, 50));
    }
    for (const [w, h] of sizes) {
      await b.send("Emulation.setDeviceMetricsOverride", { width: w, height: h, deviceScaleFactor: 1, mobile: false });
      (out[`${w}x${h}`] ??= {})[view] = JSON.parse(await b.evaluate(MEASURE));
    }
  }
} finally { await b.close(); }
process.stdout.write(JSON.stringify(out));
