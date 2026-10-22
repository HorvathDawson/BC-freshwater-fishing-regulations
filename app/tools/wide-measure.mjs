/**
 * T45 / T46 — lays out pages in headless Chrome and runs a measuring expression on each (see `regs-wide-layout.test.ts`
 * and `fish-looks-layout.test.ts`, which write the pages). Its own process, like `water-frame-measure.mjs`: the tests
 * run under jsdom, whose WebSocket is not the DevTools protocol's.
 *
 *   node tools/wide-measure.mjs '<json: {"jobs": [{"page": "<file.html>", "size": [w, h], "measure"?: "<file.js>", "shot"?: "<file.png>"}, …]}>'
 *
 * The measure file holds one expression (an async function's body is fine: `(async () => …)()`), evaluated after the
 * page's web fonts have loaded. Prints `[<the expression's value, JSON-parsed>, …]` in the jobs' order.
 */
import { pathToFileURL } from "node:url";
import { existsSync, readFileSync, writeFileSync } from "node:fs";

const MAC_CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
process.env.CHROME ??= existsSync(MAC_CHROME) ? MAC_CHROME : "google-chrome";
const { open } = await import("../design/regs-v11/tools/cdp.mjs");

const FONTS = `Promise.all([...document.fonts].map((f) => f.load().catch(() => null))).then(() => document.fonts.ready)
  .then(() => [...document.fonts].filter((f) => f.status === "loaded").map((f) => f.family).sort().join(","))`;

const { jobs } = JSON.parse(process.argv[2]);
const out = [];
const b = await open("about:blank", { width: 390, height: 844 });
try {
  // Scrollbars take no room, whatever the host does: macOS draws a classic 15 px bar (laid out INSIDE the page) when a
  // mouse is attached and an overlay one when not, so the same page measured 15 px narrower on one machine.
  await b.send("Emulation.setScrollbarsHidden", { hidden: true });
  for (const { page, size: [w, h], measure, shot } of jobs) {
    await b.send("Emulation.setDeviceMetricsOverride", { width: w, height: h, deviceScaleFactor: 1, mobile: false });
    const url = pathToFileURL(page).href;
    await b.send("Page.navigate", { url });
    for (let t = 0; t < 200; t++) {
      const loaded = `location.href === ${JSON.stringify(url)} && document.readyState === "complete"`;
      if (await b.evaluate(loaded).catch(() => false)) break;
      await new Promise((r) => setTimeout(r, 50));
    }
    const fonts = await b.evaluate(FONTS);
    const value = measure ? JSON.parse(await b.evaluate(readFileSync(measure, "utf8"))) : {};
    // a job with `shot`: the laid-out page saved as a PNG there (a picture for the owner, not a measurement)
    if (shot) {
      await new Promise((r) => setTimeout(r, 400));
      const { data } = await b.send("Page.captureScreenshot", { format: "png" });
      writeFileSync(shot, Buffer.from(data, "base64"));
    }
    out.push({ fonts, ...value });
  }
} finally { await b.close(); }
process.stdout.write(JSON.stringify(out));
