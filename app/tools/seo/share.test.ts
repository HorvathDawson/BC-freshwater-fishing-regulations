import { runInNewContext } from "node:vm";
import { setFlagsFromString } from "node:v8";
import { describe, expect, it } from "vitest";
import { SHARE_FONT, SHARE_GROUND, SHARE_H, SHARE_INK, SHARE_W } from "./html";
import { drawSharePngs, pngSize } from "./share";

setFlagsFromString("--expose-gc");
const gc = runInNewContext("gc") as () => void;
const card = (i: number) => `<svg xmlns="http://www.w3.org/2000/svg" width="${SHARE_W}" height="${SHARE_H}" viewBox="0 0 ${SHARE_W} ${SHARE_H}" ` +
  `font-family="${SHARE_FONT}"><rect width="${SHARE_W}" height="${SHARE_H}" fill="${SHARE_GROUND}"/>` +
  `<text x="64" y="190" font-size="72" font-weight="700" fill="${SHARE_INK}">Water ${i} Creek</text></svg>`;

describe("the share cards drawn for a whole release (2026-11-11: 19,000 held 59 GB)", () => {
  it("frees each card's native pixmap as it goes: 400 cards keep far less than the 1.2 GB they would hold at once", async () => {
    const N = 400, mb = () => process.memoryUsage().rss / 1048576;
    gc(); await new Promise((r) => setImmediate(r)); gc();
    const before = mb();
    let n = 0, last: Uint8Array | null = null;
    // the collector asked every 50 cards, so the test does not wait on V8's own timing; what it proves is that a collected
    // card's ~3 MB is let go between cards (a synchronous loop never runs the finalizer, collected or not)
    await drawSharePngs(Array.from({ length: N }, (_, i) => [`share/${i}.png`, card(i)] as const), (_, png) => { last = png; if (++n % 50 === 0) gc(); });
    gc(); await new Promise((r) => setImmediate(r));
    expect(n).toBe(N);
    expect(pngSize(last!)).toEqual({ width: SHARE_W, height: SHARE_H });
    expect(mb() - before).toBeLessThan(400);
  });
});
