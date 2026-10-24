/**
 * T38 — the hatch sheet's two drawings (Wild · adipose fin there / Hatchery · clipped), shot from the prototype's own
 * `FISHART.draw('RB', …)` (app/fixtures/fishart.js, which regs-v11.html loads). Only the pure part runs here:
 * rasterising needs Chrome (`node tools/emit-adipose.mjs`, `--check`), so it is not in `pnpm check`.
 *
 * What this pins:
 *   - the drawing is deterministic (the seeded `rng`; the gradient id's `Math.random` is stubbed by the emitter);
 *   - wild and clipped differ ONLY by the adipose fin ↔ the healed scar and the label's ", adipose fin clipped";
 *   - the SVGs the committed WebPs were shot from (sha256): a fishart.js change fails here → rerun the emitter;
 *   - the committed WebPs are 800×300 with an alpha channel (no ground behind the fish: D4, r20h-picture-surface);
 *   - the manifest rows and the app's `ADIPOSE_IMAGES` registration (code null → never a species picture).
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { adiposeSvgs, FISHART_SOURCE, OUT, SIZE } from "./emit-adipose.mjs";
import { emit, MANIFEST, MOBILE_OUT } from "./emit-fish.mjs";
import { FISH } from "../packages/core/src/regs/fish.generated";

const src = readFileSync(FISHART_SOURCE, "utf8");
const sha = (s: string | Buffer) => createHash("sha256").update(s).digest("hex");

describe("emit-adipose: the drawings", () => {
  it("wild and clipped differ only by the fin ↔ the healed scar and the label suffix", () => {
    const { wild, clipped } = adiposeSvgs(src);
    const tags = (s: string) => s.match(/<[^>]+>/g) ?? [];
    const only = (a: string, b: string) => { const rest = [...tags(b)]; return tags(a).filter((t) => { const i = rest.indexOf(t); if (i < 0) return true; rest.splice(i, 1); return false; }); };
    const [wSvg, fin, ...wMore] = only(wild, clipped), [cSvg, scar, ...cMore] = only(clipped, wild);
    expect([wSvg, cSvg, wMore, cMore]).toEqual([
      '<svg viewBox="0 0 400 150" class="fishsvg" role="img" aria-label="Rainbow trout">',
      '<svg viewBox="0 0 400 150" class="fishsvg" role="img" aria-label="Rainbow trout, adipose fin clipped">', [], []]);
    // the fin is a closed shape; the scar an open stroke (page v36: `clipped` swaps one for the other, nothing else)
    expect([/^<path d="[^"]*Z" fill=/.test(fin!), /^<path d="[^"]*" stroke="[^"]*" stroke-width="2\.2" fill="none" stroke-linecap="round"\/>$/.test(scar!)])
      .toEqual([true, true]);
    expect(wild.replace(fin!, scar!).replace(wSvg!, cSvg!)).toBe(clipped);
  });

  it("both carry the ring around the adipose fin (adiMark) and no numbered marks", () => {
    const { wild, clipped } = adiposeSvgs(src);
    const ring = (s: string) => (s.match(/<circle [^>]*r="13" fill="none" stroke="[^"]*" stroke-width="2\.2"\/>/g) ?? []).length;
    const marks = (s: string) => (s.match(/<text /g) ?? []).length;
    expect([ring(wild), ring(clipped), marks(wild), marks(clipped)]).toEqual([1, 1, 0, 0]);
  });

  it("the SVGs the committed WebPs were shot from (a fishart.js change lands here: rerun `node tools/emit-adipose.mjs`)", () => {
    const { wild, clipped } = adiposeSvgs(src);
    expect({ wild: sha(wild), clipped: sha(clipped) }).toEqual({
      wild: "cf51514b1fc52bee7a0de6da2f00f7657c24aacb087fd1e3020b1d035c95e5f9",
      clipped: "dc8aee55e3e764912970f37609236f3aa9f32f9d766f04737567bd39ee657ece",
    });
  });
});

/** a WebP's canvas size and whether it has alpha (RIFF → VP8X extended header). */
function webpInfo(b: Buffer) {
  const riff = b.toString("ascii", 0, 4), webp = b.toString("ascii", 8, 12), chunk = b.toString("ascii", 12, 16);
  if (chunk !== "VP8X") return { riff, webp, chunk, alpha: false, width: 0, height: 0 };
  const flags = b[20]!, w = b.readUIntLE(24, 3) + 1, h = b.readUIntLE(27, 3) + 1;
  return { riff, webp, chunk, alpha: (flags & 0x10) !== 0, width: w, height: h };
}

describe("emit-adipose: the committed pictures", () => {
  it("adipose-wild.webp and adipose-clipped.webp are 800×300 WebP with an alpha channel (a cut-out, no ground)", () => {
    const info = Object.fromEntries((["wild", "clipped"] as const).map((v) => [v, webpInfo(readFileSync(OUT[v]))]));
    const want = { riff: "RIFF", webp: "WEBP", chunk: "VP8X", alpha: true, width: SIZE.width, height: SIZE.height };
    expect(info).toEqual({ wild: want, clipped: want });
    expect(SIZE).toEqual({ width: 800, height: 300 });
  });

  it("the manifest lists both, code null, drawn for this app", () => {
    const m = JSON.parse(readFileSync(MANIFEST, "utf8")) as { images: { file: string }[] };
    expect(m.images.filter((i) => i.file.startsWith("adipose-"))).toEqual([
      { file: "adipose-wild.webp", code: null, caption: "Wild · adipose fin there", phase: null, stand_in_for: null,
        credit: "drawn for this app (FISHART, page v36)", type: "Drawing", source: "this app", width: 800, height: 300 },
      { file: "adipose-clipped.webp", code: null, caption: "Hatchery · clipped, a healed scar instead", phase: null, stand_in_for: null,
        credit: "drawn for this app (FISHART, page v36)", type: "Drawing", source: "this app", width: 800, height: 300 },
    ]);
  });

  it("code null keeps them out of FISH (no species row shows them)", () => {
    const files = Object.values(FISH).flatMap((imgs) => imgs.map((i) => i.file));
    expect(files.filter((f) => f.startsWith("adipose-"))).toEqual([]);
  });

  it("the app registers them by variant, literal require() paths (fishImages.generated.ts)", () => {
    const mobile = readFileSync(MOBILE_OUT, "utf8");
    const at = mobile.indexOf("export const ADIPOSE_IMAGES");
    expect(mobile.slice(at, mobile.indexOf("};", at) + 2).split("\n")).toEqual([
      "export const ADIPOSE_IMAGES: { readonly wild: ImageSourcePropType; readonly clipped: ImageSourcePropType } = {",
      '  wild: require("../assets/fish/adipose-wild.webp"),',
      '  clipped: require("../assets/fish/adipose-clipped.webp"),',
      "};",
    ]);
  });

  it("emit-fish refuses a manifest without both drawings (the app would draw an empty sheet)", () => {
    const m = JSON.parse(readFileSync(MANIFEST, "utf8")) as { images: { file: string }[] };
    const without = { ...m, images: m.images.filter((i) => i.file !== "adipose-clipped.webp") };
    expect(() => emit(without, "x", {})).toThrow("emit-fish: manifest.json has no adipose-clipped.webp (the hatch sheet's drawing)");
  });
});
