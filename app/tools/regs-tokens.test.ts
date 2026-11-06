/**
 * T22: the app's type scale and spacing carry what the Regulations page paints with (the prototype's --t1..--t4 and --s1..--s4,
 * pinned here as the numbers they computed to), and no regs component sets a weight, a folded colour or a literal radius.
 * (T439: the colours were compared with app/fixtures/golden/tokens.json; the palette is now the map's tokens.json alone,
 * held by check-colours / one-colour-pathway, and that file is gone.)
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { SPACE, GUTTER } from "../packages/ui-native/src/theme";
import { FACE, FACES_REQUIRED, TYPE } from "../packages/ui-native/src/type";

const here = (p: string) => fileURLToPath(new URL(p, import.meta.url));
const px = (rem: number) => rem * 16;

describe("T22 tokens", () => {
  it("the type scale is the prototype's (--t1..--t4)", () => {
    // --t1 1.625rem, --t2 1.0625rem, --t3 .9375rem, --t4 .78rem
    expect([TYPE.answer.fontSize, TYPE.lead.fontSize, TYPE.regsBody.fontSize, TYPE.meta.fontSize])
      .toEqual([px(1.625), px(1.0625), px(0.9375), px(0.78)]);
    expect([TYPE.regsBody.lineHeight, TYPE.meta.lineHeight]).toEqual([21.3, 17.72]);
    expect([SPACE.s1, SPACE.s2, SPACE.s3, SPACE.s4]).toEqual([4, 8, 12, 16]);   // --s1..--s4
    expect(GUTTER).toBe(16);
  });

  it("no regs component sets a weight, a folded colour or a literal radius", () => {
    // here, not in packages/ui-native: a ui-native file may not import node:fs (the boundaries gate)
    const dir = here("../packages/ui-native/src/regs");
    const files: string[] = [];
    const walk = (d: string) => {
      for (const f of readdirSync(d)) {
        const p = join(d, f);
        if (statSync(p).isDirectory()) walk(p);
        else if (p.endsWith(".tsx")) files.push(p);
      }
    };
    if (existsSync(dir)) walk(dir);
    for (const f of files) {
      const src = readFileSync(f, "utf8");
      expect([f, /fontWeight\s*:/.test(src)]).toEqual([f, false]);
      expect([f, /palette\.(faint|wash|quiet)\b/.test(src)]).toEqual([f, false]);
      expect([f, /borderRadius\s*:\s*\d/.test(src)]).toEqual([f, false]);
    }
  });

  it("every TYPE face is loaded, Archivo 800 included", () => {
    expect(FACES_REQUIRED).toContain(FACE.textHeavy);
    const app = readFileSync(here("../apps/mobile/src/App.tsx"), "utf8");
    for (const face of FACES_REQUIRED) expect([face, app.includes(face)]).toEqual([face, true]);
  });
});
