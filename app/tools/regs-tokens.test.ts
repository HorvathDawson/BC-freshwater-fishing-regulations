/**
 * T22: the app's theme and type scale carry what the Regulations page paints with, read from the prototype's computed
 * tokens (app/fixtures/golden/tokens.json), never retyped. A prototype bump that moves a token fails here.
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { DARK, LIGHT, SPACE, GUTTER, type Palette } from "../packages/ui-native/src/theme";
import { FACE, FACES_REQUIRED, TYPE } from "../packages/ui-native/src/type";

const here = (p: string) => fileURLToPath(new URL(p, import.meta.url));
const tokens = JSON.parse(readFileSync(here("../fixtures/golden/tokens.json"), "utf8")) as {
  tokens: { light: Record<string, string>; dark: Record<string, string> };
  compare: { token: string; light: string; dark: string; same: boolean }[];
};
const px = (rem: string) => parseFloat(rem) * 16;

/** tokens.md §1.1: the prototype token -> the Palette field it lands in. */
const FIELD: Record<string, keyof Palette> = {
  "--page": "page", "--card": "card", "--tint": "tint", "--line": "line", "--line2": "line2", "--ink": "ink",
  "--accent": "accent", "--open": "open", "--restricted": "restricted", "--closed": "closed",
};
const lower = (v: unknown) => String(v).toLowerCase();

describe("T22 tokens", () => {
  it("the theme's sub is the prototype's (D3 a)", () => {
    expect(LIGHT.sub).toBe(tokens.tokens.light["--sub"]);
    expect(DARK.sub).toBe(tokens.tokens.dark["--sub"]);
  });

  it("(T21) the size section's rule is the prototype's (--rule, round UR7)", () => {
    expect([LIGHT.rule, DARK.rule]).toEqual([tokens.tokens.light["--rule"], tokens.tokens.dark["--rule"]]);
  });

  it("every token the prototype and the app share still agrees", () => {
    const rows = tokens.compare.filter((r) => r.same && FIELD[r.token]);
    expect(rows.length).toBeGreaterThanOrEqual(8);
    for (const r of rows) {
      expect([r.token, lower(LIGHT[FIELD[r.token]!])]).toEqual([r.token, lower(r.light)]);
      expect([r.token, lower(DARK[FIELD[r.token]!])]).toEqual([r.token, lower(r.dark)]);
    }
  });

  it("the type scale is the prototype's (--t1..--t4)", () => {
    const L = tokens.tokens.light;
    expect([TYPE.answer.fontSize, TYPE.lead.fontSize, TYPE.regsBody.fontSize, TYPE.meta.fontSize])
      .toEqual([px(L["--t1"]!), px(L["--t2"]!), px(L["--t3"]!), px(L["--t4"]!)]);
    expect([TYPE.regsBody.lineHeight, TYPE.meta.lineHeight]).toEqual([21.3, 17.72]);
    const len = (v: string) => (v.endsWith("rem") ? px(v) : parseFloat(v));
    expect([SPACE.s1, SPACE.s2, SPACE.s3, SPACE.s4])
      .toEqual([len(L["--s1"]!), len(L["--s2"]!), len(L["--s3"]!), len(L["--s4"]!)]);
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
