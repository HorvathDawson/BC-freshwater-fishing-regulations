/**
 * (T759, owner 2026-11-15: "I think it should dash with closed color maybe? Or a better color what ever is best practice")
 * THE ISSUE STRIPE'S COLOUR, MEASURED. The closed colour was tried first and fails; the stripe is dark ink on a paper dash
 * (`color.issue.stripe` on `color.issue.casing`), which leaves it a contrast of its own on every line a Salmon stream can
 * wear, on every basemap ground, in all three themes and for every colour-blind reader. Numbers, not an eyeball: WCAG 1.4.11
 * (3:1, as map-contrast.test.ts) and ΔE2000 under protan / deutan / tritan in both models (cvd.ts, floor 15 as the gear
 * marks' ground floor in method-cvd.test.ts).
 *
 * Why not closed-red ink on the same paper: it would pass these numbers, but a red dash on a blue line says "closed" about a
 * rule nobody has placed; the stripe is no status colour (asserted below), the banner says what it is.
 */
import { describe, expect, it } from "vitest";
import { namedFlavor } from "@protomaps/basemaps";
import { SALMON_MAP_STATUSES } from "@app/core";
import { resolveTheme } from "@app/map";
import { salmonStatusColour } from "@app/map/colours";
import { contrast, DEFICIENCIES, deltaE, MODELS, simulate } from "./cvd";
import { CVD, DARK, LIGHT } from "../packages/ui-native/src/theme";

const THEMES = [{ th: "light", flavor: "light", palette: LIGHT }, { th: "dark", flavor: "black", palette: DARK },
                { th: "cvd", flavor: "light", palette: CVD }] as const;
const NON_TEXT = 3, FLOOR = 15, INK_ON_PAPER = 7;

/** the worst ΔE2000 between two colours over normal vision and every deficiency in both models */
function worstDE(a: string, b: string): number {
  let w = deltaE(a, b);
  for (const k of DEFICIENCIES) for (const m of MODELS) w = Math.min(w, deltaE(simulate(a, k, m), simulate(b, k, m)));
  return w;
}
const tok = (th: string) => resolveTheme(th) as Record<string, string>;
/** the colours a Salmon stream wears: the four answers and the grey of a section with none */
const lines = (th: string): [string, string][] =>
  [...SALMON_MAP_STATUSES.map((s): [string, string] => [s, salmonStatusColour(th, s)]), ["no answer", tok(th)["color.water.unmapped"]!]];
/** what the stripe can be drawn on besides a line: the basemap flavour's grounds, and every grey (satellite is any colour) */
function grounds(flavor: string): [string, string][] {
  const f = namedFlavor(flavor) as unknown as Record<string, string>;
  const greys = Array.from({ length: 17 }, (_, i): [string, string] => { const h = Math.min(255, i * 16).toString(16).padStart(2, "0"); return [`grey ${i * 16}`, `#${h}${h}${h}`]; });
  return [["land", f["earth"]!], ["water", f["water"]!], ["wood", f["wood_b"]!], ["forest", f["park_b"]!], ["scrub", f["scrub_b"]!],
          ["glacier", f["glacier"]!], ...greys];
}

describe("(T759) the closed colour fails as the stripe's colour", () => {
  for (const { th } of THEMES)
    it(`${th}: closed dashes on a closed line are the line's own colour (ratio 1.00, ΔE 0), and thin on the others`, () => {
      const closed = salmonStatusColour(th, "closed");
      expect(contrast(closed, closed)).toBe(1);
      expect(worstDE(closed, closed)).toBe(0);
      // and where it does not vanish it is under 3:1 on the open and release lines of the light themes
      if (th !== "dark") for (const s of ["open", "release"] as const) expect(contrast(closed, salmonStatusColour(th, s)), `${th} ${s}`).toBeLessThan(NON_TEXT);
    });
});

describe("(T759) the stripe's ink on its paper, over every line, ground and reader", () => {
  for (const { th, flavor, palette } of THEMES) {
    const { stripe, casing } = { stripe: tok(th)["color.issue.stripe"]!, casing: tok(th)["color.issue.casing"]! };
    it(`${th}: ink reads on paper (${INK_ON_PAPER}:1), and apart from closed's own paper stripe by ink and dash`, () => {
      expect(contrast(stripe, casing)).toBeGreaterThanOrEqual(INK_ON_PAPER);
      expect(worstDE(stripe, casing)).toBeGreaterThanOrEqual(FLOOR * 2);
      // closed's cue is paper dashes: the issue stripe's casing is that paper, its ink and its dash are not
      expect(stripe).not.toBe(tok(th)["color.cue.closed"]);
      // and no status colour at all: an unplaced rule is not an answer, so the stripe may not say "closed" (or open, release, check)
      for (const [name, line] of lines(th)) expect([th, name, stripe === line]).toEqual([th, name, false]);
      expect(resolveTheme(th)["dash.issue.stripe"]).not.toEqual(resolveTheme(th)["dash.cue.closed"]);
    });
    it(`${th}: on each Salmon line (a closed one included) the stripe has a part at 3:1 and a ΔE of ${FLOOR} for every reader`, () => {
      for (const [name, line] of lines(th)) {
        const part = Math.max(contrast(stripe, line), contrast(casing, line));
        const apart = Math.max(worstDE(stripe, line), worstDE(casing, line));
        expect([th, name, part >= NON_TEXT]).toEqual([th, name, true]);
        expect([th, name, apart >= FLOOR]).toEqual([th, name, true]);
      }
    });
    it(`${th}: on every basemap ground and any grey (satellite imagery) the stripe has a part at 3:1 and a ΔE of ${FLOOR}`, () => {
      for (const [name, ground] of grounds(flavor)) {
        expect([th, name, Math.max(contrast(stripe, ground), contrast(casing, ground)) >= NON_TEXT]).toEqual([th, name, true]);
        expect([th, name, Math.max(worstDE(stripe, ground), worstDE(casing, ground)) >= FLOOR]).toEqual([th, name, true]);
      }
    });
    it(`${th}: the legend's swatch (the stripe on the key's neutral line, on the card) is held to the same`, () => {
      expect(Math.max(contrast(stripe, palette.sub), contrast(casing, palette.sub))).toBeGreaterThanOrEqual(NON_TEXT);
      expect(contrast(palette.sub, palette.card)).toBeGreaterThanOrEqual(NON_TEXT);
      expect(Math.max(worstDE(stripe, palette.sub), worstDE(casing, palette.sub))).toBeGreaterThanOrEqual(FLOOR);
    });
  }
});
