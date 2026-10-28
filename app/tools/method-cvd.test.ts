/**
 * (T213) THE BAIT & FLY KEY, held apart for every reader: fly only, bait ban, no restriction and closed — four classes on
 * a thin line — stand a "different category at a glance" apart (`water-status-cvd.test.ts` STATUS_FLOOR) in every
 * theme, for normal vision and under protanopia, deuteranopia and tritanopia (both simulation models); and every class
 * stands off the ground it is drawn on (`water-status-cvd.test.ts` GROUND_FLOOR). The status hues alone could not do
 * both (light: restricted vs open 15.9 for a deuteranope), so `none` and dark's `fly` carry their own hexes.
 */
import { describe, expect, it } from "vitest";
import { namedFlavor } from "@protomaps/basemaps";
import { resolveTheme } from "../packages/map/src/style";
import { DEFICIENCIES, MODELS, deltaE, parseHex, simulate } from "./cvd";

const CLASSES = ["fly", "bait", "none", "closed"] as const;
const FLOOR = 20, GROUND_FLOOR = 15;

/** `fill` laid over `under` at `alpha` — what the eye gets from a translucent fill (as water-status-cvd.test.ts). */
function over(fill: string, under: string, alpha: number): string {
  const [f, u] = [parseHex(fill), parseHex(under)];
  return `#${f.map((c, i) => Math.round(c * alpha + u[i]! * (1 - alpha)).toString(16).padStart(2, "0")).join("")}`;
}
function ground(theme: string, flavor: string): [string, string][] {
  const f = namedFlavor(flavor) as unknown as Record<string, string>;
  const t = resolveTheme(theme) as Record<string, string | number>;
  const land = f["earth"]!;
  const fill = (c: string, o: string) => over(t[c] as string, land, t[o] as number);
  return [["land", land], ["forest", f["wood_b"]!], ["water", f["water"]!],
          ["lake", fill("color.lake.fill", "opacity.lake")], ["wetland", fill("color.wetland.fill", "opacity.wetland")],
          ["park", fill("color.park.fill", "opacity.park")]];
}
const FLAVOR: Record<string, string> = { light: "light", dark: "black", cvd: "light" };

function worst(a: string, b: string): { dE: number; under: string } {
  let out = { dE: deltaE(a, b), under: "normal vision" };
  for (const k of DEFICIENCIES)
    for (const m of MODELS) {
      const dE = deltaE(simulate(a, k, m), simulate(b, k, m));
      if (dE < out.dE) out = { dE, under: `${k} (${m})` };
    }
  return out;
}

describe("the Bait & fly key under colour-blindness (T213)", () => {
  for (const theme of ["light", "dark", "cvd"]) {
    it(`keeps every pair of classes apart — ${theme}`, () => {
      const t = resolveTheme(theme) as Record<string, string>;
      for (let i = 0; i < CLASSES.length; i++)
        for (let j = i + 1; j < CLASSES.length; j++) {
          const a = t[`color.method.${CLASSES[i]}`]!, b = t[`color.method.${CLASSES[j]}`]!;
          const w = worst(a, b);
          expect(w.dE, `${theme}: ${CLASSES[i]} ${a} vs ${CLASSES[j]} ${b}, under ${w.under}`).toBeGreaterThanOrEqual(FLOOR);
        }
      for (const c of CLASSES)
        for (const [g, hex] of ground(theme, FLAVOR[theme]!)) {
          const w = worst(t[`color.method.${c}`]!, hex);
          expect(w.dE, `${theme}: ${c} on ${g} ${hex}, under ${w.under}`).toBeGreaterThanOrEqual(GROUND_FLOOR);
        }
    });
  }
});
