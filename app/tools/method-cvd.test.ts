/**
 * (T213) THE GEAR MARKS, held apart for every reader. Bait ban and fly only are marks on the Standard map (owner
 * 2026-10-28): an ink edge round the status line, solid for fly only, dashed for a bait ban (`@app/map` `gearPaint`).
 * The two marks differ by SHAPE, which no colour deficiency removes; what colour must still do is make the edge visible
 * — against the status line it rings (an open water: own or base; a closed or no-access water carries no mark) and
 * against every ground it is drawn on — in every theme, for normal vision and under protanopia, deuteranopia and
 * tritanopia (both simulation models). The floors are `water-status-cvd.test.ts`'s: a status apart from another
 * (STATUS_FLOOR 20) and a line off its ground (GROUND_FLOOR 15).
 */
import { describe, expect, it } from "vitest";
import { namedFlavor } from "@protomaps/basemaps";
import { resolveTheme } from "../packages/map/src/style";
import { DEFICIENCIES, MODELS, deltaE, parseHex, simulate } from "./cvd";

/** the statuses a marked line can wear: closed and no-access sections are never marked */
const MARKED = ["color.status.own", "color.status.base"] as const;
const LINE_FLOOR = 20, GROUND_FLOOR = 15;

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

describe("the gear marks under colour-blindness (T213)", () => {
  for (const theme of ["light", "dark", "cvd"]) {
    it(`the edge stands off every marked line and every ground, and the two marks differ by dash — ${theme}`, () => {
      const t = resolveTheme(theme) as Record<string, string | number[]>;
      const edge = t["color.gear.edge"] as string;
      for (const s of MARKED) {
        const w = worst(edge, t[s] as string);
        expect(w.dE, `${theme}: edge ${edge} vs ${s} ${t[s]}, under ${w.under}`).toBeGreaterThanOrEqual(LINE_FLOOR);
      }
      for (const [g, hex] of ground(theme, FLAVOR[theme]!)) {
        const w = worst(edge, hex);
        expect(w.dE, `${theme}: edge on ${g} ${hex}, under ${w.under}`).toBeGreaterThanOrEqual(GROUND_FLOOR);
      }
      // the bait ban's edge is broken, fly only's whole: a real dash with a real gap
      const dash = t["dash.gear.bait"] as number[];
      expect(dash.length, theme).toBe(2);
      expect(Math.min(...dash), theme).toBeGreaterThanOrEqual(1);
    });
  }
});
