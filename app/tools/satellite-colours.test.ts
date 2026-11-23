/**
 * (T940, owner 2026-11-23: "Build as proposed (Recommended)") THE SATELLITE SETS ARE MEASURED, not trusted.
 *
 * Over satellite imagery each theme paints the water statuses in `color.imagery.status.*`. Two gates, on the tokens as built:
 *   VISIBLE   every colour of every theme's set reaches 3:1 (WCAG 1.4.11) against the dark satellite grounds
 *             (`color.ui.imagery.water`, `.forest`: aerial photography is dark water and forest whatever the theme);
 *   SEPARATE  under cvd, every pair a view draws together is at least 20 apart (CIEDE2000) for the worst reader (Viénot and Machado,
 *             as tools/cvd.ts measures; the repo's floor). The pairs that are NEVER drawn together are recorded here, whole:
 *             `open` (Salmon keep) and `restricted` (Salmon release) are Salmon-only colours, `base` is a Standard-only one, so
 *             keep/base and release/base never meet on one map. Nothing else is exempt.
 * The light and dark set is held by the visible gate and by map-contrast (key swatches); it is not a colour-blind theme.
 */
import { describe, expect, it } from "vitest";
import { resolveTheme } from "../packages/map/src/style";
import { contrast, worstSeparation } from "./cvd";

const KEYS = ["open", "restricted", "closed", "own", "base", "no_access"] as const;
const NEVER_TOGETHER = new Set(["open/base", "restricted/base"]);
const FLOOR = 20;
const colourOf = (t: string, k: string) => String((resolveTheme(t) as Record<string, string | number | number[]>)[`color.imagery.status.${k}`]);

describe("T940 the satellite sets are measured", () => {
  /**
   * RECORDED UNDER 3:1 (the approved light / dark set, owner 2026-11-22): closed #FF3B6D on `color.ui.imagery.forest` (the preview
   * swatch's mid forest green) is 2.26:1; on the water ground it is well over. Listed whole: a new one fails, one lifted to 3:1
   * fails until it is taken off, and none may slip under its recorded ratio. cvd has none (its set was tuned to this gate).
   */
  const UNDER: Record<string, number> = { "light closed forest": 2.25, "dark closed forest": 2.25 };
  for (const th of ["light", "dark", "cvd"] as const)
    it(`every colour reaches 3:1 on the satellite grounds (water, forest), or is a recorded exception that has not slipped — ${th}`, () => {
      const t = resolveTheme(th) as Record<string, string>;
      const grounds: [string, string][] = [["water", t["color.ui.imagery.water"]!], ["forest", t["color.ui.imagery.forest"]!]];
      const under: string[] = [], slipped: string[] = [];
      for (const k of KEYS) for (const [g, hex] of grounds) {
        const r = contrast(colourOf(th, k), hex);
        if (r >= 3) continue;
        const key = `${th} ${k} ${g}`;
        under.push(key);
        if (UNDER[key] !== undefined && r < UNDER[key]!) slipped.push(`${key} ${r.toFixed(2)} < recorded ${UNDER[key]}`);
      }
      expect(slipped).toEqual([]);
      expect(under).toEqual(Object.keys(UNDER).filter((k) => k.startsWith(th + " ")));
    });

  it(`cvd: every pair a view draws together is at least ${FLOOR} apart for the worst reader`, () => {
    const low: string[] = [];
    for (let i = 0; i < KEYS.length; i++) for (let j = i + 1; j < KEYS.length; j++) {
      const name = `${KEYS[i]}/${KEYS[j]}`;
      if (NEVER_TOGETHER.has(name)) continue;
      const d = worstSeparation(colourOf("cvd", KEYS[i]!), colourOf("cvd", KEYS[j]!), ["vienot", "machado"]).dE;
      if (d < FLOOR) low.push(`${name} ${d.toFixed(1)}`);
    }
    expect(low).toEqual([]);
  });

  it("the recorded never-together pairs are Salmon-only against Standard-only colours, and only those", () => {
    expect([...NEVER_TOGETHER].sort()).toEqual(["open/base", "restricted/base"]);
  });
});
