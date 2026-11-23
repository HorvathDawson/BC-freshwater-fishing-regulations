/**
 * (T812) The closed-area circles' source and look: drawn in the Salmon view only, the day's circles in their status, the
 * legend's "only" honoured, every colour a theme token (none undefined in any theme).
 */
import { describe, expect, it } from "vitest";
import type { SalmonFacts } from "@app/core";
import { NO_CIRCLES, SALMON_CIRCLE_TOKENS, salmonCircleLayers, salmonCirclesData } from "./salmon-circles";
import { STYLE_META } from "./style";

const RING: [number, number][] = [[-126.28, 55.15], [-126.27, 55.15], [-126.27, 55.16], [-126.28, 55.15]];
const facts = {
  handles: "0011223344556677", chains: 2, loaded: [], chainOf: () => 0,
  payload: {
    format: "salmon/4", credit: "DFO", default: 0, regions: {}, gears: [], unmapped: [],
    verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 }, { s: "closed", t: "No fishing for salmon" }],
    chains: [{ r: [], w: "", l: [], sp: [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]], g: [0, -1], m: [0, 3] },
             { r: ["6"], w: "Babine Lake — Morrison Creek", l: ["6:babine-lake#closed:f4c347aad97f"],
               sp: [[0, 1], [0, 1], [0, 1], [0, 1], [0, 1]], g: [0, -1], m: [0, 3] }],
    ca: { "6:babine-lake#closed:f4c347aad97f": { c: [-126.275, 55.152], radius: 400, g: RING, chain: 1, r: "6", w: "Babine Lake — Morrison Creek", items: [] } },
  },
} as unknown as SalmonFacts;
const DAY = new Date(2026, 7, 10);

describe("the closed-area circles on the map (T812)", () => {
  it("draws the day's circles in the Salmon view only", () => {
    const got = salmonCirclesData({ facts, day: DAY }, "salmon");
    expect(got.features).toEqual([{ type: "Feature", properties: { id: "6:babine-lake#closed:f4c347aad97f", status: "closed" },
                                    geometry: { type: "Polygon", coordinates: [RING] } }]);
    expect(salmonCirclesData({ facts, day: DAY }, "status")).toEqual(NO_CIRCLES);
    expect(salmonCirclesData({ facts, day: DAY }, undefined)).toEqual(NO_CIRCLES);
    expect(salmonCirclesData(null, "salmon")).toEqual(NO_CIRCLES);
  });

  it("honours the legend's one class (`only`)", () => {
    expect(salmonCirclesData({ facts, day: DAY, only: "open" }, "salmon").features).toEqual([]);
    expect(salmonCirclesData({ facts, day: DAY, only: "closed" }, "salmon").features).toHaveLength(1);
  });

  it("paints from the theme's tokens: every status colour defined in every theme", () => {
    for (const theme of Object.keys(STYLE_META.themes)) {
      const t = STYLE_META.themes[theme] as Record<string, string>;
      for (const tok of [...Object.values(SALMON_CIRCLE_TOKENS), "color.salmon.casing"]) expect(t[tok], `${theme} ${tok}`).toBeTruthy();
      const layers = salmonCircleLayers(theme);
      expect(layers.map((l) => l.type)).toEqual(["fill", "line", "line"]);
      const fill = layers[0]!.paint["fill-color"] as unknown[];
      expect(fill).toContain(t["color.salmon.closed"]);
      expect(fill.some((x) => x === undefined)).toBe(false);
    }
  });
});
