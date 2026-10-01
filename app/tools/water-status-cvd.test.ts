/**
 * A WATER'S STATUS, measured for the readers who cannot check it by eye.
 *
 * The Map tab's status colouring, the search dots and the legend all paint @app/core
 * `WATER_STATUS` through the map's tokens: base (regional rules only) in BLUE, a water with
 * its own regulations in AMBER, closed today in RED. Those three have to stay three for a
 * reader with protanopia, deuteranopia or tritanopia, in the light theme AND the dark one,
 * and each has to stand off the ground it is drawn on.
 *
 * Every pair is simulated under BOTH standard models — Viénot–Brettel–Mollon 1999 and
 * Machado–Oliveira–Fernandes 2009 at full severity (`tools/cvd.ts`) — and measured in
 * CIEDE2000. The number held is the WORST of normal vision and all six simulations.
 *
 * WHAT IT CAUGHT. The light theme's closed red was #B33124. Under deuteranopia it sat ΔE 1.6
 * from the places accent (#A64B00, the town pins and the places group in search) and under
 * tritanopia ΔE 2.4 from `color.highlight`, the water you searched for — one colour, three
 * meanings. And "base" was `water.unmapped`, a GREY, while "own" was `water.mapped`, the
 * plain map's blue: recolouring a status would have repainted the plain map.
 *
 * Colour is still not the only channel. A closed line is drawn wider
 * (`width.status.closed`), and the legend swatch at the same ratio — see the last test.
 */
import { describe, expect, it } from "vitest";
import { namedFlavor } from "@protomaps/basemaps";
import { WATER_STATUS, WATER_STATUSES, type WaterStatus } from "@app/core";
import { resolveTheme, waterStatusColour, waterStatusWeight } from "@app/map";
import { DARK, LIGHT, type Palette } from "../packages/ui-native/src/theme";
import { MODELS, parseHex, worstSeparation } from "./cvd";

/** The worst ΔE2000 across normal vision and protan/deutan/tritan under both models. */
const worst = (a: string, b: string) => worstSeparation(a, b, MODELS);

/**
 * THE TWO THEMES A READER CHOOSES BETWEEN, each with the ground the status lines sit on:
 * the basemap's land, forest and water (Protomaps' flavour for that theme, read from the
 * package rather than copied) and our own lake, wetland and park fills. `cvd` rides on the
 * light ground and is held by tools/cvd.test.ts.
 */
const THEMES: { name: "light" | "dark"; palette: Palette; flavor: string }[] = [
  { name: "light", palette: LIGHT, flavor: "light" },
  { name: "dark", palette: DARK, flavor: "black" },
];

/** `fill` laid over `under` at `alpha` — what the eye gets from a translucent fill. */
function over(fill: string, under: string, alpha: number): string {
  const [f, u] = [parseHex(fill), parseHex(under)];
  return `#${f.map((c, i) => Math.round(c * alpha + u[i]! * (1 - alpha))
    .toString(16).padStart(2, "0")).join("")}`;
}

function ground(theme: string, flavor: string): [string, string][] {
  const f = namedFlavor(flavor) as unknown as Record<string, string>;
  const t = resolveTheme(theme) as Record<string, string | number>;
  const land = f["earth"]!;
  // Our fills are translucent, so the ground is the fill AS DRAWN over the land.
  const fill = (c: string, o: string) => over(t[c] as string, land, t[o] as number);
  return [
    ["land", land], ["forest", f["wood_b"]!], ["water", f["water"]!],
    ["lake", fill("color.lake.fill", "opacity.lake")],
    ["wetland", fill("color.wetland.fill", "opacity.wetland")],
    ["park", fill("color.park.fill", "opacity.park")],
  ];
}

/** Between any two statuses. ~20 is "a different category at a glance" on a thin line. */
const STATUS_FLOOR = 20;
/** A status line against the ground under it. */
const GROUND_FLOOR = 15;
/**
 * Closed against the two other warm marks drawn over water. Lower than the status floor, and
 * that is a measured limit rather than a preference: in the light theme both are dark warm
 * hues held to 4.5:1 as text, and any red that stays a red sits within reach of one of them
 * under deuteranopia or tritanopia. Width separates them as well — the highlight has its own
 * floor, a closed line its own weight, and a town is a pin, not a line.
 */
const NEIGHBOUR_FLOOR = 10;

describe("a water's status, for a colour-blind reader", () => {
  for (const { name, palette, flavor } of THEMES) {
    const colour = (s: WaterStatus) => waterStatusColour(name, s);

    it(`keeps closed, own and base apart under every deficiency — ${name}`, () => {
      for (let i = 0; i < WATER_STATUSES.length; i++)
        for (let j = i + 1; j < WATER_STATUSES.length; j++) {
          const [a, b] = [WATER_STATUSES[i]!, WATER_STATUSES[j]!];
          const w = worst(colour(a), colour(b));
          expect(w.dE, `${name}: ${a} ${colour(a)} vs ${b} ${colour(b)}, under ${w.under}`)
            .toBeGreaterThanOrEqual(STATUS_FLOOR);
        }
    });

    it(`stands every status off the ground it is drawn on — ${name}`, () => {
      for (const s of WATER_STATUSES)
        for (const [g, hex] of ground(name, flavor)) {
          const w = worst(colour(s), hex);
          expect(w.dE, `${name}: ${s} ${colour(s)} on ${g} ${hex}, under ${w.under}`)
            .toBeGreaterThanOrEqual(GROUND_FLOOR);
        }
    });

    it(`keeps closed off the search highlight and the places accent — ${name}`, () => {
      const t = resolveTheme(name) as Record<string, string>;
      for (const [n, hex] of [["highlight", t["color.highlight"]!], ["place", palette.place]]) {
        const w = worst(colour("closed"), hex!);
        expect(w.dE, `${name}: closed ${colour("closed")} vs ${n} ${hex}, under ${w.under}`)
          .toBeGreaterThanOrEqual(NEIGHBOUR_FLOOR);
      }
    });

    it(`the dots and the legend wear the map's colours — ${name}`, () => {
      for (const s of WATER_STATUSES) {
        expect(palette.waterStatus[s]).toBe(colour(s));
        expect(palette.waterStatusWeight[s]).toBe(waterStatusWeight(name, s));
      }
    });
  }

  it("base is blue, closed is red", () => {
    // The user's words, held as hue: base on the blue side of the wheel, closed on the red.
    const hue = (hex: string) => {
      const [r, g, b] = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16));
      return { blue: b! > r! && b! > g! * 0.9, red: r! > g! * 1.8 && r! > b! * 1.8 };
    };
    for (const { name } of THEMES) {
      expect(hue(waterStatusColour(name, "base")).blue, `${name} base`).toBe(true);
      expect(hue(waterStatusColour(name, "closed")).red, `${name} closed`).toBe(true);
    }
  });

  it("does not rest closed on colour alone: it is the one status drawn wider", () => {
    for (const { name } of THEMES)
      for (const s of WATER_STATUSES) {
        const w = waterStatusWeight(name, s);
        if (s === "closed") expect(w, `${name} closed weight`).toBeGreaterThanOrEqual(1.4);
        else expect(w, `${name} ${s} weight`).toBe(1);
      }
    expect(WATER_STATUS.closed.weight).toBe("width.status.closed");
  });
});
