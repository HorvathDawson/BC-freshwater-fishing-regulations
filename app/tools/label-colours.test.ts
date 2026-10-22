/**
 * EVERY NAME ON THE MAP READS, AND NONE OF THEM IS A STATUS (refinement pass, owner 2026-10-22: "makes names and
 * labels and icon universally well labelled and styled" — in regulations, conditions, plain and satellite, light and dark).
 *
 * One label style (`packages/map/src/labels.ts`): water names in `color.label.water`, the names a reader steers by in
 * `color.label.place`, the quiet ones in `color.label.minor`, region and unit numbers in the boundary grey, every one cut
 * out of the paper (`color.outside`); over satellite imagery, the imagery inks on a near-black halo. Held here:
 *
 *   distinct   no label ink sits near any of the five status colours, as the WORST of normal vision and protan / deutan /
 *              tritan (Viénot and Machado) — a closure's name in the closure's crimson read as one more closed thing,
 *              and the basemap's station blue sat a step from `status.base`;
 *   legible    every ink holds WCAG 4.5:1 against its halo (the halo is what the word is read on), on both grounds;
 *   whole      every name layer in the style wears one of those inks and that halo — no layer keeps a colour of its own.
 */
import { describe, expect, it } from "vitest";
import { MAP_STYLE, resolveTheme, STYLE_META } from "../packages/map/src/style";
import { BASEMAP_LABELS, basemapLabelPaint, OWN_LABELS, ownLabelOnImagery } from "../packages/map/src/labels";
import { contrast, deltaE, lch, MODELS, worstSeparation } from "./cvd";

const THEMES = ["light", "dark", "cvd"] as const;
const STATUSES = ["closed", "own", "base", "restricted", "open"];
const INKS = ["color.label.water", "color.label.place", "color.label.minor", "color.region.edge"];
/**
 * The style build's own one-meaning floor (ΔE 12, tools/build-style.mjs), here held for the WORST reader. The blues are
 * crowded — base and open are blues — and no water-label blue clears 15 for every dichromat in any theme
 * (handoff/PLAN/app-data/map-polish/scripts/label-search.ts proves it); 12 is reachable in all three, and the water inks
 * were moved to reach it (light was 7.0 from `status.open` under tritanopia). Dark gave one pair back when its ink was
 * returned to blue (owner, 2026-10-22 late) — see UNDER_FLOOR.
 */
const FLOOR_DE = 12;
/**
 * THE ONE PAIR BELOW THE FLOOR, BY OWNER DIRECTION (2026-10-22 late: "dark-mode water ink: bring it back to the blue
 * family of light-mode water (it read sea-green)"). Under palette D's dark statuses no blue clears 12 from all five for
 * every reader — restricted's mauve collapses toward blue for protans and open's mint toward cyan for tritans, so the
 * floor had pushed the ink to sea-green #5AB49C. The blue that gives up least is #2498FC: 12+ from four statuses and
 * 10.6 from `status.restricted` under protanopia (Machado) — still several just-noticeable steps, between an italic
 * word on a halo and a line. Listed WHOLE, so a further slip, a second pair, or any other theme going under fails.
 */
const UNDER_FLOOR: Record<(typeof THEMES)[number], string[]> = {
  light: [],
  dark: ["color.label.water ~ status.restricted: ΔE 10.6 (protan (machado))"],
  cvd: [],
};
/**
 * A water's name is a BLUE in every theme: its CIE LCh hue within 30° of the light map's water (#4A90E2, 275°), and
 * coloured (chroma 30+), not a grey. Dark's ink had drifted to sea-green (#5AB49C, 173°) while clearing the floor above.
 */
const WATER_HUE = 275;
const WATER_HUE_SPAN = 30;
const WATER_MIN_CHROMA = 30;
/** Water ink against place ink: a clear step for normal vision (CIEDE2000 15), and 10 for the worst reader. */
const WATER_PLACE_DE = 15;
const WATER_PLACE_WORST = 10;
/**
 * WCAG on the halo: 4.5:1 for the names a reader steers by (water, places, areas); 3:1 (AA large / non-text) for the
 * quiet orientation inks — roads, neighbourhoods, the region and unit numbers — which are the boundary's own grey, set
 * large (the numbers) or secondary by design; a darker grey collides with the mask and the place ink.
 */
const FLOOR_CONTRAST: Record<string, number> = {
  "color.label.water": 4.5, "color.label.place": 4.5, "color.label.minor": 3, "color.region.edge": 3,
  "color.ui.imagery.label": 4.5, "color.ui.imagery.label-water": 4.5,
};

describe("the map's names", () => {
  for (const theme of THEMES) {
    const t = resolveTheme(theme) as Record<string, string>;

    it(`no ink is a status colour, for any reader (${theme})`, () => {
      const close: string[] = [];
      for (const ink of INKS)
        for (const s of STATUSES) {
          const w = worstSeparation(t[ink]!, t[`color.status.${s}`]!, MODELS);
          if (w.dE < FLOOR_DE) close.push(`${ink} ~ status.${s}: ΔE ${w.dE.toFixed(1)} (${w.under})`);
        }
      expect(close).toEqual(UNDER_FLOOR[theme]);
    });

    it(`a water's name is a blue, the light map's water family (${theme})`, () => {
      const { C, h } = lch(t["color.label.water"]!);
      const off = Math.abs(((h - WATER_HUE + 540) % 360) - 180);
      expect({ ink: t["color.label.water"], blue: off <= WATER_HUE_SPAN, coloured: C >= WATER_MIN_CHROMA })
        .toEqual({ ink: t["color.label.water"], blue: true, coloured: true });
    });

    it(`a water's name never reads as a town's (${theme})`, () => {
      /*
       * Review 2026-10-22: dark's water ink had been moved to #C8D0F0 to clear the statuses, ΔE 8.9 from the place ink
       * (3.9 under tritanopia) — Pitt Lake and Hemlock Valley the same pale grey-white, told apart by the italic alone.
       * Water and place are two kinds of name; held apart for normal vision by a clear step, and for the worst reader by
       * more than a just-noticeable one. Over imagery too.
       */
      const near: string[] = [];
      const pair = (what: string, a: string, b: string) => {
        const n = deltaE(a, b), w = worstSeparation(a, b, MODELS);
        if (n < WATER_PLACE_DE || w.dE < WATER_PLACE_WORST)
          near.push(`${what}: ΔE ${n.toFixed(1)}, worst ${w.dE.toFixed(1)} (${w.under})`);
      };
      pair("drawn", t["color.label.water"]!, t["color.label.place"]!);
      pair("imagery", t["color.ui.imagery.label-water"]!, t["color.ui.imagery.label"]!);
      expect(near).toEqual([]);
    });

    it(`every ink reads on its halo, on the drawn map and over imagery (${theme})`, () => {
      const low: string[] = [];
      const check = (what: string, ink: string, halo: string) => {
        const c = contrast(ink, halo);
        if (c < FLOOR_CONTRAST[what]!) low.push(`${what}: ${c.toFixed(2)}:1`);
      };
      for (const ink of INKS) check(ink, t[ink]!, t["color.outside"]!);
      for (const ink of ["color.ui.imagery.label", "color.ui.imagery.label-water"])
        check(ink, t[ink]!, t["color.ui.imagery.halo"]!);
      expect(low).toEqual([]);
    });

    it(`every name layer wears a label ink on the paper — ours and the basemap's (${theme})`, () => {
      const inks = new Set(INKS.map((k) => t[k]));
      // by VALUE: the unit numbers' `color.mu.edge` is an alias of the boundary grey
      const ours = OWN_LABELS.map((id) => {
        const m = STYLE_META.colorModes[id]?.plain as { color?: { token: string } } | undefined;
        return [id, m?.color?.token, STYLE_META.labelHalos[id]?.color];
      }).filter(([, ink, halo]) => !inks.has(t[ink!]) || halo !== "color.outside");
      const theirs = BASEMAP_LABELS.map((id) => [id, basemapLabelPaint(id, t, "map")] as const)
        .filter(([, p]) => !inks.has(p["text-color"] as string) || p["text-halo-color"] !== t["color.outside"]);
      expect({ ours, theirs }).toEqual({ ours: [], theirs: [] });
    });

    it(`over imagery every name turns light on a dark halo (${theme})`, () => {
      const imagery = new Set([t["color.ui.imagery.label"], t["color.ui.imagery.label-water"]]);
      const off = [...OWN_LABELS.map((id) => [id, ownLabelOnImagery(id, t)] as const),
                   ...BASEMAP_LABELS.map((id) => [id, basemapLabelPaint(id, t, "satellite")] as const)]
        .filter(([, p]) => !imagery.has(p["text-color"] as string) || p["text-halo-color"] !== t["color.ui.imagery.halo"])
        .map(([id]) => id);
      expect(off).toEqual([]);
    });
  }

  it("names every name layer the style has", () => {
    expect([...OWN_LABELS].sort()).toEqual(MAP_STYLE.layers.map((l) => l.id).filter((id) => id.endsWith("__label")).sort());
  });

  it("draws an area's name without the source's record id", () => {
    const l = MAP_STYLE.layers.find((x) => x.id === "no_access__label") as { layout: Record<string, unknown> };
    expect(JSON.stringify(l.layout["text-field"])).toBe(JSON.stringify(["case",
      ["all", [">", ["index-of", " [", ["get", "name"]], 0],
       ["==", ["slice", ["get", "name"], ["-", ["length", ["get", "name"]], 1]], "]"]],
      ["slice", ["get", "name"], 0, ["index-of", " [", ["get", "name"]]],
      ["get", "name"]]));
  });
});
