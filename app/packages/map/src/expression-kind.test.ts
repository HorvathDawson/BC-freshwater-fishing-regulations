/**
 * (T494) A PAINT VALUE KEEPS ITS KIND ACROSS MODES. MapLibre builds a tile's paint for the kind of expression a data-driven
 * property had (data only, or zoom AND data) and does not rebuild the tile when a mode swap changes it: the stream's
 * order-faded opacity (a zoom curve, Map tab) became data-only on Conditions and Runs, and every tile already built
 * threw `this.expression.interpolationFactor is not a function` on the web. So for every layer and property, the data-
 * driven values over every mode, theme and day are all zoom curves or none are.
 */
import { describe, expect, it } from "vitest";
import type { DayProfiles } from "@app/core";
import { paintFor } from "./paint";
import { NO_VALUES, resolveTheme, STYLE_META, type ChannelValues } from "./style";

const DATA = new Set(["get", "has", "feature-state", "id", "properties", "geometry-type"]);

/** MapLibre's four kinds, read from the expression's operators. */
function kind(v: unknown): "constant" | "camera" | "source" | "composite" {
  let zoom = false, data = false;
  const walk = (x: unknown): void => {
    if (!Array.isArray(x)) return;
    if (x[0] === "zoom") zoom = true;
    if (typeof x[0] === "string" && DATA.has(x[0])) data = true;
    x.forEach(walk);
  };
  walk(v);
  return zoom ? (data ? "composite" : "camera") : (data ? "source" : "constant");
}

const DAY: DayProfiles = { closed: ["p1"], own: ["p2"], base: ["b"], no_access: ["n"], none: [] };
const WITH_DAY = { ...NO_VALUES, status: DAY } as ChannelValues;

describe("(T494) a data-driven paint value keeps its zoom dependence in every mode", () => {
  it("every layer with colour modes, every property, light / dark / cvd, with and without the day", () => {
    const mixed: string[] = [];
    for (const [layerId, modes] of Object.entries(STYLE_META.colorModes)) {
      const kinds = new Map<string, Set<string>>();
      for (const theme of ["light", "dark", "cvd"]) for (const mode of Object.keys(modes))
        for (const values of [NO_VALUES, WITH_DAY])
          for (const [prop, v] of Object.entries(paintFor(layerId, mode, resolveTheme(theme), values))) {
            const k = kind(v);
            if (k === "source" || k === "composite") (kinds.get(prop) ?? kinds.set(prop, new Set()).get(prop)!).add(k);
          }
      for (const [prop, ks] of kinds) if (ks.size > 1) mixed.push(`${layerId} ${prop}`);
    }
    expect(mixed).toEqual([]);
  });
});
