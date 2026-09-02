/**
 * THE COMPOSED STYLE, THROUGH MAPLIBRE'S OWN VALIDATOR.
 *
 * `check-style.mjs` proves the generated style is intact and internally consistent. It says
 * nothing about whether MapLibre will ACCEPT it — and the composed style is not the
 * generated one: `runtimeStyle` stacks the basemap under it, appends the gauge and route
 * layers, and the adapter rewrites paint per mode and per theme. All of that is code, and
 * none of it was validated.
 *
 * It shipped a style that failed to parse. `line-width` came out as
 * `case(selected, max(<zoom interpolate>, 3.2), <zoom interpolate>)`, which is illegal:
 * "Only one zoom-based step or interpolate subexpression may be used in an expression."
 * MapLibre does not warn and carry on — the layer does not parse, and the map comes up
 * unstyled with one line in the console. Nothing in the suite could see it, because every
 * test asserted on the expression we BUILT rather than on whether the renderer would take
 * it.
 *
 * This runs the spec's own validator over every theme and every colour mode a view can put
 * a layer in, which is the matrix the app actually produces.
 */
import { describe, expect, it } from "vitest";
import { validateStyleMin } from "@maplibre/maplibre-gl-style-spec";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { STYLE_META, runtimeStyle } from "@app/map";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const MAP_STYLE = JSON.parse(readFileSync(
  join(ROOT, "app/packages/map/style/style.json"), "utf8")) as { layers: { id: string }[] };

const AT = {
  atlas: "https://example.invalid/atlas.pmtiles",
  basemap: "https://example.invalid/basemap.pmtiles",
  outside: "https://example.invalid/outside.geojson",
  gauges: '{"type":"FeatureCollection","features":[]}',
};

/** Every (layer, mode) pair the style declares — the matrix the app can actually ask for. */
const modeMatrix = (): Record<string, string>[] => {
  const out: Record<string, string>[] = [{}];
  for (const [layerId, modes] of Object.entries(STYLE_META.colorModes))
    for (const mode of Object.keys(modes as Record<string, unknown>))
      out.push({ [layerId]: mode });
  return out;
};

describe("MapLibre will accept what we hand it", () => {
  for (const theme of Object.keys(STYLE_META.themes)) {
    it(`validates in the ${theme} theme, in every colour mode`, () => {
      const problems: string[] = [];
      for (const modes of modeMatrix()) {
        const style = runtimeStyle(AT, theme, modes) as unknown as Record<string, unknown>;
        for (const e of validateStyleMin(style as never))
          problems.push(`${JSON.stringify(modes)}: ${e.message}`);
      }
      expect(problems, problems.slice(0, 6).join("\n")).toEqual([]);
    });
  }

  it("names a real layer in every mode it validates", () => {
    // A guard on the guard: if `colorModes` ever stops matching the drawn layers, the loop
    // above would quietly validate nothing and keep passing.
    const drawn = new Set(MAP_STYLE.layers.map((l) => l.id));
    const covered = Object.keys(STYLE_META.colorModes).filter((id) => drawn.has(id));
    expect(covered.length).toBeGreaterThan(3);
  });
});
