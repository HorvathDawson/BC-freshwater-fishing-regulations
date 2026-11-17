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
import { nativeStyle } from "../packages/map/src/native-style";
import { statusInput } from "../packages/map/src/colouring";
import { NO_VALUES, type ChannelValues } from "../packages/map/src/style";
import type { SalmonFacts, StatusIndex } from "@app/core";

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

/*
 * THE PHONE'S STYLE TOO (N2). MapLibre Native is handed `nativeStyle`: the same layers with no `promoteId`, no
 * fill-pattern, no gauge pill, and the basin field's values written in as a lookup by `basin_id` — the one expression
 * the web never sends. A style that fails to parse on a phone is dropped layer by layer, silently.
 */
describe("MapLibre Native will accept what we hand it", () => {
  for (const theme of Object.keys(STYLE_META.themes)) {
    // (T594) on both grounds: over satellite the stream lines open (`line-gap-width`) and the lake fills fade
    it(`validates in the ${theme} theme, in every colour mode, with basin values, on the map and on satellite`, () => {
      const problems: string[] = [];
      for (const modes of modeMatrix()) for (const basemap of ["map", "satellite"] as const) {
        const { style } = nativeStyle({
          at: AT, theme, view: "plain", modes, basemap, gauges: AT.gauges,
          areaData: { basin: { "B-1": { standing: 30 }, "7": { standing: -100 } } },
        });
        for (const e of validateStyleMin(style as never))
          problems.push(`${JSON.stringify(modes)} ${basemap}: ${e.message}`);
      }
      expect(problems, problems.slice(0, 6).join("\n")).toEqual([]);
    });
  }
});

/*
 * (T796 A6) THE STANDARD MAP UNDER A DFO FINFISH CLOSURE: every status layer's colour (and its outline, closed stripe,
 * gear marks, no-access dots and order fade) reads the salmon chain label `sp` ahead of the day's profile — a `case` the
 * plain matrix above never builds (it is handed no day). Shown, and isolated away by a legend tap; both grounds.
 */
describe("MapLibre accepts the finfish closure paint", () => {
  it("every Standard layer with a chain closed to all fishing, on the map and on satellite, shown and isolated away", () => {
    const facts = { handles: "0".repeat(16), chains: 2, chainOf: () => 1, payload: {
      format: "salmon/4", credit: "DFO", default: 0, regions: {}, gears: [], unmapped: [],
      verdicts: [{ s: "closed", t: "Not opened by DFO", d: 1 }, { s: "closed", t: "Finfish closure", f: 1 }],
      chains: [{ r: [], w: "", l: [], sp: [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]], g: [0, -1], m: [0, 3] },
               { r: [], w: "", l: [], sp: [[0, 1], [0, 0], [0, 0], [0, 0], [0, 0]], g: [0, -1], m: [0, 3] }] } } as SalmonFacts;
    const index = { handles: "0".repeat(16), reachDigest: "0".repeat(16), sections: 0, waters: 0, profiles: 1, profileOf: () => 0,
                    profileCodeOn: () => "own", codeOn: () => "own", waterCodeOn: () => "own" } as StatusIndex;
    const day = new Date(2026, 6, 20);
    const method = { fly: ["m1"], bait: ["m2"] } as unknown as ChannelValues["method"];
    const modes = Object.fromEntries(Object.entries(STYLE_META.colorModes)
      .filter(([, m]) => (m as Record<string, unknown>)["profile"] !== undefined).map(([id]) => [id, "profile"]));
    const problems: string[] = [];
    for (const only of [null, "own"] as const) for (const basemap of ["map", "satellite"] as const) {
      const status = statusInput({ index, day, only }, { facts, day }).values() as ChannelValues["status"];
      const { style } = nativeStyle({ at: AT, theme: "light", view: "plain", modes, basemap, gauges: AT.gauges,
                                      values: { ...NO_VALUES, status, method } });
      expect(JSON.stringify(style)).toContain('["feature-state","sp"]');
      for (const e of validateStyleMin(style as never)) problems.push(`${only} ${basemap}: ${e.message}`);
    }
    expect(problems, problems.slice(0, 6).join("\n")).toEqual([]);
  });
});
