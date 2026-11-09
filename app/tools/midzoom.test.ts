/**
 * (T470 B, owner 2026-11-07) SMALL WATERS RECEDE AT MID ZOOM — AND ONLY THERE. From z12 in, every layer the change
 * touches (the stream and its marks, the lakes and their shoreline, the region and unit borders, the outside mask) must
 * paint exactly what it painted before T470: `midzoom-today.fixture.json` is that paint, frozen from the pre-T470 map by
 * `midzoomPaint` below. Each expression is EVALUATED (MapLibre's own evaluator) over a grid of zooms, stream orders,
 * lake areas and feature-states, in every theme, and the values compared — not the expressions, which T470 reshapes.
 * The z8 half proves the check is not vacuous: mid zoom DID change.
 *
 * The fixture is `JSON.stringify` of `{ light, dark, cvd }: midzoomPaint(theme)` run on the tree before T470 (2026-11-07).
 * A later change that means to alter the close-in map regenerates it the same way, on purpose and in its own commit.
 * (T470 WCAG, 2026-11-09) Two palette moves were carried into it by hand, hex for hex and nothing else: the dark base
 * (~0368EB -> ~4C70FC) and the colour-blind selection (~F55C00 -> ~C04400), each to reach 3:1 on the ground
 * (tools/map-contrast.test.ts).
 */
import { describe, expect, it } from "vitest";
import { createPropertyExpression, latest } from "@maplibre/maplibre-gl-style-spec";
import type { DayProfiles, MethodDay } from "@app/core";
import { baseAdapter, type MapHandle } from "../packages/map/src/adapters/contract";
import { runtimeStyle } from "../packages/map/src/runtime-style";
import ENCODED from "./midzoom-today.fixture.json";

type Paint = Record<string, Record<string, unknown>>;
/** The frozen paint, its colours written `~` for the hash (the house trick: no colour literal outside the token files). */
const TODAY = JSON.parse(JSON.stringify(ENCODED).replace(/~/g, "#")) as Record<string, Paint>;

/** The paint the recede touches, as the map is handed it: the Map tab's profile day, gear marks and all. */
const MIDZOOM_DAY: DayProfiles = { closed: ["p1"], own: ["p2"], base: ["b"], no_access: ["n"], none: ["p3"] };
const MIDZOOM_METHOD: MethodDay = { none: ["m0"], bait: ["m1"], fly: ["m2"] };
const MIDZOOM_LAYERS = ["stream", "stream-closed", "stream-gear-fly", "stream-gear-bait", "selection-casing",
  "stream-noaccess", "lake", "lake_flowing", "lake__edge", "region__edge", "mu__edge", "outside"] as const;

function midzoomPaint(theme: string): Record<string, Record<string, unknown>> {
  const paint: Record<string, Record<string, unknown>> = {};
  const h: MapHandle = { setVisibility: () => {}, setPaint: (l, p, v) => { (paint[l] ??= {})[p] = v; },
                         setFeatureState: () => {}, clearFeatureStates: () => {} };
  const a = baseAdapter("web");
  a.applyView(h, "plain", theme);
  for (const l of ["stream", "lake", "lake_flowing"]) a.setLayerMode(h, l, "profile", theme);
  a.setChannel(h, "status", MIDZOOM_DAY, theme);
  a.setChannel(h, "method", MIDZOOM_METHOD, theme);
  const s = runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles", outside: "o.geojson" } as never, theme) as
    { layers: { id: string; paint?: Record<string, unknown> }[] };
  paint["outside"] = s.layers.find((l) => l.id === "outside")!.paint!;
  return Object.fromEntries(MIDZOOM_LAYERS.map((l) => [l, paint[l] ?? {}]));
}

const THEMES = ["light", "dark", "cvd"] as const;
const ORDS = [1, 3, 4, 5, 6, 7, 9];
const AREAS = [1e4, 1e7];
const PS = ["p1", "p2", "b", "n", "p3", ""];
const FLAGS = [{}, { selected: true }, { part: true }, { water: true }];
const GMS = ["m0", "m1", "m2"];
const STATES = PS.flatMap((p, i) => FLAGS.map((f, j) => ({ p, gm: GMS[(i + j) % 3]!, ...f })));

const specOf = (prop: string) =>
  (latest as unknown as Record<string, Record<string, { default?: unknown }>>)[`paint_${prop.split("-")[0]}`]![prop]!;
/** The property's value as a function of zoom, feature and state; an absent property is MapLibre's default. */
const at = (prop: string, value: unknown) => {
  const spec = specOf(prop);
  const v = value ?? spec.default;
  // a bare dash pattern is a constant to the style, but an expression must say it is a literal
  const e = createPropertyExpression(Array.isArray(v) && typeof v[0] === "number" ? ["literal", v] : v,
                                     prop as never, spec as never);
  if (e.result !== "success") throw new Error(`${prop} does not parse: ${JSON.stringify(e.value)}`);
  return (zoom: number, ord: number, area: number, state: Record<string, unknown>) =>
    JSON.stringify(e.value.evaluate({ zoom } as never, { type: 2, properties: { ord, area_m2: area } } as never,
                                    state as never), (_, v) => (typeof v === "number" ? Math.round(v * 1e6) / 1e6 : v));
};

/** Every (layer, prop) whose evaluated value differs between `a` and `b` at one of `zooms`, with its first case. */
function differences(a: Paint, b: Paint, zooms: number[]): string[] {
  const out: string[] = [];
  for (const layer of MIDZOOM_LAYERS) {
    const props = new Set([...Object.keys(a[layer] ?? {}), ...Object.keys(b[layer] ?? {})]);
    for (const prop of props) {
      const [fx, fy] = [at(prop, a[layer]?.[prop]), at(prop, b[layer]?.[prop])];
      found: for (const z of zooms) for (const ord of ORDS) for (const area of AREAS) for (const s of STATES)
        if (fx(z, ord, area, s) !== fy(z, ord, area, s)) {
          out.push(`${layer} ${prop} z${z} ord ${ord} ${JSON.stringify(s)}: ${fx(z, ord, area, s)} -> ${fy(z, ord, area, s)}`);
          break found;
        }
    }
  }
  return out;
}

describe("(T470) the mid-zoom recede", () => {
  for (const theme of THEMES) {
    it(`leaves the map from z12 in exactly as it was — ${theme}`, () => {
      expect(differences(TODAY[theme]!, midzoomPaint(theme),
                         [12, 12.25, 12.5, 13, 14, 16, 18, 20])).toEqual([]);
    });

    it(`changes it at mid zoom (the check is not vacuous) — ${theme}`, () => {
      const changed = differences(TODAY[theme]!, midzoomPaint(theme), [8])
        .map((d) => d.split(" ").slice(0, 2).join(" "));
      for (const want of ["stream line-width", "stream line-opacity", "lake__edge line-width", "region__edge line-width",
                          "region__edge line-opacity", "mu__edge line-width", "mu__edge line-opacity", "outside fill-opacity"])
        expect(changed).toContain(want);
    });
  }

  it("fades own and base by order, never closed; closed is the heaviest line (light, z8)", () => {
    const p = midzoomPaint("light").stream!;
    const op = at("line-opacity", p["line-opacity"]), w = at("line-width", p["line-width"]);
    const o = (ord: number, s: string) => Number(op(8, ord, 0, { p: s }));
    // z8 is halfway between the z7 and z9 rows of `fadeByOrder`
    expect([o(3, "p2"), o(4, "p2"), o(5, "b"), o(6, "b"), o(7, "p2"), o(9, "b")]).toEqual([0.475, 0.575, 0.725, 0.875, 1, 1]);
    expect([o(3, "p1"), o(5, "p1"), o(5, "n"), o(5, "p3")]).toEqual([1, 1, 0, 1]);
    expect(Number(w(8, 5, 0, { p: "p1" })) / Number(w(8, 5, 0, { p: "p2" }))).toBeCloseTo(1.3, 6);
    expect(Number(w(12, 5, 0, { p: "p1" }))).toBe(Number(w(12, 5, 0, { p: "p2" })));
  });
});
