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
 * (T735 look C, 2026-11-16) The dark `selection-casing` only, its colour and width from `midzoomPaint("dark")` and nothing
 * else: the colour is the case `midzoomPaint` gives (the part/selected stream edged in the ground, `color.selection.edge`;
 * the in-season colour elsewhere), and the width's casing addend is +3 at every stop, down from +5 (`width.selection.casing`
 * 1.5 each side). The frozen pre-T470 curve and the line-opacity stay as they were: a full regeneration of the entry would
 * also have brought T470's mid-zoom shape and an older in-season opacity clause, neither of them T735's.
 * (T467 shaded band, 2026-11-22) The light and dark `lake__edge` line-color only, hex for hex: each colour the shoreline
 * follows drawn `shade.lake.edge` off it (`shadeHex`, light -0.4, dark 0.45); the colour-blind theme keeps its solid edge.
 */
import { describe, expect, it } from "vitest";
import { createPropertyExpression, latest } from "@maplibre/maplibre-gl-style-spec";
import type { DayProfiles, MethodDay } from "@app/core";
import { baseAdapter, type MapHandle } from "../packages/map/src/adapters/contract";
import { runtimeStyle } from "../packages/map/src/runtime-style";
import { PMTILES } from "../packages/map/src/tile-access";
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
  const s = runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles", outside: "o.geojson" } as never, theme, {}, PMTILES) as
    { layers: { id: string; paint?: Record<string, unknown> }[] };
  paint["outside"] = s.layers.find((l) => l.id === "outside")!.paint!;
  return { ...Object.fromEntries(MIDZOOM_LAYERS.map((l) => [l, paint[l] ?? {}])), "stream-heavy": paint["stream-heavy"] ?? {} };
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

/**
 * (T848) The stream AS DRAWN: `stream`, or where the lit and closed sections' own layer shows (`stream-heavy`), that one.
 * The frozen paint has no `stream-heavy`: there the stream is its one layer.
 */
const drawn = (p: Paint, layer: string, prop: string) => {
  const own = at(prop, p[layer]?.[prop]);
  if (layer !== "stream" || !p["stream-heavy"]) return own;
  const [h, ho] = [at(prop, p["stream-heavy"][prop]), at("line-opacity", p["stream-heavy"]["line-opacity"])];
  return (zoom: number, ord: number, area: number, state: Record<string, unknown>) =>
    (Number(ho(zoom, ord, area, state)) > 0 ? h : own)(zoom, ord, area, state);
};

/** Every (layer, prop) whose evaluated value differs between `a` and `b` at one of `zooms`, with its first case. */
function differences(a: Paint, b: Paint, zooms: number[]): string[] {
  const out: string[] = [];
  for (const layer of MIDZOOM_LAYERS) {
    const props = new Set([...Object.keys(a[layer] ?? {}), ...Object.keys(b[layer] ?? {})]);
    // (T848) a value nothing shows is not the look: a line's width and colour count only where it is drawn
    const opProp = Object.keys({ ...a[layer], ...b[layer] }).find((p) => p.endsWith("-opacity") && !p.startsWith("text"));
    const [ox, oy] = opProp ? [drawn(a, layer, opProp), drawn(b, layer, opProp)] : [null, null];
    for (const prop of props) {
      const [fx, fy] = [drawn(a, layer, prop), drawn(b, layer, prop)];
      const hidden = (z: number, ord: number, area: number, s: Record<string, unknown>) => prop !== opProp && ox !== null
        && Number(ox(z, ord, area, s)) === 0 && Number(oy!(z, ord, area, s)) === 0;
      found: for (const z of zooms) for (const ord of ORDS) for (const area of AREAS) for (const s of STATES)
        if (!hidden(z, ord, area, s) && fx(z, ord, area, s) !== fy(z, ord, area, s)) {
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
    const all = midzoomPaint("light");
    const op = drawn(all, "stream", "line-opacity"), w = drawn(all, "stream", "line-width");
    const o = (ord: number, s: string) => Number(op(8, ord, 0, { p: s }));
    // z8 is halfway between the z7 and z9 rows of `fadeByOrder`
    expect([o(3, "p2"), o(4, "p2"), o(5, "b"), o(6, "b"), o(7, "p2"), o(9, "b")]).toEqual([0.475, 0.575, 0.725, 0.875, 1, 1]);
    expect([o(3, "p1"), o(5, "p1"), o(5, "n"), o(5, "p3")]).toEqual([1, 1, 0, 1]);
    expect(Number(w(8, 5, 0, { p: "p1" })) / Number(w(8, 5, 0, { p: "p2" }))).toBeCloseTo(1.3, 6);
    expect(Number(w(12, 5, 0, { p: "p1" }))).toBe(Number(w(12, 5, 0, { p: "p2" })));
  });
});
